package com.huc.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.huc.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * todo 跳出明细计算
 * 跳出就是用户成功访问了网站的一个页面后就退出，不在继续访问网站的其它页面。而跳出率就是用跳出次数除以访问次数。
 * 关注跳出率，可以看出引流过来的访客是否能很快的被吸引，渠道引流过来的用户之间的质量对比，对于应用优化前后跳出率的对比也能看出优化改进的成果。
 * <p>
 * todo 首先要识别哪些是跳出行为，要把这些跳出的访客最后一个访问的页面识别出来。那么要抓住几个特征：
 * 	todo 该页面是用户近期访问的第一个页面
 * 这个可以通过该页面是否有上一个页面（last_page_id）来判断，如果这个表示为空，就说明这是这个访客这次访问的第一个页面。
 * 	todo 首次访问之后很长一段时间（自己设定），用户没继续再有其他页面的访问。
 * <p>
 * todo 第二个条件最简单的办法就是Flink自带的CEP技术。这个CEP非常适合通过多条数据组合来识别某个事件。用户跳出事件，本质上就是一个条件事件加一个超时事件的组合。
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 生产环境中应该与Kafka的分区数保持一致  这样会导致效率最大化

    /*    //设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // 开启CK 以及 指定状态后端
        // 每5min做一次checkpoint
        env.enableCheckpointing(5 * 60000L);
        // 最多可以同时存在几个checkpoint
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // 两个checkpoint之间最小间隔时间
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        // checkpoint的重启策略，现在默认一般是三次，不用管了
        env.getRestartStrategy();

        env.setStateBackend(new FsStateBackend(""));*/

        // TODO 2.读取Kafka页面主题的数据创建流
        String sourceTopic = "dwd_page_log";
        String groupId = "user_Jump_Detail_App_211025";
        String sinkTopic = "dwm_user_jump_detail";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(sourceTopic, groupId));

        // TODO 3.将每行数据转换为JSON对象
        //{
        //    "actions":[{"action_id":"get_coupon","item":"1","item_type":"coupon_id","ts":1639625089772}],
        //
        //    "common":{"ar":"110000","ba":"iPhone","ch":"Appstore","is_new":"1","md":"iPhone X","mid":"mid_6383","os":"iOS 12.4.1","uid":"26","vc":"v2.1.134"},
        //
        //    "displays":[
        //    {"display_type":"query","item":"5","item_type":"sku_id","order":1,"pos_id":2},
        //    {"display_type":"query","item":"4","item_type":"sku_id","order":7,"pos_id":3}
        //    ],
        //
        //    "page":{"during_time":19544,"item":"1","item_type":"sku_id","last_page_id":"home","page_id":"good_detail","source_type":"promotion"},
        //
        //    "ts":1639625080000
        //}
        SingleOutputStreamOperator<JSONObject> JsonObjDS = kafkaDS.map(JSON::parseObject);

        // TODO 4.提取事件时间生成WaterMark
        SingleOutputStreamOperator<JSONObject> jsonObjectWithWaterMarkDS = JsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject jsonObject, long recordTimestamp) {
                        return jsonObject.getLong("ts");
                    }
                }));

        // TODO 5.按照mid进行分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjectWithWaterMarkDS.keyBy(line -> line.getJSONObject("common").getString("mid"));

        // TODO 6.定义模式序列
        Pattern<JSONObject, JSONObject> pattern = Pattern
                // 泛型指的是 事件模式的基本类型
                .<JSONObject>begin("start")
                .where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject, Context<JSONObject> ctx) throws Exception {
                        return jsonObject.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
                .next("next")
                .where(new IterativeCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObject, Context<JSONObject> ctx) throws Exception {
                        return jsonObject.getJSONObject("page").getString("last_page_id") == null;
                    }
                })
                .within(Time.seconds(10));

        // TODO 7.将模式序列作用在流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        // TODO 8.提取事件（匹配上的事件和超时事件）
        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("outputTag") {
        };

        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(outputTag, new PatternTimeoutFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject timeout(Map<String, List<JSONObject>> listMap, long timeoutTimestamp) throws Exception {
                return listMap.get("start").get(0);
            }
        }, new PatternSelectFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject select(Map<String, List<JSONObject>> listMap) throws Exception {
                return listMap.get("start").get(0);
            }
        });

        // TODO 9.结合两个事件流
        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(outputTag);
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);

        // TODO 10.将数据写入Kafka
        unionDS.print("unionDS>>>>");

        unionDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        // TODO 11.启动任务
        env.execute("UserJumpDetailApp");
    }
}
