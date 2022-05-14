package com.huc.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.huc.utils.DateTimeUtil;
import com.huc.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;

/**
 * UV，全称是Unique Visitor，即独立访客，对于实时计算中，也可以称为DAU(Daily Active User)，即每日活跃用户，因为实时计算中的UV通常是指当日的访客数。
 * <p>
 * 从日志中识别出当日的访客，有两点：
 * 	todo 其一，是识别出该访客打开的第一个页面，表示这个访客开始进入我们的应用
 * 	todo 其二，由于访客可以在一天中多次进入应用，所以我们要在一天的范围内进行去重
 */
public class UniqueVisitApp {
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

        // TODO 2.消费Kafka   dwd_page_log    主题的数据
        String groupId = "unique_visit_app";
        String sourceTopic = "dwd_page_log";
        String sinkTopic = "dwm_unique_visit";

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

        // TODO 4.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = JsonObjDS.keyBy(new KeySelector<JSONObject, String>() {
            @Override
            public String getKey(JSONObject jsonObject) throws Exception {
                return jsonObject.getJSONObject("common").getString("mid");
            }
        });

        // TODO 5.使用状态编程，对于非今天访问的第一条数据做过滤
        // @FunctionalInterface 有这个注解才可以使用函数式编程
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("value_state", String.class);
                stateDescriptor.enableTimeToLive(new StateTtlConfig.Builder(Time.days(1))
                        // 设置ttl  状态过期时间  最多保存一天的日期
                        // todo OnCreateAndWrite  上次访问时间戳在每次写入操作时创建和更新状态时初始化。
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build());
                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                String last_page_id = value.getJSONObject("page").getString("last_page_id");
                if (last_page_id == null) {
                    // 取出状态数据
                    String dateState = valueState.value();

                    // 获取数据携带的日期
                    Long ts = value.getLong("ts");
                    String currentDate = DateTimeUtil.toYMDhms(new Date(ts));

                    if (dateState == null || !dateState.equals(currentDate)) {
                        // 更新状态并将数据保留
                        valueState.update(currentDate);
                        return true;
                    }
                }
                return false;
            }
        });

        // TODO 6.将数据写入到Kafka
        filterDS.print("UniqueVisitApp>>>>");
        filterDS.map(line -> JSON.toJSONString(line)).addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        // TODO 7.启动任务
        env.execute("UniqueVisitApp");
    }
}
