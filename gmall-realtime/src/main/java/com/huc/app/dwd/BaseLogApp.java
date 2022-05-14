package com.huc.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.huc.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

/*        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔5分钟做一次CK
        env.enableCheckpointing(5000*60L);
        //2.2 指定CK的一致性语义(默认就是 EXACTLY_ONCE ,所以这块可以不取指定)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //2.3 设置任务关闭的时候保留最后一次CK数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 指定从CK自动重启策略（现在也不用设置了，默认重启一次）
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
        //2.5 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkCDC"));
        //2.6 设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "atguigu");*/

        // TODO 2.读取Kafka ods_base_log 主题的数据 创建流
        String SourceTopic = "ods_base_log";
        String GroupId = "base_log_app_211025";

        // TODO 3.将数据转为JSON对象
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(SourceTopic, GroupId));

        OutputTag<String> DirtyTag = new OutputTag<String>("dirtyTag") {
        };

        SingleOutputStreamOperator<JSONObject> JsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    // 将转换成功的数据写入主流
                    out.collect(jsonObject);
                } catch (Exception e) {
                    // 将转换失败的数据写入侧输出流
                    ctx.output(DirtyTag, value);
                }
            }
        });

//        JsonObjDS.print("JSON>>>>");
//        JsonObjDS.getSideOutput(DirtyTag).print("Dirty>>>>");

        // TODO 4.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = JsonObjDS.keyBy(line -> line.getJSONObject("common").getString("mid"));

        // TODO 5.新老用户校验
        // todo is_new为1是新用户，is_new为0是老用户
        SingleOutputStreamOperator<JSONObject> JsonObjWithIs_New = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("valuestate", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                // 获取新老用户标记
                String is_new = value.getJSONObject("common").getString("is_new");
                // 判断是否为1（新用户）
                if ("1".equals(is_new)) {
                    // 获取状态数据，并判断是否为null
                    String state = valueState.value();
                    if (state == null) {
                        // 更新状态
                        valueState.update("老用户");
                    } else {
                        // 更新数据
                        value.getJSONObject("common").put("is_new", "0");
                    }
                }
                return value;
            }
        });

//        JsonObjWithIs_New.print("is_new>>>>");

        // TODO 6.分流    将页面日志放在主流   启动和曝光日志放在测输出流

        // todo 启动日志
        //{
        //    "common":{"ar":"370000","ba":"Xiaomi","ch":"oppo","is_new":"1","md":"Xiaomi 10 Pro ","mid":"mid_262","os":"Android 10.0","uid":"6","vc":"v2.1.134"},
        //
        //    "start":{"entry":"icon","loading_time":16743,"open_ad_id":18,"open_ad_ms":2077,"open_ad_skip_ms":0},
        //
        //    "ts":1639625080000
        //}

        // todo 曝光日志
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

        // todo 侧输出流
        OutputTag<JSONObject> startTag = new OutputTag<JSONObject>("start") {
        };
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display") {
        };

        SingleOutputStreamOperator<JSONObject> pageDS = JsonObjWithIs_New.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                if (value.getString("start") != null) {
                    // 将数据写入启动日志侧输出流
                    ctx.output(startTag, value);
                } else {
                    // todo 不是启动日志就是页面日志
                    // 将数据写入启动日志主流
                    out.collect(value);

                    // 获取曝光数据字段
                    JSONArray displays = value.getJSONArray("displays");

                    Long ts = value.getLong("ts");
                    String page_id = value.getJSONObject("page").getString("page_id");

                    // 判断是否存在曝光数据
                    if (displays != null && displays.size() > 0) {
                        // 遍历曝光数据，将曝光数据写入到display侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);

                            // 补充页面ID和时间戳
                            display.put("ts", ts);
                            display.put("page_id", page_id);

                            // 将数据写入到曝光侧输出流
                            ctx.output(displayTag, display);
                        }
                    }
                }
            }
        });

        // TODO 7.提取各个流的数据
        pageDS.print("page>>>>");

        DataStream<JSONObject> startDS = pageDS.getSideOutput(startTag);
        startDS.print("start>>>>");

        DataStream<JSONObject> displayDS = pageDS.getSideOutput(displayTag);
        displayDS.print("display");

        // TODO 8.将数据写入Kafka
        pageDS.map(line -> JSON.toJSONString(line)).addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        startDS.map(line -> JSON.toJSONString(line)).addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        displayDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));

        // TODO 9.启动任务
        env.execute("BaseLogApp");
    }
}
