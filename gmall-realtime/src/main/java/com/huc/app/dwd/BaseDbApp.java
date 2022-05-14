package com.huc.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.huc.app.func.DimSinkFunction;
import com.huc.app.func.TableProcessFunction;
import com.huc.bean.TableProcess;
import com.huc.utils.MyDeserialization;
import com.huc.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

public class BaseDbApp {
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

        //TODO 2.读取Kafka ods_base_db 主题的数据创建流
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_db", "base_db_app_211025"));

        //TODO 3.将数据转为JSON对象
        SingleOutputStreamOperator<JSONObject> JsonObjDS = kafkaDS.map(JSON::parseObject);

        //TODO 4.过滤空值数据（控制数据即类型为delete的数据）
        // 获取主流
        /**
         * {
         * "database":"",
         * "tablename":"",
         * "after":{"id":"1001","name":"zs",...},
         * "before":{},
         * "type":"insert"
         * }
         */
        SingleOutputStreamOperator<JSONObject> filterDS = JsonObjDS.flatMap(new FlatMapFunction<JSONObject, JSONObject>() {
            @Override
            public void flatMap(JSONObject value, Collector<JSONObject> out) throws Exception {
                if (!"delete".equals(value.getString("type"))) {
                    out.collect(value);
                }
            }
        });

        //TODO 5.使用flinkCDC读取配置表创建广播流
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall-211025-realtime")
                .tableList("gmall-211025-realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDeserialization())
                .build();

        DataStreamSource<String> FlinkcdcDS = env.addSource(sourceFunction);

        // todo 获取广播流
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map_state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = FlinkcdcDS.broadcast(mapStateDescriptor);

        //TODO 6.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcastStream);

        //TODO 7.处理主流和广播流数据
        OutputTag<JSONObject> outputTag = new OutputTag<JSONObject>("HbaseTag") {
        };
        SingleOutputStreamOperator<JSONObject> KafkaMainDS = connectDS.process(new TableProcessFunction(mapStateDescriptor, outputTag));

        //TODO 8.提取两个流的数据
        DataStream<JSONObject> HbaseDS = KafkaMainDS.getSideOutput(outputTag);

        //TODO 9.将两个流的数据分别写出
        KafkaMainDS.print("kafka>>>>");
        HbaseDS.print("hbase>>>>");

        /**
         * {
         * "sinkTable":"dim_base_category1",
         * "database":"",
         * "tablename":"",
         * "after":{"id":"1001","name":"zs",...},
         * "before":{},
         * "type":"insert"
         * }
         */
        // todo 写到hbase
//        HbaseDS.rebalance();
        HbaseDS.addSink(new DimSinkFunction());

        // todo 写到Kafka
        KafkaMainDS.addSink(MyKafkaUtil.getKafkaSink(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long timestamp) {
                return new ProducerRecord<>(jsonObject.getString("sinkTable"), jsonObject.getString("after").getBytes());
            }
        }));

        //TODO 10.启动任务
        env.execute("BaseDbApp");
    }
}
