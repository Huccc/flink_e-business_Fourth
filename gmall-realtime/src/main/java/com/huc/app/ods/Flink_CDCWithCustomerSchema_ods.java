package com.huc.app.ods;

import com.huc.utils.MyDeserialization;
import com.huc.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink_CDCWithCustomerSchema_ods {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取流的执行环境
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

        //TODO 2.设置相关属性
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_flink_1025")
//                .tableList("gmall_flink_1025.base_trademark")
                .username("root")
                .password("123456")
                // todo 程序启动时以什么方式来读取到数据
                // 从头读，查询到的数据类型是read
//                .startupOptions(StartupOptions.initial())

                .startupOptions(StartupOptions.latest())
                // todo 读数据设置反序列化
//                .deserializer(new StringDebeziumDeserializationSchema())
                .deserializer(new MyDeserialization())
                .build();

        // 将flinkcdc添加到source中    指定source端
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        streamSource.print();
        streamSource.addSink(MyKafkaUtil.getKafkaSink("ods_base_db"));

        env.execute("Flink_CDCWithCustomerSchema_ods");
    }
}
