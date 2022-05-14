package com.huc.app.dws;

import com.huc.app.func.SplitFunction;
import com.huc.bean.KeywordStats;
import com.huc.utils.ClickHouseUtil;
import com.huc.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  //生产环境应该设置为Kafka主题的分区数

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔5秒钟做一次CK
//        env.enableCheckpointing(5000L);
//        //2.2 指定CK的一致性语义
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //2.3 设置任务关闭的时候保留最后一次CK数据
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        //2.4 指定从CK自动重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
//        //2.5 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flinkCDC"));
//        //2.6 设置访问HDFS的用户名
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2.DDL建表  提取事件时间生成WaterMark
        String groupId = "keyword_stats_app_210726";
        String pageViewSourceTopic = "dwd_page_log";

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

        tableEnv.executeSql("create table page_log(" +
                "page map<string,string>," +
                "ts bigint," +
                "rt as TO_TIMESTAMP_LTZ(ts,3)," +
                "WATERMARK FOR rt AS rt - INTERVAL '2' second " +
                ")with(" +
                MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) +
                ")");

//        tableEnv.executeSql("select * from page_log").print();

        // TODO 3.过滤数据    只需要搜索的日志
        Table filtertable = tableEnv.sqlQuery("select " +
                "page['item'] keywords," +
                "rt " +
                "from page_log " +
                "where page['last_page_id']='search' " +
                "and page['item'] is not null");

//        filtertable.execute().print();
        tableEnv.createTemporaryView("filter_table", filtertable);

        // TODO 4.注册UDTF并使用其完成切词
        tableEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);

        Table splitTable = tableEnv.sqlQuery("select " +
                "word," +
                "rt " +
                "from filter_table," +
                "lateral table(SplitFunction(keywords))");
//        splitTable.execute().print();
        tableEnv.createTemporaryView("split_table", splitTable);

        // TODO 5.计算每个分词出现的次数
        Table resultTable = tableEnv.sqlQuery("select " +
                "'search' source," +
                "DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' second), 'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' second), 'yyyy-MM-dd HH:mm:ss') edt," +
                "word keyword," +
                "count(*) ct," +
                "UNIX_TIMESTAMP()*1000 ts " +
                "from split_table " +
                "group by " +
                "word," +
                "TUMBLE(rt, INTERVAL '10' second)");
//        resultTable.execute().print();

        // TODO 6.将动态表转换为流
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(resultTable, KeywordStats.class);

        // TODO 7.将数据写入ClickHouse
//        keywordStatsDataStream.print(">>>>");
        keywordStatsDataStream.addSink(ClickHouseUtil.getClickHouseSink("insert into keyword_stats_211025(keyword,ct,source,stt,edt,ts) values(?,?,?,?,?,?)"));

        // TODO 8.启动任务
        env.execute("KeywordStatsApp");
    }
}
