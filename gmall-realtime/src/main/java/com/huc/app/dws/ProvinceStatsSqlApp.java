package com.huc.app.dws;

import com.huc.bean.ProvinceStats;
import com.huc.utils.ClickHouseUtil;
import com.huc.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 地区主题主要是反映各个地区的销售情况。从业务逻辑上地区主题比起商品更加简单，业务逻辑也没有什么特别的就是做一次轻度聚合然后保存，所以在这里我们体验一下使用FlinkSQL，来完成该业务。
 */
public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 生产环境中应该与Kafka的分区数保持一致  这样会导致效率最大化

        // todo 获取表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        String groupId = "province_stats";
        String orderWideTopic = "dwm_order_wide";

        // TODO 2.使用DDL的方式读取Kafka主题的数据   注意：需要提取事件时间生成WaterMark
        tableEnv.executeSql("create table order_wide(" +
                "province_id bigint," +
                "province_name string," +
                "province_area_code string," +
                "province_iso_code string," +
                "province_3166_2_code string," +
                "order_id bigint," +
                "split_total_amount decimal," +
                "create_time string," +
                "rt as TO_TIMESTAMP(create_time)," +
                "watermark for rt as rt - interval '2' second" +
                ")with(" + MyKafkaUtil.getKafkaDDL(orderWideTopic, groupId) +
                ")");

        // TODO 3.计算订单数以及订单金额   分组、开窗、聚合
        Table resultTable = tableEnv.sqlQuery("select " +
                "DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' second), 'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' second), 'yyyy-MM-dd HH:mm:ss') edt," +
                "province_id," +
                "province_name," +
                "province_area_code," +
                "province_iso_code," +
                "province_3166_2_code," +
                "count(distinct order_id) order_count," +
                "sum(split_total_amount) order_amount," +
                // 作为版本判断依据
                "UNIX_TIMESTAMP()*1000 ts " +
                "from order_wide " +
                "group by " +
                "province_id," +
                "province_name," +
                "province_area_code," +
                "province_iso_code," +
                "province_3166_2_code," +
                "TUMBLE(rt, INTERVAL '10' second)");

//        resultTable.execute().print();

        // TODO 4.将动态表转为流
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv.toAppendStream(resultTable, ProvinceStats.class);

        // todo 打印测试
        provinceStatsDataStream.print("result>>>>");

        // TODO 5.将流数据写到clickhouse
        provinceStatsDataStream.addSink(ClickHouseUtil.getClickHouseSink("insert into province_stats_211025 values(?,?,?,?,?,?,?,?,?,?)"));

        // TODO 6.启动任务
        env.execute("ProvinceStatsSqlApp");
    }
}









