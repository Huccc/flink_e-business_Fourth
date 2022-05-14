package com.huc.app.dws;

/**
 * 设计一张DWS层的表其实就两件事：维度和度量(事实数据)
 * 	度量包括PV、UV、跳出次数、进入页面数(session_count)、连续访问时长
 * 	维度包括在分析中比较重要的几个字段：渠道、地区、版本、新老用户进行聚合
 * <p>
 * 	接收各个明细数据，变为数据流
 * 	把数据流合并在一起，成为一个相同格式对象的数据流
 * 	对合并的流进行聚合，聚合的时间窗口决定了数据的时效性
 * 	把聚合结果写在数据库中
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.huc.bean.VisitorStats;
import com.huc.utils.ClickHouseUtil;
import com.huc.utils.DateTimeUtil;
import com.huc.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * Desc: 访客主题宽表计算
 * <p>
 * 要不要把多个明细的同样的维度统计在一起?
 * 因为单位时间内mid的操作数据非常有限不能明显的压缩数据量（如果是数据量够大，或者单位时间够长可以）
 * todo 所以用常用统计的四个维度进行聚合 渠道、新老用户、app版本、省市区域
 * todo 度量值包括 启动、日活（当日首次启动）、访问页面数、新增用户数、跳出数、平均页面停留时长、总访问时长
 * 聚合窗口： 10秒
 * <p>
 * 各个数据在维度聚合前不具备关联性，所以先进行维度聚合
 * 进行关联  这是一个fulljoin
 * 可以考虑使用flinksql 完成
 */

public class VisitorStatsApp {
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

        // TODO 2.读取三个主题的数据创建流
        String groupId = "visitor_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        DataStreamSource<String> pageViewKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));
        DataStreamSource<String> uniqueVisitKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId));
        DataStreamSource<String> userJumpKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId));

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
        // TODO 3.将每个流转换为相同的数据格式 Javabean
        // todo 转换page_log_datestream
        SingleOutputStreamOperator<VisitorStats> pageDS = pageViewKafkaDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            long sv = 0;
            if (jsonObject.getJSONObject("page").getString("last_page_id") == null) {
                sv = 1L;
            }

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L, // 独立访客数
                    1L, // 页面访问数
                    sv, // 进入次数
                    0L, // 跳出次数
                    jsonObject.getJSONObject("page").getLong("during_time"),
                    jsonObject.getLong("ts")
            );
        });

        // todo 转换uniqueVisitKafkaDS
        SingleOutputStreamOperator<VisitorStats> uvDS = uniqueVisitKafkaDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");


            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    1L,
                    0L,
                    0L,
                    0L,
                    0L,
                    jsonObject.getLong("ts"));
        });

        // todo 转换userJumpKafkaDS
        SingleOutputStreamOperator<VisitorStats> ujDS = userJumpKafkaDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);
            JSONObject common = jsonObject.getJSONObject("common");

            return new VisitorStats("", "",
                    common.getString("vc"),
                    common.getString("ch"),
                    common.getString("ar"),
                    common.getString("is_new"),
                    0L,
                    0L,
                    0L,
                    1L,
                    0L,
                    jsonObject.getLong("ts"));
        });

        // TODO 4.合并三个流的数据
        DataStream<VisitorStats> unionDS = pageDS.union(uvDS, ujDS);

        // TODO 5.提取事件时间生成WaterMark
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWaterMarkDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(15))
                .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                    @Override
                    public long extractTimestamp(VisitorStats visitorStats, long recordTimestamp) {
                        return visitorStats.getTs();
                    }
                }));

        // TODO 6.分组、开窗、聚合
        // todo 分组、开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = visitorStatsWithWaterMarkDS.keyBy(new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> getKey(VisitorStats visitorStats) throws Exception {
                return Tuple4.of(visitorStats.getAr(), visitorStats.getCh(), visitorStats.getVc(), visitorStats.getIs_new());
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .trigger()
                ;

        // todo 聚合
        // todo 增量聚合  数据少 算的快
//        windowStream.reduce(new ReduceFunction<VisitorStats>() {
//            @Override
//            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
//                return null;
//            }
//        });

        // todo 全量聚合  可以求前百分比  可以获取窗口信息
//        windowStream.apply(new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
//            @Override
//            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
//                window.getStart();
//                window.getEnd();
//            }
//        });

        // todo flink 提供了，结合两种聚合的聚合方式
        SingleOutputStreamOperator<VisitorStats> reduceResult = windowDS.reduce(new ReduceFunction<VisitorStats>() {
            // 增量聚合
            @Override
            public VisitorStats reduce(VisitorStats value1, VisitorStats value2) throws Exception {
                value1.setUv_ct(value1.getUv_ct() + value2.getUv_ct());
                value1.setPv_ct(value1.getPv_ct() + value2.getPv_ct());
                value1.setSv_ct(value1.getSv_ct() + value2.getSv_ct());
                value1.setUj_ct(value1.getUj_ct() + value2.getUj_ct());
                value1.setDur_sum(value1.getDur_sum() + value2.getDur_sum());

                return value1;
            }
        }, new WindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void apply(Tuple4<String, String, String, String> stringStringStringStringTuple4, TimeWindow window, Iterable<VisitorStats> input, Collector<VisitorStats> out) throws Exception {
                // 从迭代器中取出数据
                VisitorStats visitorStats = input.iterator().next();

                // 设置窗口信息
                visitorStats.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                visitorStats.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));

                out.collect(visitorStats);
            }
        });

        // TODO 7.将数据写入ClickHouse
        reduceResult.print("result>>>>");
        reduceResult.addSink(ClickHouseUtil.getClickHouseSink("insert into visitor_stats_211025 values(?,?,?,?,?,?,?,?,?,?,?,?)"));

        // TODO 8.启动任务
        env.execute("VisitorStatsApp");
    }
}
