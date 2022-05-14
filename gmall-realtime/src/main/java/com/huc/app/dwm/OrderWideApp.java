package com.huc.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.huc.app.func.DimAsyncFunction;
import com.huc.bean.OrderDetail;
import com.huc.bean.OrderInfo;
import com.huc.bean.OrderWide;
import com.huc.utils.DateTimeUtil;
import com.huc.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

/**
 * todo 订单宽表
 * 订单是统计分析的重要的对象，围绕订单有很多的维度统计需求，比如用户、地区、商品、品类、品牌等等。
 * 为了之后统计计算更加方便，减少大表之间的关联，所以在实时计算过程中将围绕订单的相关数据整合成为一张订单的宽表。
 * 那究竟哪些数据需要和订单整合在一起？
 * <p>
 * todo 在之前的操作我们已经把数据分拆成了事实数据和维度数据，
 * 事实数据进入kafka数据流（DWD层）中，
 * 维度数据（蓝色）进入hbase中长期保存。
 * 那么我们在DWM层中要把实时和维度数据进行整合关联在一起，形成宽表。
 * 那么这里就要处理有两种关联，事实数据和事实数据关联、事实数据和维度数据关联。
 * 	todo 事实数据和事实数据关联，其实就是流与流之间的关联。
 * 	todo 事实数据与维度数据关联，其实就是流计算中查询外部数据源。
 */
public class OrderWideApp {
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

        // TODO 2.读取Kafka 订单和订单明细主题数据创建流
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group_211025";

        // todo 数据在baseDbApp写入Kafka过程中只写入了 after数据 {"id":"1001","name":"zs",...},
        DataStreamSource<String> orderInfoKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId));
        DataStreamSource<String> orderDetailKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId));

        // TODO 3.将每行数据转换为Javabean对象并提取时间戳
        // todo 转换订单表
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWaterMarkDS = orderInfoKafkaDS.map(line -> {
                    OrderInfo orderInfo = JSON.parseObject(line, OrderInfo.class);

                    // todo create_time 时间格式为  yyyy-MM-dd HH:mm:ss
                    String[] create_time = orderInfo.getCreate_time().split(" ");
                    orderInfo.setCreate_date(create_time[0]);
                    orderInfo.setCreate_hour(create_time[1].split(":")[0]);

                    // 获取时间戳字段
                    orderInfo.setCreate_ts(DateTimeUtil.toTs(orderInfo.getCreate_time()));

                    return orderInfo;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<OrderInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                                return orderInfo.getCreate_ts();
                            }
                        }));

        // todo 转换订单详情表
        SingleOutputStreamOperator<OrderDetail> orderDetailWithWaterMarkDS = orderDetailKafkaDS.map(line -> {
                    OrderDetail orderDetail = JSON.parseObject(line, OrderDetail.class);

                    orderDetail.setCreate_ts(DateTimeUtil.toTs(orderDetail.getCreate_time()));

                    return orderDetail;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<OrderDetail>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail orderDetail, long recordTimestamp) {
                                return orderDetail.getCreate_ts();
                            }
                        }));

        // TODO 4.双流Join
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoWithWaterMarkDS
                .keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailWithWaterMarkDS.keyBy(OrderDetail::getOrder_id))
                // 给定公司中的最大延迟时间
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });

//        orderWideDS.print("orderWideDS>>>>");

        // TODO 5.关联维表
        // 采用异步方式关联维度  可提高查询速度  提高效率

        // todo 5.1关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(orderWideDS,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws ParseException {

                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

                        // 取出用户维度中的生日
                        String birthday = jsonObject.getString("BIRTHDAY");
                        long CurrentTs = System.currentTimeMillis();
                        long ts = sdf.parse(birthday).getTime();

                        // 将生日字段处理成年纪
                        long ageLong = (CurrentTs - ts) / 1000L / 3600 / 24 / 365;
                        orderWide.setUser_age((int) ageLong);

                        // 取出用户维度中的性别
                        String gender = jsonObject.getString("GENDER");
                        orderWide.setUser_gender(gender);

                    }
                }, 70, TimeUnit.SECONDS);

//        orderWideWithUserDS.print("withUserDS>>>>");

//        // todo 5.2关联地区维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        // 提取维度信息 写入orderwide
                        orderWide.setProvince_name(jsonObject.getString("NAME"));
                        orderWide.setProvince_area_code(jsonObject.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(jsonObject.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(jsonObject.getString("ISO_3166_2"));
                    }
                }, 70, TimeUnit.SECONDS);

//        orderWideWithProvinceDS.print("withProvince>>>>");

        // todo 5.3关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(orderWideWithProvinceDS,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setSku_name(jsonObject.getString("SKU_NAME"));
                        orderWide.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(jsonObject.getLong("SPU_ID"));
                        orderWide.setTm_id(jsonObject.getLong("TM_ID"));
                    }
                }, 70, TimeUnit.SECONDS);

//        orderWideWithSkuDS.print(">>>>");

        // todo 5.4关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(orderWideWithSkuDS,
                new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getSpu_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }
                }, 70, TimeUnit.SECONDS);

//        orderWideWithSpuDS.print(">>>>");

        // todo 5.5关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(orderWideWithSpuDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getTm_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }
                }, 70, TimeUnit.SECONDS);

        // todo 5.6关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3_idDS = AsyncDataStream.unorderedWait(orderWideWithTmDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(OrderWide input) {
                        return input.getCategory3_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }
                }, 70, TimeUnit.SECONDS);

        orderWideWithCategory3_idDS.print("orderWideDS>>>>");

        // TODO 6.将数据写入Kafka主题
        orderWideWithCategory3_idDS.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));

        // TODO 7.启动任务
        env.execute("OrderWideApp");
    }
}
