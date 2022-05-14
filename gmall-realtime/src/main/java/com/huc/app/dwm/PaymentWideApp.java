package com.huc.app.dwm;

import com.alibaba.fastjson.JSON;
import com.huc.bean.OrderWide;
import com.huc.bean.PaymentInfo;
import com.huc.bean.PaymentWide;
import com.huc.utils.DateTimeUtil;
import com.huc.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * 支付宽表的目的，
 * 最主要的原因是支付表没有精确到到订单明细，支付金额没有细分到商品上，没有办法统计商品级的支付状况。
 * 所以本次宽表的核心就是要把支付表的信息与订单宽表关联上。
 *
 * todo 支付表解决方法：
 * 	一个是把订单宽表输出到HBase上，在支付宽表计算时查询HBase，这相当于把订单宽表作为一种维度进行管理。
 * 	一个是用流的方式接收订单宽表，然后用双流join方式进行合并。因为订单与支付产生有一定的时差。所以必须用intervalJoin来管理流的状态时间，保证当支付到达时订单宽表还保存在状态中。
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        // TODO 1.获取流的执行环境
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

        // TODO 2.消费订单宽表和支付主题的数据创建流
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        DataStreamSource<String> paymentInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId));

        DataStreamSource<String> orderWideDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));

        // TODO 3.将每行数据转为JavaBean   并提取事件时间生成WaterMark
        // todo 将JSON格式数据转为JavaBean时，字段名相同即可，顺序无所谓，是根据字段来的！
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWaterMarkDS = paymentInfoDS.map(line -> JSON.parseObject(line, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                            @Override
                            public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                                return DateTimeUtil.toTs(element.getCreate_time());
                            }
                        }));

        SingleOutputStreamOperator<OrderWide> orderWideWithWaterMarkDS = orderWideDS.map(line -> JSON.parseObject(line, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                            @Override
                            public long extractTimestamp(OrderWide element, long recordTimestamp) {
                                return DateTimeUtil.toTs(element.getCreate_time());
                            }
                        }));

        // TODO 4.双流Join 注意延迟时间
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = orderWideWithWaterMarkDS
                .keyBy(OrderWide::getOrder_id)
                .intervalJoin(paymentInfoWithWaterMarkDS.keyBy(PaymentInfo::getOrder_id))
                .between(Time.seconds(-5), Time.minutes(15))
                .process(new ProcessJoinFunction<OrderWide, PaymentInfo, PaymentWide>() {
                    @Override
                    public void processElement(OrderWide left, PaymentInfo right, ProcessJoinFunction<OrderWide, PaymentInfo, PaymentWide>.Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(right, left));
                    }
                });

        paymentWideDS.print("paymentWideDS>>>>");

        // TODO 5.将数据写入Kafka
        paymentWideDS.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        // TODO 6.执行程序
        env.execute("PaymentWideApp");
    }
}
