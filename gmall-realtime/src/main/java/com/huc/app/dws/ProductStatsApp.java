package com.huc.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.huc.app.func.DimAsyncFunction;
import com.huc.bean.OrderWide;
import com.huc.bean.PaymentWide;
import com.huc.bean.ProductStats;
import com.huc.common.GmallConstant;
import com.huc.utils.ClickHouseUtil;
import com.huc.utils.DateTimeUtil;
import com.huc.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * 与访客的dws层的宽表类似，也是把多个事实表的明细数据汇总起来组合成宽表。
 * Desc: 形成以商品为准的统计
 * 曝光  点击  收藏  购物车  下单  支付  退单  评价
 * 宽表
 */
public class ProductStatsApp {
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

        // TODO 2.读取Kafka主题的数据创建流
        String groupId = "product_stats_app";

        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        DataStreamSource<String> pageViewKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));
        DataStreamSource<String> favorInfoKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId));
        DataStreamSource<String> cartInfoKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId));
        DataStreamSource<String> orderWideKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));
        DataStreamSource<String> paymentWideKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId));
        DataStreamSource<String> refundInfoKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId));
        DataStreamSource<String> commentInfoKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId));

        // TODO 3.将每个流转换成统一的Javabean
        // todo 3.1 转换点击和曝光数据
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
        // todo 用于计算曝光数和点击数
        SingleOutputStreamOperator<ProductStats> pageWithDisplayDS = pageViewKafkaDS.flatMap(new FlatMapFunction<String, ProductStats>() {
            @Override
            public void flatMap(String value, Collector<ProductStats> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);
                // 获取时间戳字段
                Long ts = jsonObject.getLong("ts");

                // todo 取出页面信息
                JSONObject page = jsonObject.getJSONObject("page");

                if ("good_detail".equals(page.getString("page_id")) && "sku_id".equals(page.getString("item_type"))) {
                    ProductStats productStats = ProductStats.builder()
                            .sku_id(page.getLong("item"))
                            .click_ct(1L)
                            .ts(ts)
                            .build();
                    out.collect(productStats);
                }

                // todo 获取曝光数据
                JSONArray displays = jsonObject.getJSONArray("displays");
                if (displays != null && displays.size() > 0) {
                    // 遍历曝光数据
                    for (int i = 0; i < displays.size(); i++) {
                        // 获取每一条曝光数据
                        JSONObject display = displays.getJSONObject(i);

                        if ("sku_id".equals(display.getString("item_type"))) {
                            out.collect(ProductStats.builder()
                                    .sku_id(display.getLong("item"))
                                    .display_ct(1L)
                                    .ts(ts)
                                    .build());
                        }
                    }
                }
            }
        });

        // todo 3.2 转换收藏数据
        SingleOutputStreamOperator<ProductStats> favorInfoDS = favorInfoKafkaDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .favor_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // todo 3.3 转换加购(添加购物车)数据
        SingleOutputStreamOperator<ProductStats> cartInfoDS = cartInfoKafkaDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .cart_ct(1L)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // todo 3.4 转换订单数据
        SingleOutputStreamOperator<ProductStats> orderWideDS = orderWideKafkaDS.map(line -> {
            OrderWide orderWide = JSON.parseObject(line, OrderWide.class);

            // 用于存放下单订单号
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(orderWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(orderWide.getSku_id())
                    // 下单商品个数
                    .order_sku_num(orderWide.getSku_num())
                    // 下单商品金额
                    .order_amount(orderWide.getSplit_total_amount())
                    // 存放订单的集合
                    .orderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(orderWide.getCreate_time()))
                    .build();
        });

        // todo 3.5 转换支付数据
        SingleOutputStreamOperator<ProductStats> paymentWideDS = paymentWideKafkaDS.map(line -> {
            PaymentWide paymentWide = JSON.parseObject(line, PaymentWide.class);

            // 用于存放支付订单号
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(paymentWide.getOrder_id());

            return ProductStats.builder()
                    .sku_id(paymentWide.getSku_id())
                    // 支付金额
                    .payment_amount(paymentWide.getSplit_total_amount())
                    .paidOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(paymentWide.getPayment_create_time()))
                    .build();
        });

        // todo 3.6 转换退单数据
        SingleOutputStreamOperator<ProductStats> refundInfoDS = refundInfoKafkaDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            // 用于存放退单 订单号
            HashSet<Long> orderIds = new HashSet<>();
            orderIds.add(jsonObject.getLong("order_id"));

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    .refund_amount(jsonObject.getBigDecimal("refund_amount"))
                    .refundOrderIdSet(orderIds)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // todo 3.7 转换评价数据
        SingleOutputStreamOperator<ProductStats> commentInfoDS = commentInfoKafkaDS.map(line -> {
            JSONObject jsonObject = JSON.parseObject(line);

            long goodct = 0L;
            if (GmallConstant.APPRAISE_GOOD.equals(jsonObject.getString("appraise"))) {
                goodct = 1L;
            }

            return ProductStats.builder()
                    .sku_id(jsonObject.getLong("sku_id"))
                    // 评论订单数
                    .comment_ct(1L)
                    // 好评订单数
                    .good_comment_ct(goodct)
                    .ts(DateTimeUtil.toTs(jsonObject.getString("create_time")))
                    .build();
        });

        // TODO 4.union各个流
        DataStream<ProductStats> unionDS = pageWithDisplayDS.union(favorInfoDS, cartInfoDS, orderWideDS, paymentWideDS, refundInfoDS, commentInfoDS);

        // TODO 5.提取事件时间生成waterMark
        SingleOutputStreamOperator<ProductStats> productStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy
                .<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<ProductStats>() {
                    @Override
                    public long extractTimestamp(ProductStats productStats, long recordTimestamp) {
                        return productStats.getTs();
                    }
                }));

        // TODO 6.分组、开窗、聚合
        // todo 分组、开窗
        WindowedStream<ProductStats, Long, TimeWindow> windowedStream = productStatsWithWatermarkDS.keyBy(ProductStats::getSku_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)));

        // todo 聚合
        SingleOutputStreamOperator<ProductStats> productReduceDS = windowedStream.reduce(new ReduceFunction<ProductStats>() {
            @Override
            public ProductStats reduce(ProductStats value1, ProductStats value2) throws Exception {
                value1.setDisplay_ct(value1.getDisplay_ct() + value2.getDisplay_ct());
                value1.setClick_ct(value1.getClick_ct() + value2.getClick_ct());
                value1.setCart_ct(value1.getCart_ct() + value2.getCart_ct());
                value1.setFavor_ct(value1.getFavor_ct() + value2.getFavor_ct());
                value1.setOrder_amount(value1.getOrder_amount().add(value2.getOrder_amount()));
                value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                //value1.setOrder_ct(value1.getOrderIdSet().size() + 0L);
                value1.setOrder_sku_num(value1.getOrder_sku_num() + value2.getOrder_sku_num());
                value1.setPayment_amount(value1.getPayment_amount().add(value2.getPayment_amount()));

                value1.getRefundOrderIdSet().addAll(value2.getRefundOrderIdSet());
                //value1.setRefund_order_ct(value1.getRefundOrderIdSet().size() + 0L);
                value1.setRefund_amount(value1.getRefund_amount().add(value2.getRefund_amount()));

                value1.getPaidOrderIdSet().addAll(value2.getPaidOrderIdSet());
                //value1.setPaid_order_ct(value1.getPaidOrderIdSet().size() + 0L);

                value1.setComment_ct(value1.getComment_ct() + value2.getComment_ct());
                value1.setGood_comment_ct(value1.getGood_comment_ct() + value2.getGood_comment_ct());
                return value1;
            }
        }, new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
            @Override
            public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {
                // 从迭代器中取出数据
                ProductStats productStats = input.iterator().next();

                // 设置窗口信息
                productStats.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                productStats.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));

                // 补充三个订单次数指标
                productStats.setOrder_ct((long) productStats.getOrderIdSet().size());
                productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());
                productStats.setRefund_order_ct((long) productStats.getRefundOrderIdSet().size());

                out.collect(productStats);
            }
        });

        // TODO 7.关联维度信息
        // todo 7.1 SKU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(productReduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                        productStats.setSku_name(jsonObject.getString("SKU_NAME"));
                        productStats.setSku_price(jsonObject.getBigDecimal("PRICE"));
                        productStats.setSpu_id(jsonObject.getLong("SPU_ID"));
                        productStats.setTm_id(jsonObject.getLong("TM_ID"));
                        productStats.setCategory3_id(jsonObject.getLong("CATEGORY3_ID"));
                    }
                }, 70, TimeUnit.SECONDS);

        // todo 7.2 SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSpu_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                        productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }
                }, 70, TimeUnit.SECONDS);

        // todo 7.3 品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS = AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getCategory3_id().toString();
                    }

                    @Override
                    public void join(ProductStats input, JSONObject jsonObject) throws Exception {
                        input.setCategory3_name(jsonObject.getString("NAME"));
                    }
                }, 70, TimeUnit.SECONDS);

        // todo 7.4 品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTradeMarkDS = AsyncDataStream.unorderedWait(productStatsWithCategory3DS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                    @Override
                    public String getKey(ProductStats input) {
                        return input.getTm_id().toString();
                    }

                    @Override
                    public void join(ProductStats input, JSONObject jsonObject) throws Exception {
                        input.setTm_name(jsonObject.getString("TM_NAME"));
                    }
                }, 70, TimeUnit.SECONDS);

        // TODO 8.将数据写入ClickHouse
        productStatsWithTradeMarkDS.print("ProductStatsApp>>>>");

        productStatsWithTradeMarkDS.addSink(ClickHouseUtil.getClickHouseSink("insert into product_stats_211025 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        // TODO 9.启动任务
        env.execute("ProductStatsApp");
    }
}

