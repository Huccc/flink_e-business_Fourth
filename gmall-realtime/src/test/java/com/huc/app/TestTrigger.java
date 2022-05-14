package com.huc.app;

import com.huc.bean.Sensor;
import com.huc.trigger.MyTrigger;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

public class TestTrigger {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

//        streamSource.map(line -> {
//                    String[] split = line.split(",");
//                    return new Sensor(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
//                })
//                .assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<Sensor>forMonotonousTimestamps()
//                        .withTimestampAssigner(new SerializableTimestampAssigner<Sensor>() {
//                            @Override
//                            public long extractTimestamp(Sensor element, long recordTimestamp) {
//                                return element.getTs();
//                            }
//                        })).keyBy(Sensor::getId)
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
//                .trigger(new MyTrigger(40000L))
//                .sum("vc")
//                .print();

        WindowedStream<Sensor, String, TimeWindow> result = streamSource.map(line -> {
                    String[] split = line.split(",");
                    return new Sensor(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Sensor>forBoundedOutOfOrderness(Duration.ofSeconds(20))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Sensor>() {
                            @Override
                            public long extractTimestamp(Sensor sensor, long recordTimestamp) {
                                return sensor.getTs();
                            }
                        })).keyBy(Sensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .trigger(new MyTrigger(40000L));

        result.sum("vc").print();

        env.execute();
    }
}
