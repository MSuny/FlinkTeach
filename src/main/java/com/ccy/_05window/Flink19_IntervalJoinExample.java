package com.ccy._05window;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Flink19_IntervalJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> orderStream = env
                .fromElements(
                        Tuple3.of("Mary", "order", 20 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, String, Long> e, long l) {
                                        return e.f2;
                                    }
                                })
                );

        SingleOutputStreamOperator<Tuple3<String, String, Long>> pvStream = env
                .fromElements(
                        Tuple3.of("Mary", "pv", 15 * 60 * 1000L),
                        Tuple3.of("Mary", "pv", 11 * 60 * 1000L),
                        Tuple3.of("Mary", "pv", 9 * 60 * 1000L),
                        Tuple3.of("Mary", "pv", 21 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, String, Long> e, long l) {
                                        return e.f2;
                                    }
                                })
                );

        // a -> b
        // (lower, higher)
        orderStream
                .keyBy(r -> r.f0)
                .intervalJoin(pvStream.keyBy(r -> r.f0))
                .between(Time.minutes(-10), Time.minutes(5))
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    @Override
                    public void processElement(Tuple3<String, String, Long> left, Tuple3<String, String, Long> right, Context context, Collector<String> collector) throws Exception {
                        collector.collect(left + "=>" + right);
                    }
                })
                .print();

        // b -> a
        // (-higher, -lower)
        pvStream
                .keyBy(r -> r.f0)
                .intervalJoin(orderStream.keyBy(r -> r.f0))
                .between(Time.minutes(-5), Time.minutes(10))
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    @Override
                    public void processElement(Tuple3<String, String, Long> left, Tuple3<String, String, Long> right, Context context, Collector<String> collector) throws Exception {
                        collector.collect(right + "->" + left);
                    }
                })
                .print();

        env.execute();
    }
}