package com.ccy._05window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;

import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Flink04_Window_Reduce {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
                .socketTextStream("localhost", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        for (String word : value.split(" ")) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                })
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1,
                                                               Tuple2<String, Long> value2) throws Exception {
                                System.out.println("Flink06_Window_Reduce.reduce");
                                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                            }
                        },
                        // ProcessWindowFunction是在reduce方法完成整个窗口计算之后 对reduce最终的结果进行处理
                        new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                            @Override
                            public void process(String key,
                                                Context ctx,
                                                Iterable<Tuple2<String, Long>> elements,
                                                Collector<String> out) throws Exception {
                                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
                                // 因为窗口中的所有数据经过reduce方法之后 只有一个结果
                                // 所以进入process方法的数据只有一个，我们直接用next方法取出。
                                System.out.println("Flink06_Window_Reduce.process");
                                Tuple2<String, Long> result = elements.iterator().next();
                                out.collect("数据为" +
                                        result +
                                        "。时间窗口为：[" +
                                        simpleDateFormat.format(new Date(ctx.window().getStart())) +
                                        "," +
                                        simpleDateFormat.format(new Date(ctx.window().getEnd())) +
                                        ")"
                                );
                            }
                        })
                .print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
