package com.ccy._05window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
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

/**
 * @description 使用Aggregate计算传入的数据的平均值  数据样式为 a,2
 */
public class Flink05_Window_Aggregate {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
                .socketTextStream("localhost", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {

                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] data = line.split(",");
                        out.collect(Tuple2.of(data[0], Long.valueOf(data[1])));
                    }
                })
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))

                // ACC我们定义为Tuple2<Long,Long> 使用元组中的第一个值保存数据的和 第二个值保存数据个数
                // 输出类型我们定义为Double 将ACC中的数值相除求平均值。
                .aggregate(new AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double>() {

                    // 创建一个累加器，这就是为聚合创建了一个初始状态，每个聚合任务只会调用一次。
                    @Override
                    public Tuple2<Long, Long> createAccumulator() {
                        System.out.println("Flink07_Window_Aggregate.createAccumulator");
                        return Tuple2.of(0L, 0L);
                    }

                    // 将输入的元素与累加器进行累加
                    @Override
                    public Tuple2<Long, Long> add(Tuple2<String, Long> value,
                                                  Tuple2<Long, Long> accumulator) {
                        System.out.println("Flink07_Window_Aggregate.add");
                        return Tuple2.of(value.f1 + accumulator.f0, accumulator.f1 + 1);
                    }

                    // 从累加器中提取聚合的输出结果。
                    @Override
                    public Double getResult(Tuple2<Long, Long> accumulator) {
                        System.out.println("Flink07_Window_Aggregate.getResult");
                        return accumulator.f0 * 1.0 / accumulator.f1;
                    }

                    // 合并两个累加器，并将合并后的状态作为一个累加器返回。只有会话窗口会用到 其他窗口用不到。会话窗口会对窗口进行合并。
                    @Override
                    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a,
                                                    Tuple2<Long, Long> b) {
                        System.out.println("Flink07_Window_Aggregate.merge");
                        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                    }
                }, new ProcessWindowFunction<Double, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<Double> elements,
                                        Collector<String> out) throws Exception {
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
                        // 因为窗口中的所有数据经过aggregate方法之后 只有一个结果
                        // 所以进入process方法的数据只有一个，我们直接用next方法取出。
                        System.out.println("Flink06_Window_Reduce.process");
                        Double result = elements.iterator().next();
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