package com.ccy._05window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;

import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;


/**
 * @author Hefei
 * @description
 * @project_name FlinkTutorial
 * @package_name com.atguigu.chapter06
 */
public class Flink03_Window_Sliding {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(2);

        env.socketTextStream("localhost", 9999)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {

                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        for (String word : value.split(" ")) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                })
                .keyBy(t -> t.f0)
                // window一般用在keyby之后
                // 创建一个5秒的窗口 4秒的滑动步长  只计算窗口内的数据  窗口结束的时候才开始计算
                .window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(4)))
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {

                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<Tuple2<String, Long>> elements, // 这个窗口内所有的元素 每个key有自己的窗口
                                        Collector<String> out)
                            throws Exception {
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS");
                        ArrayList<String> words = new ArrayList<>();
                        for (Tuple2<String, Long> word : elements) {
                            words.add(word.f0);

                        }
                        out.collect("数据为" +
                                words +
                                "时间窗口为：[" +
                                simpleDateFormat.format(new Date(ctx.window().getStart())) +
                                "," +
                                simpleDateFormat.format(new Date(ctx.window().getEnd())) +
                                ")");
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