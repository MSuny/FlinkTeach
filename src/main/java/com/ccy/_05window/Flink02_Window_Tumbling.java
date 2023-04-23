package com.ccy._05window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;


public class Flink02_Window_Tumbling {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        // TODO 设置本地webUI端口号
        conf.setInteger("rest.port", 8050);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);

        env.socketTextStream("192.168.31.208", 9999)
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
                // 创建一个5秒的滚动窗口 只计算窗口内的数据  窗口结束的时候才开始计算 可以在of内加入offset（多数用来时区处理）
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
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
                                "时间窗口为：[" + simpleDateFormat.format(new Date(ctx.window().getStart())) +
                                "," + simpleDateFormat.format(new Date(ctx.window().getEnd())) +
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
