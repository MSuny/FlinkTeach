package com.ccy._05window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class Flink13_ProcessingTimeTimerExampleKeyed {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new CustomSource())
                .keyBy(r -> true)
                .process(new KeyedProcessFunction<Boolean, String, String>() {
                    // 每来一条数据都会调用一次
                    @Override
                    public void processElement(String s, Context context, Collector<String> collector) throws Exception {
                        long currTs = context.timerService().currentProcessingTime();
                        collector.collect("数据到达，到达时间是：" + new Timestamp(currTs));
                        // 注册10s之后的定时器
                        context.timerService().registerProcessingTimeTimer(currTs + 10 * 1000L);
                    }

                    // 定时器触发时的操作
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("定时器触发，触发时间是：" + new Timestamp(timestamp));
                    }
                })
                .print();

        env.execute();
    }

    public static class CustomSource implements SourceFunction<String> {
        @Override
        public void run(SourceFunction.SourceContext<String> ctx) throws Exception {
            ctx.collect("a");
            // 为了让程序不直接退出，等待20秒
            Thread.sleep(20 * 1000L);
        }

        @Override
        public void cancel() {
        }
    }
}
