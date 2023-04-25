package com.ccy._06state;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Random;

public class Flink03_AggregateStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env
                .addSource(new SourceFunction<Tuple2<String, Integer>>() {
                    private boolean running = true;
                    private Random random = new Random();
                    @Override
                    public void run(SourceContext<Tuple2<String, Integer>> sourceContext) throws Exception {
                        while (true) {
                            sourceContext.collect(Tuple2.of("key", random.nextInt(10)));
                            Thread.sleep(500);
                        }
                    }

                    @Override
                    public void cancel() {
                        running = false;
                    }
                })
                .keyBy(r -> r.f0)
                .flatMap(new CountFunction())
                .print();
        env.execute();
    }

    public static class CountFunction extends RichFlatMapFunction<Tuple2<String, Integer>, Integer> {
        private int count = 0;
        // 声明聚合状态变量
        private AggregatingState<Tuple2<String, Integer>, Integer> aggregatingState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            AggregatingStateDescriptor<Tuple2<String, Integer>, Integer, Integer> descriptor = new AggregatingStateDescriptor<Tuple2<String, Integer>, Integer, Integer>(
                    "aggregatingState", new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
                @Override
                public Integer createAccumulator() {
                    return 0;
                }

                @Override
                public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                    return accumulator + 1;
                }

                @Override
                public Integer getResult(Integer accumulator) {
                    return accumulator;
                }

                // 仅session窗口调用
                @Override
                public Integer merge(Integer a, Integer b) {
                    return a + b;
                }
            }, Types.INT);
            aggregatingState = getRuntimeContext().getAggregatingState(descriptor);
        }

        /**
         *
         * @param value  (key,0)
         * @param out  aggregatingState.get()的值
         * @throws Exception
         */
        @Override
        public void flatMap(Tuple2<String, Integer> value, Collector<Integer> out) throws Exception {
            count++;
            // 第10个数清零
            if (count % 10 == 0) {
                out.collect(aggregatingState.get());
                aggregatingState.clear(); // 清空状态变量
            } else {
                // 增量更新AggregatingState，这里每来一个新元素，对ACC累加1
                aggregatingState.add(value);
                System.out.println(getRuntimeContext().getNumberOfParallelSubtasks()+"-------"+aggregatingState.get());
                System.out.println(value);
            }
        }
    }
}