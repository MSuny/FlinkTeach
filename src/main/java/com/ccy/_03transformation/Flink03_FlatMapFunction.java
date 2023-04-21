package com.ccy._03transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 需求：使用FlatMap算子将数字转换为数字,数字的平方,数字的立方并重新放回流中
 */
public class Flink03_FlatMapFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> DS = env.fromElements(1, 2, 3, 4);

        DS.flatMap(new FlatMapFunction<Integer, Integer>() {
            @Override
            public void flatMap(Integer value, Collector<Integer> out) throws Exception {
                out.collect(value);
                out.collect(value * value);
                out.collect(value * value * value);
            }
        }).print();

        env.execute("Flink03_FlatMapFunction");
    }
}
