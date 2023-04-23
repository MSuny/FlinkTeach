package com.ccy._03transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 需求：连接两个不同的流
 *
 * @author Hefei
 * @description </p>
 * @project_name com.atguigu.chapter05.transform
 * @since 2021/10/10 1:09
 */
public class Flink08_union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Integer> DS1 = env.fromElements(1, 2, 3, 4);
        DataStreamSource<Integer> DS2 = env.fromElements(55, 66, 77, 88);
        DataStreamSource<String> DS3 = env.fromElements("a", "b", "c");
        DataStreamSource<Integer> DS4 = env.fromElements(111, 222, 333, 444);

        /**
         * 1.union之前两个或多个流的类型必须是一样，connect可以不一样
         * 2.connect只能操作两个流，union可以操作多个。
         */
        DS1.union(DS2).union(DS4).print();
        env.execute("Flink07_connect");
    }
}