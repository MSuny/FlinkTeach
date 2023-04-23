package com.ccy._03transformation;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * 需求：连接两个不同的流
 *
 * @author Hefei
 * @description </p>
 * @project_name com.atguigu.chapter05.transform
 * @since 2021/10/10 1:09
 */
public class Flink07_connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Integer> DS1 = env.fromElements(1, 2, 3, 4);
        DataStreamSource<String> DS2 = env.fromElements("a", "b", "c");

        ConnectedStreams<Integer, String> connect = DS1.connect(DS2);

        connect.map(new CoMapFunction<Integer, String, String>() {
            @Override
            public String map1(Integer value) throws Exception {
                return value + "00000";
            }

            @Override
            public String map2(String value) throws Exception {
                return value + "11111";
            }
        }).print();


        env.execute("Flink07_connect");
    }
}

