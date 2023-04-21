package com.ccy._02source.udsource;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink04_UDSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new RandomWatersensor()).print();
        env.execute();
    }
}
