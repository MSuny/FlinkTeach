package com.ccy._05window;

import com.ccy._00bean.WaterSensor;
import com.ccy._02source.udsource.RandomWatersensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.time.Duration;

/**
 * 代码中，我们提取数据中的ts字段作为时间戳，并且以5秒的延迟时间创建了处理乱序流的水位线生成器。
 */
public class Flink07_Watermark_OutOfOrderness {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
//        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.addSource(new RandomWatersensor())
                .assignTimestampsAndWatermarks(
                        // 针对乱序数据流插入水位线，乱序容忍度为5s
                        WatermarkStrategy
                                // 需要加上泛型信息  因为此处无法进行自动推断。
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                // 使用.withTimestampAssigner进行时间戳抽取
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    // 需要重写时间戳抽取方法extractTimestamp
                                    @Override
                                    public long extractTimestamp(WaterSensor waterSensor, long recordTimestamp) {
                                        return waterSensor.getTs();
                                    }
                                })

                )
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}