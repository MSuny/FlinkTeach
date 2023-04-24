package com.ccy._05window;

import com.ccy._00bean.WaterSensor;
import com.ccy._02source.udsource.RandomWatersensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Flink09_CustomPeriodicWatermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new RandomWatersensor())
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
                .print();

        env.execute();
    }

    public static class CustomWatermarkStrategy implements WatermarkStrategy<WaterSensor> {
        @Override
        public TimestampAssigner<WaterSensor> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<WaterSensor>() {
                @Override
                public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                    // 告诉程序数据源里的时间戳是哪一个字段
                    return element.getTs();
                }
            };
        }

        @Override
        public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomBoundedOutOfOrdernessGenerator();
        }
    }

    public static class CustomBoundedOutOfOrdernessGenerator implements WatermarkGenerator<WaterSensor> {
        // 延迟时间
        private Long delayTime = 5000L;
        // 观察到的最大时间戳
        private Long maxTs = -Long.MAX_VALUE + delayTime + 1L;

        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            // 每来一条数据就调用一次
            // 更新最大时间戳
            maxTs = Math.max(event.getTs(), maxTs);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 发射水位线，默认200ms调用一次
            output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }
}