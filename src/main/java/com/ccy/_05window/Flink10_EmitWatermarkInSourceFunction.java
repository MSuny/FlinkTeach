package com.ccy._05window;

import com.ccy._00bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Calendar;
import java.util.Random;


public class Flink10_EmitWatermarkInSourceFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new RandomWatersensor2()).print();
        env.execute();
    }

    // 泛型是数据源中的类型
    public static class RandomWatersensor2 implements SourceFunction<WaterSensor> {
        private boolean running = true;

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            Random random = new Random();
            while (running) {
                WaterSensor waterSensor = new WaterSensor(
                        "sensor" + random.nextInt(5),
                        Calendar.getInstance().getTimeInMillis(),
                        random.nextInt(100));

                // 使用collectWithTimestamp方法将数据发送出去，并指明数据中的时间戳的字段
                ctx.collectWithTimestamp(waterSensor, waterSensor.getTs());
                // 发送水位线
                ctx.emitWatermark(new Watermark(waterSensor.getTs() - 1L));
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}
