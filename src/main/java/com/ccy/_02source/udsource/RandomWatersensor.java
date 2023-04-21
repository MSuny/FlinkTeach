package com.ccy._02source.udsource;

import com.ccy._00bean.WaterSensor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class RandomWatersensor implements SourceFunction<WaterSensor> {
    private Boolean running = true;

    @Override
    public void run(SourceContext<WaterSensor> ctx) throws Exception {
        Random random = new Random();
        while (running) {
            ctx.collect(new WaterSensor(
                    "sensor" + random.nextInt(50),
                    Calendar.getInstance().getTimeInMillis(),
                    random.nextInt(100)
            ));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;

    }
}