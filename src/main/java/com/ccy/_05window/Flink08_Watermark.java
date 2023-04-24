package com.ccy._05window;

import com.ccy._00bean.WaterSensor;
import com.ccy._05window.util.MyUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * 在hadoop102的9999端口输入如下数据：
 * sensor1,1000,10
 * sensor1,2000,10
 * sensor1,3000,10
 * sensor1,4000,10
 * sensor1,6000,10
 * sensor1,6999,10
 * sensor1,7000,10
 * sensor1,12000,10
 * sensor1,11000,10
 * sensor1,12000,10
 * sensor1,17000,10
 * sensor1,13000,10
 * sensor1,22000,10
 */
public class Flink08_Watermark {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
//        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] data = line.split(",");
                    return new WaterSensor(
                            data[0],
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2])
                    );
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor waterSensor, long recordTimestamp) {
                                        return waterSensor.getTs();
                                    }
                                })
                )
                .keyBy(WaterSensor::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context ctx,
                                        Iterable<WaterSensor> waterSensors,
                                        Collector<String> out) throws Exception {
                        // 使用MyUtil工具类将Iterable转换为List
                        List<WaterSensor> waterSensorList = MyUtil.toList(waterSensors);
                        out.collect("key = " + key +
                                ", window=[" + ctx.window().getStart() +
                                ", " + ctx.window().getEnd() +
                                ") , 窗口内数据条数为：" + waterSensorList.size());
                    }
                })
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}