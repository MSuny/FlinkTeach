package com.ccy._03transformation;

import com.ccy._00bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Hefei
 * @description
 * @project_name flink
 * @package_name com.atguigu.chapter05.transform
 * @since 2021/11/7 14:38
 */
public class Flink11_Process_keyBy {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        // 注意当我们设置多个并行度时候，求和发生在每个并行度中
        env.setParallelism(2);
        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));
        waterSensors.add(new WaterSensor("sensor_3", 1607527997000L, 100));
        DataStreamSource<WaterSensor> waterSensorDS = env.fromCollection(waterSensors);
        waterSensorDS
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    HashMap<String, Integer> sums = new HashMap<String, Integer>();

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<WaterSensor> out) throws Exception {

                        Integer sum = sums.get(value.getId());
                        if (sum == null) {
                            sum = 0;
                        }
                        sum += value.getVc();
                        sums.put(value.getId(), sum);

                        value.setVc(sum);
                        out.collect(value);
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