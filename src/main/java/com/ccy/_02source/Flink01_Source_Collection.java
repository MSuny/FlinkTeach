package com.ccy._02source;


import com.ccy._00bean.WaterSensor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


import java.util.Arrays;
import java.util.List;

/**
 *
 * @author Hefei
 * @project_name com.atguigu.chapter05.sink
 * @since 2021/10/9 23:04
 */
public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {
        // 准备数据
        List<WaterSensor> waterSensors = Arrays.asList(
                new WaterSensor("sensor_1", 1633792000L, 10),
                new WaterSensor("sensor_2", 1633794000L, 50),
                new WaterSensor("sensor_3", 1633796000L, 40)
        );
        List<Integer> data = Arrays.asList(1, 2, 3, 4);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 可以使用fromCollection从集合中获取数据
         env.fromCollection(waterSensors).print();
         env.fromCollection(data).print();

         // TODO 观察三者打印顺序

        // 也可以使用fromElements直接传入数据
        env.fromElements(
                new WaterSensor("sensor_1_", 1633792000L, 10),
                new WaterSensor("sensor_2_", 1633794000L, 50),
                new WaterSensor("sensor_3_", 1633796000L, 40))
                .print();
        env.execute();
    }
}