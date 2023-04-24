package com.ccy._05window;


import com.ccy._00bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;

import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * 输入：
 * sensor2,1,1
 * sensor222,1,1000
 * sensor1,1,1
 */
public class Flink06_SideOutput {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 10000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> main = env.socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] data = line.split(",");
                    return new WaterSensor(
                            data[0],
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2])
                    );
                })

                .process(new ProcessFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<WaterSensor> out) throws Exception {
                        // 将id为sensor1的数据放入主流，将id为sensor2的数据放入OutputTag为sensor2的侧流中，其他的放入other
                        if ("sensor1".equals(value.getId())) {
                            out.collect(value);
                        } else if ("sensor2".equals(value.getId())) {
                            // 侧输出流中的数据类型可以不相同
                            ctx.output(new OutputTag<String>("sensor2") {
                            }, "将sensor2的数据输出为String类型，数据为：" + value.toString());
                        } else {
                            ctx.output(new OutputTag<Integer>("other") {
                            }, value.getVc());
                        }
                    }
                });

        main.print("sensor1");
        main.getSideOutput(new OutputTag<String>("sensor2") {
        }).print("sensor2");
        main.getSideOutput(new OutputTag<Integer>("other") {
        }).print("other");


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}