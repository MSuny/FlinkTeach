//package com.ccy._02source;
//
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//
//import java.nio.charset.StandardCharsets;
//import java.util.Properties;
//
//
//public class Flink03_Source_Kafka {
//    public static void main(String[] args) throws Exception {
//
//        // 创建kafka的配置类
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "服务器名1:9092,服务器名2:9092,服务器名3:9092");
//        properties.setProperty("group.id", "Flink03_Source_Kafka");
//        properties.setProperty("auto.offset.reset", "latest");
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 使用官方连接依赖中的FlinkKafkaConsumer创建kafka消费者来从kafka中获取数据
//        env.addSource(
//                new FlinkKafkaConsumer<>(
//                        "test",
//                        new SimpleStringSchema(StandardCharsets.UTF_8),
//                        properties)
//        )
//                .print();
//
//        env.execute("Flink03_Source_Kafka");
//    }
//}
