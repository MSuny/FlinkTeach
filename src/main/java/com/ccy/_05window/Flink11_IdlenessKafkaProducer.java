package com.ccy._05window;


import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.eclipse.jetty.util.ajax.JSON;


import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Future;

/**
 * https://www.modb.pro/db/180420
 */
public class Flink11_IdlenessKafkaProducer {

    public static void main(String[] args) {
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.130.11.208:9092,10.130.10.243:9092,10.130.11.207:9092");
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        prop.put(ProducerConfig.RETRIES_CONFIG, 0);
        prop.put(ProducerConfig.BATCH_SIZE_CONFIG, 4096);                 //默认的批量处理消息字节数
        prop.put(ProducerConfig.LINGER_MS_CONFIG, 1);                     //延迟等待发送时间
        prop.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 1024 * 1024 * 50);  //producer可以用来缓存数据的内存大小。

        //发送客户端
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(prop);

        //控制台输入
        Scanner scanner = new Scanner(System.in);
        System.out.println("请输入： 姓名,业务时间,分区");
        while (scanner.hasNext()) {
            String rs = scanner.nextLine();
            if (StringUtils.isBlank(rs)) {
                continue;
            }
            if (rs.equalsIgnoreCase("exit")) {
                kafkaProducer.close();
                return;
            }
            String[] split = rs.split(",");
            Map<String, Object> map = new HashMap<>(2);
            map.put("name", split[0]);
            map.put("ts", Long.valueOf(split[1]));
            Integer partition = Integer.valueOf(split[2]);

            //实际发送数据
            ProducerRecord record = new ProducerRecord("idleness_topic", partition, null, JSON.toString(map));
            Future send = kafkaProducer.send(record, (RecordMetadata data, Exception e) -> {
                System.out.println("数据发送成功！" + data.topic() + ":" + data.partition() + ":" + data.offset());
            });
            System.out.println("请输入下一条：");
        }

    }
}
