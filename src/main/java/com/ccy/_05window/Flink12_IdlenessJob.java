//package com.ccy._05window;
//
//
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.tuple.Tuple4;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
//import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
//import org.apache.flink.util.Collector;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//
//import javax.naming.Context;
//import java.io.Serializable;
//import java.time.Duration;
//import java.util.Iterator;
//import java.util.Properties;
//import java.util.concurrent.TimeUnit;
//
//public class Flink12_IdlenessJob {
//    public static void main(String[] args) throws Exception {
//        //自己封装的带环境区分的参数工具类
//        ParameterTool parameterTool = ParameterToolEnvironmentUtils.createParameterTool(args);
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //设置为业务时间
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//        //设置kafka消费基本信息
//        String bootstrapServers = parameterTool.get(KafkaSinkUtil.KAFKA_BOOTSTRAP_SERVERS);
//        String groupId = "idleness-group";
//        Properties kafkaProp = new Properties();
//        kafkaProp.setProperty(KafkaConstant.BOOTSTRAP_SERVERS_KEY, bootstrapServers);
//        kafkaProp.setProperty(KafkaConstant.GROUP_ID_KEY, groupId);
//
//        FlinkKafkaConsumer<Buy> kafkaConsumer = new FlinkKafkaConsumer("idleness_topic", new KafkaDeserializationSchema<Buy>() {
//            @Override
//            public boolean isEndOfStream(Buy nextElement) {
//                return false;
//            }
//
//            @Override
//            public Buy deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
//                //将接收到的数据对象 解析到 Buy 封装的对象中
//                String json = new String(record.value(), "UTF-8");
//                return JSONObject.parseObject(json, Buy.class);
//            }
//
//            @Override
//            public TypeInformation getProducedType() {
//                return TypeInformation.of(Buy.class);
//            }
//        }, kafkaProp);
//
//        kafkaConsumer.setStartFromLatest(); //每次启动从卡夫卡最新数据拉取
//
//        //抽取EventTime生成Watermark  可以处理迟到2秒数据
//        FlinkKafkaConsumerBase<Buy> source = kafkaConsumer.assignTimestampsAndWatermarks(WatermarkStrategy.<Buy>forBoundedOutOfOrderness(Duration.ofSeconds(2))
//                .withTimestampAssigner((e, t) -> e.ts));
//
//        DataStreamSource<Buy> dataStreamSource = env.addSource(source);
//
//        //通过name分组后 5秒滚动一个窗口
//        SingleOutputStreamOperator<Tuple4<String, Long, Long, Long>> process = dataStreamSource.keyBy(e -> e.name).window(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
//                .process(new ProcessWindowFunction<Buy, Tuple4<String, Long, Long, Long>, String, TimeWindow>() {
//                    @Override
//                    public void process(String key, Context context, Iterable<Buy> elements, Collector<Tuple4<String, Long, Long, Long>> out) throws Exception {
//                        long start = context.window().getStart();  //窗口开始时间
//                        long end = context.window().getEnd();      //窗口结束时间
//                        Long total = 0L;                           //总条数
//                        String name = "";                          //用户名
//                        for (Iterator<Buy> iterator = elements.iterator(); iterator.hasNext(); ) {
//                            Buy next = iterator.next();
//                            name = next.name;
//                            total += 1;
//                        }
//                        out.collect(Tuple4.of(name, total, start, end));
//                    }
//                });
//
//        //控制台输出
//        process.print();
//
//        env.execute("Test IdlenessJob");
//    }
//
//    static class Buy implements Serializable {
//        private String name;  //用户名
//        private Long ts;      //业务时间
//
//        //get set 省略
//    }
//}
