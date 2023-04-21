package com.ccy._03transformation;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 需求：使用keyBy算子按照数据的奇偶来标记不同的key
 */
public class Flink05_KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        env.setParallelism(2);

        DataStreamSource<Integer> DS = env.fromElements(1, 2, 3, 4, 66, 77, 88, 99,100,200);
//         DS.keyBy(new KeySelector<Integer, Integer>() {
//             @Override
//             public Integer getKey(Integer num) throws Exception {
//                 if (num % 2 == 0) {
//                     return 0;
//                 } else {
//                     return 1;
//                 }
//             }
//         }).print();

        // TODO KeySelector<分区号类型，键值类型>
        DS.keyBy(new KeySelector<Integer, String>() {
            @Override
            public String getKey(Integer value) throws Exception {
                if (value % 2 ==0) {
                    return "偶数";
                } else {
                    return "奇数";
                }
            }
        }).print();
        env.execute("Flink05_KeyBy");
    }
}
