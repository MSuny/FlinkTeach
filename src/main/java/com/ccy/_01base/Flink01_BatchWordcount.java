package com.ccy._01base;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Flink01_BatchWordcount {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2. 从文件读取数据  按行读取(存储的元素就是每行的文本)
        DataSource<String> wordDS = env.readTextFile("/Users/changchaoyi/cc/aa.txt");
        FlatMapOperator<String, String> wordFMO = wordDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                for (String word : line.split(",")) {
                    out.collect(word);
                }
            }
        });
        // 3. 使用map将数据转换格式  word -> (word,1L)
        MapOperator<String, Tuple2<String, Long>> wordTuple = wordFMO.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String word) throws Exception {
                return Tuple2.of(word, 1L);
            }
        });
        // 4. 对相同key 的数据进行聚合操作
        UnsortedGrouping<Tuple2<String, Long>> wordUG = wordTuple.groupBy(0);
        // 5. 对分组内的数据进行求和
        AggregateOperator<Tuple2<String, Long>> wordAgg = wordUG.sum(1);
        // 6. 打印
        wordAgg.print();
    }
}
