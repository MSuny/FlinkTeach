package com.ccy._01base;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理的Lambda表达式写法
 */
public class Flink01_BatchWordcount_Lambda {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2. 从文件读取数据  按行读取(存储的元素就是每行的文本)
        DataSource<String> lineDS = env.readTextFile("/Users/changchaoyi/cc/aa.txt");
        // 3. 转换数据格式
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lineDS
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] split = line.split(" ");
                    for (String word : split) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                })
                // 当Lambda表达式使用 java 泛型的时候, 由于泛型擦除的存在, 需要显示的声明类型信息
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 4. 按照 word 进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG = wordAndOne.groupBy(0);
        // 5. 分组内聚合统计
        AggregateOperator<Tuple2<String, Long>> result = wordAndOneUG.sum(1);
        // 6. 打印结果
        result.print();
    }
}