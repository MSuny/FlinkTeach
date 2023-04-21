package com.ccy._01base;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 无界流处理的Lambda表达式写法
 */
public class Flink03_UnBoundedStreamingWordcount_Lambda {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 读取socket流数据  与有界流数据的区别就在这里
        DataStreamSource<String> lineDSS = env.readTextFile("/Users/changchaoyi/cc/aa.txt");
        env.setParallelism(8);
        env.disableOperatorChaining();
        // 3. 转换数据格式
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS
                .flatMap((String line, Collector<String> words) -> {
                    Arrays.stream(line.split(",")).forEach(words::collect);
                })
                // 泛型之中又有泛型，涉及到泛型擦除，需要用returns方法显式指定返回的类型
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1L))
                // 泛型之中又有泛型，涉及到泛型擦除，需要用returns方法显式指定返回的类型
                .returns(Types.TUPLE(Types.STRING, Types.LONG));
        // 4. 分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKS = wordAndOne
                .keyBy(t -> t.f0);
        // 5. 求和
        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordAndOneKS
                .sum(1);
        // 6. 打印
        // TODO 1、由于并行度不为1且有keyed算子，每行数据切分后写入了不同的分区，导致数据展示的顺序和文件中的顺序不同。
        // TODO 2、如果去掉keyBy算子，不管并行度为何值，打印结果都是文件中字符的顺序
        wordAndOne.print();
        System.out.println(env.getParallelism());
        // 7. 执行
        env.execute();
    }
}