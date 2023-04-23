package com.ccy._04sink;

import com.ccy._00bean.WaterSensor;
import com.ccy._02source.udsource.RandomWatersensor;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @author Hefei
 * @description
 * @project_name flink-0521
 * @package_name com.atguigu.chapter05.sink
 * @since 2021/11/7 21:08
 */
public class Flink01_Sink_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<WaterSensor> input = env.addSource(new RandomWatersensor());
        SingleOutputStreamOperator<String> map = input.map(WaterSensor::toString);

        final StreamingFileSink<String> sink = StreamingFileSink
                .forRowFormat(
                        new Path("./output"),
                        new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .build())
                .build();

        map.addSink(sink);

        env.execute();
    }
}