package com.ccy._06state;

import com.ccy._00bean.Event;
import com.ccy._02source.udsource.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class Flink01_ValueStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.getTimestamp();
                                    }
                                })
                )
                .keyBy(r -> r.getUsername())
                .process(new KeyedProcessFunction<String, Event, String>() {
                    private ValueState<Long> valueState;
                    private ValueState<Long> timerTs;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("pv", Types.LONG));
                        timerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("pv1", Types.LONG));
                    }

                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        if (valueState.value() == null) {
                            valueState.update(1L);
                        } else {
                            valueState.update(valueState.value() + 1L);
                        }
                        if ( timerTs.value() == null) {
                            timerTs.update(value.getTimestamp() + 10 * 100L);
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        super.onTimer(timestamp, ctx, out);
                        out.collect("用户 " + ctx.getCurrentKey() + " 的PV是：" + valueState.value());
                        timerTs.clear();
                    }
                })
                .print();

        env.execute();
    }
}
