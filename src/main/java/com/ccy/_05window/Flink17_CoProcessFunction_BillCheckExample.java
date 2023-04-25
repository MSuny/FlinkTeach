package com.ccy._05window;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class Flink17_CoProcessFunction_BillCheckExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env
                .fromElements(
                        Tuple3.of("order-1", "app", 1000L),
                        Tuple3.of("order-2", "app", 2000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, String, Long> in, long l) {
                                        return in.f2;
                                    }
                                })
                );

        SingleOutputStreamOperator<Tuple3<String, String, Long>> thirdPartyStream = env
                .fromElements(
                        Tuple3.of("order-1", "third-party", 3000L),
                        Tuple3.of("order-3", "third-party", 4000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                                    @Override
                                    public long extractTimestamp(Tuple3<String, String, Long> in, long l) {
                                        return in.f2;
                                    }
                                })
                );

        appStream.keyBy(r -> r.f0).connect(thirdPartyStream.keyBy(r -> r.f0)).process(new Match()).print();

        env.execute();
    }

    public static class Match extends CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String> {
        // 声明状态变量用来保存app的支付事件
        private ValueState<Tuple3<String, String, Long>> appEvent;
        // 声明状态变量用来保存第三方平台的到账事件
        private ValueState<Tuple3<String, String, Long>> thirdPartyEvent;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            appEvent = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>("app", Types.TUPLE(
                            Types.STRING, Types.STRING, Types.LONG
                    ))
            );
            thirdPartyEvent = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>("third-party", Types.TUPLE(
                            Types.STRING, Types.STRING, Types.LONG
                    ))
            );
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> app, Context context, Collector<String> collector) throws Exception {
            if (thirdPartyEvent.value() != null) {
                collector.collect(app.f0 + " 对账成功");
                thirdPartyEvent.clear();
            } else {
                appEvent.update(app);
                context.timerService().registerEventTimeTimer(app.f2 + 5000L);
            }
        }

        @Override
        public void processElement2(Tuple3<String, String, Long> thirdParty, Context context, Collector<String> collector) throws Exception {
            if (appEvent.value() != null) {
                collector.collect(thirdParty.f0 + " 对账成功");
                appEvent.clear();
            } else {
                thirdPartyEvent.update(thirdParty);
                context.timerService().registerEventTimeTimer(thirdParty.f2 + 5000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            if (appEvent.value() != null) {
                out.collect(appEvent.value().f0 + "对账失败，订单的第三方支付信息未到");
                appEvent.clear();
            }
            if (thirdPartyEvent.value() != null) {
                out.collect(thirdPartyEvent.value().f0 + "对账失败，订单的app支付信息未到");
                thirdPartyEvent.clear();
            }
        }
    }
}