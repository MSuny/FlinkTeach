package com.ccy._06state;

import com.ccy._00bean.Event;
import com.ccy._02source.udsource.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class Flink02_MapState_FakeWindowExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.getTimestamp();
                            }
                        }))
                .keyBy(r -> r.getUrl())
                .process(new FakeWindow(5000L))
                .print();

        env.execute();
    }

    public static class FakeWindow extends KeyedProcessFunction<String, Event, String> {
        // 窗口的开始时间 -> 窗口中的pv
        private MapState<Long, Long> mapState;
        // 滚动窗口的长度
        private Long windowSize;

        public FakeWindow(Long windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<Long, Long>("window-pv", Types.LONG, Types.LONG)
            );
        }

        @Override
        public void processElement(Event event, Context context, Collector<String> collector) throws Exception {
            long windowStart = event.getTimestamp() - event.getTimestamp() % windowSize;
            long windowEnd = windowStart + windowSize - 1L;
            context.timerService().registerEventTimeTimer(windowEnd);
            if (mapState.contains(windowStart)) {
                long pv = mapState.get(windowStart);
                mapState.put(windowStart, pv + 1L);
            } else {
                mapState.put(windowStart, 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);


            long start = timestamp + 1L - windowSize;
            long end = timestamp + 1L;
            out.collect(ctx.getCurrentKey() + ":" + new Timestamp(start) + "~" + new Timestamp(end) + ":" + mapState.get(start));
// 删除窗口，因为窗口的默认操作是计算完成以后销毁窗口
            mapState.remove(start);
        }
    }
}
