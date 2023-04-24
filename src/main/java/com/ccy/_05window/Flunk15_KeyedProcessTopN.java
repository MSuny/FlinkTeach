package com.ccy._05window;

import com.ccy._00bean.Event;
import com.ccy._02source.udsource.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

public class Flunk15_KeyedProcessTopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> pvStream = env
                .addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.getTimestamp();
                                    }
                                })
                );

        // 先求出每个url在每个窗口的浏览量
        SingleOutputStreamOperator<UrlViewCount> uvcStream = pvStream
                .keyBy(r -> r.getUrl())
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new CountAgg(), new WindowResult());    // 增量聚合，并结合全窗口函数包装UrlViewCount

        // 针对同一个窗口中的不同url的UrlViewCount次数，进行排序输出
        KeyedStream<UrlViewCount, Long> uvcKeyedStream = uvcStream
                .keyBy(r -> r.windowEnd);

        uvcKeyedStream
                .process(new TopN(2))
                .print();

        env.execute();
    }

    public static class TopN extends KeyedProcessFunction<Long, UrlViewCount, String> {
        // 列表状态变量
        private ListState<UrlViewCount> UrlViewCountListState;
        private Integer threshold;

        public TopN(Integer threshold) {
            this.threshold = threshold;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 声明一个列表状态，保存已经到达的统计结果
            UrlViewCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlViewCount>("list-state", Types.POJO(UrlViewCount.class))
            );
        }

        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
            // 添加到列表状态变量中
            UrlViewCountListState.add(value);
            // 水位线达到 窗口结束时间 + 1毫秒 时触发定时器来进行排序
            ctx.timerService().registerEventTimeTimer(value.windowEnd + 1);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            // 将数据从列表状态变量中取出，放入ArrayList
            ArrayList<UrlViewCount> UrlViewCountArrayList = new ArrayList<>();
            for (UrlViewCount uvc : UrlViewCountListState.get()) {
                UrlViewCountArrayList.add(uvc);
            }
            // 清空状态释放资源
            UrlViewCountListState.clear();

            // 排序
            UrlViewCountArrayList.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.count.intValue() - o1.count.intValue();
                }
            });

            // 取前三名，构建输出结果
            StringBuilder result = new StringBuilder();
            result.append("========================================\n");
            for (int i = 0; i < this.threshold; i++) {
                UrlViewCount UrlViewCount = UrlViewCountArrayList.get(i);
                result
                        .append("浏览量No." + (i + 1) + " ")
                        .append("url：" + UrlViewCount.url + " ")
                        .append("浏览量：" + UrlViewCount.count + " ")
                        .append("窗口结束时间：" + new Timestamp(timestamp - 1) + "\n");
            }
            result
                    .append("========================================\n\n\n");
            out.collect(result.toString());
        }
    }

    public static class CountAgg implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String s, Context context, Iterable<Long> elements, Collector<UrlViewCount> out) throws Exception {
            out.collect(new UrlViewCount(s, elements.iterator().next(), context.window().getStart(), context.window().getEnd()));
        }
    }

    public static class UrlViewCount {
        public String url;
        public Long count;
        public Long windowStart;
        public Long windowEnd;

        public UrlViewCount() {
        }

        public UrlViewCount(String url, Long count, Long windowStart, Long windowEnd) {
            this.url = url;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return "UrlViewCount{" +
                    "url='" + url + '\'' +
                    ", count=" + count +
                    ", windowStart=" + new Timestamp(windowStart) +
                    ", windowEnd=" + new Timestamp(windowEnd) +
                    '}';
        }
    }
}