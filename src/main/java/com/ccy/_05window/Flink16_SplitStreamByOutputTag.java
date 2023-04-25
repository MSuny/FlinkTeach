package com.ccy._05window;


import com.ccy._00bean.Event;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Calendar;
import java.util.Random;

public class Flink16_SplitStreamByOutputTag {
    // 定义输出标签，侧输出流的数据类型为三元组(user, url, timestamp)
    private static OutputTag<Tuple3<String, String, Long>> MaryTag = new OutputTag<Tuple3<String, String, Long>>("Mary-pv"){};
    private static OutputTag<Tuple3<String, String, Long>> BobTag = new OutputTag<Tuple3<String, String, Long>>("Bob-pv"){};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Event> stream = env
                .addSource(new ClickSource());

        SingleOutputStreamOperator<Event> processedStream = stream.process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<Event> out) throws Exception {
                if (value.getUsername().equals("Mary")){
                    ctx.output(MaryTag, new Tuple3<>(value.getUsername(), value.getUrl(), value.getTimestamp()));
                } else if (value.getUsername().equals("Bob")){
                    ctx.output(BobTag, new Tuple3<>(value.getUsername(), value.getUrl(), value.getTimestamp()));
                } else {
                    out.collect(value);
                }
            }
        });

        processedStream.getSideOutput(MaryTag).print("Mary pv");
        processedStream.getSideOutput(BobTag).print("Bob pv");
        processedStream.print("else");

        env.execute();
    }

    // 这里重新实现了ClickSource()，因为我们在自定义数据源中发送了水位线
    public static class ClickSource implements SourceFunction<Event> {
        private boolean running = true;
        @Override
        public void run(SourceContext<Event> sourceContext) throws Exception {
            Random random = new Random();
            String[] userArr = {"Mary", "Bob", "Alice"};
            String[] urlArr  = {"./home", "./cart", "./prod?id=1"};
            while (running) {
                long currTs = Calendar.getInstance().getTimeInMillis(); // 毫秒时间戳
                String username = userArr[random.nextInt(userArr.length)];
                String url      = urlArr[random.nextInt(urlArr.length)];
                Event event = new Event(username, url, currTs);
                // 使用collectWithTimestamp方法将数据发送出去，并指明数据中的时间戳的字段
                sourceContext.collectWithTimestamp(event, event.getTimestamp());
                // 发送水位线
                sourceContext.emitWatermark(new Watermark(event.getTimestamp()));
                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }
}

