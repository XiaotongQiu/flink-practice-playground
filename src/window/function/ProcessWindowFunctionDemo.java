package window.function;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class ProcessWindowFunctionDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<Tuple3<String, Long, Long>> data = new ArrayList<>();
        Tuple3<String, Long, Long> a1 = new Tuple3<>("first event", 1L, 1111L);
        Tuple3<String, Long, Long> a2 = new Tuple3<>("second event", 1L, 1112L);
        Tuple3<String, Long, Long> a3 = new Tuple3<>("third event", 1L, 20121L);
        Tuple3<String, Long, Long> b1 = new Tuple3<>("first event", 2L, 1111L);
        Tuple3<String, Long, Long> b2 = new Tuple3<>("second event", 2L, 1112L);
        Tuple3<String, Long, Long> b3 = new Tuple3<>("third event", 2L, 30111L);
        data.add(a1);
        data.add(a2);
        data.add(a3);
        data.add(b1);
        data.add(b2);
        data.add(b3);
        DataStreamSource<Tuple3<String, Long, Long>> input = env.fromCollection(data);

        input.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Long, Long>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<String, Long, Long> stringLongLongTuple3) {
                return stringLongLongTuple3.f2;
            }
        })
                .keyBy(x -> x.f1)
//                .timeWindow(Time.seconds(1), Time.seconds(1))
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(1)))
                .process(new ProcessWindowFunction<Tuple3<String, Long, Long>, String, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, ProcessWindowFunction<Tuple3<String, Long, Long>, String, Long, TimeWindow>.Context context, Iterable<Tuple3<String, Long, Long>> iterable, Collector<String> collector) throws Exception {
                        long count = 0L;
                        for (Tuple3<String, Long, Long> element : iterable) {
                            count += element.f0.split(" ").length;
                        }
                        collector.collect("window: " + context.window() + " word count " + count);
                    }
                }).print();
        env.execute();
    }

}
