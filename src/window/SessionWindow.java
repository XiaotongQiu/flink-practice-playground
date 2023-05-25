package window;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class SessionWindow {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // try some input date
        // s.t. we can get session gap of 3s

        final List<Tuple3<String, Long, Integer>> input = new ArrayList<>();

        // here max gap is 2
        // so they will be in the same session window
        input.add(new Tuple3<>("a", 1L, 1));
        input.add(new Tuple3<>("b", 1L, 1));
        input.add(new Tuple3<>("b", 3L, 1));
        input.add(new Tuple3<>("b", 5L, 1));
        input.add(new Tuple3<>("c", 6L, 1));

        // oops. it's been a while since last data
        // there will be another window
        input.add(new Tuple3<>("a", 10L, 1));
        input.add(new Tuple3<>("c", 11L, 1));

        DataStream<Tuple3<String, Long, Integer>> source = env.addSource(new SourceFunction<Tuple3<String, Long, Integer>>() {
            @Override
            public void run(SourceContext<Tuple3<String, Long, Integer>> sourceContext) throws Exception {
                for (Tuple3<String, Long, Integer> data : input) {
                    sourceContext.collectWithTimestamp(data, data.f1);
                    sourceContext.emitWatermark(new Watermark(data.f1-1));
                }
                sourceContext.emitWatermark(new Watermark(Long.MAX_VALUE));
            }

            @Override
            public void cancel() {

            }
        });

        DataStream<Tuple3<String, Long, Integer>> agg = source.keyBy(0)
                .window(EventTimeSessionWindows.withGap(Time.seconds(3)))
                .sum(2);

        agg.print();

        env.execute();
    }
}
