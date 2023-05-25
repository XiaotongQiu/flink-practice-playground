package window.function;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class CombinedWindowFunction {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        List<Tuple2<Long, Long>> data = new ArrayList<>();
        Tuple2<Long, Long> a = new Tuple2<>(1L, 1L);
        Tuple2<Long, Long> b = new Tuple2<>(3L, 1L);
        data.add(a);
        data.add(b);
        DataStream<Tuple2<Long, Long>> input = env.fromCollection(data);

        input.keyBy(1)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(1)))
//                .timeWindow(Time.seconds(10), Time.seconds(1))
                .reduce((r1, r2) -> {
                    return r1.f0 < r2.f0 ? r1 : r2;
                }, new ProcessWindowFunction<Tuple2<Long, Long>, Object, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, ProcessWindowFunction<Tuple2<Long, Long>, Object, Tuple, TimeWindow>.Context context, Iterable<Tuple2<Long, Long>> iterable, Collector<Object> collector) throws Exception {
                        collector.collect("window: " + context.window());
                    }
                })
                .print();

        env.execute();
    }
}
