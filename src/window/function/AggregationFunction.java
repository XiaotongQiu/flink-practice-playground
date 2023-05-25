package window.function;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class AggregationFunction {

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<String, Long>> data = new ArrayList<>();
        Tuple2<String, Long> a = new Tuple2<>("first event", 1L);
        Tuple2<String, Long> b = new Tuple2<>("second event", 2L);
        data.add(a);
        data.add(b);
        DataStreamSource<Tuple2<String, Long>> input = env.fromCollection(data);

        input.keyBy(x -> x.f1)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(1)))
                .aggregate(new MyAggFunc());

        env.execute();
    }

    public static class MyAggFunc implements AggregateFunction<Tuple2<String, Long>, String, String> {
        @Override
        public String createAccumulator() {
            return "";
        }

        @Override
        public String add(Tuple2<String, Long> stringLongTuple2, String o) {
            return stringLongTuple2.f0 + o;
        }

        @Override
        public String getResult(String o) {
            return o.trim();
        }

        @Override
        public String merge(String o, String acc1) {
            return o + " " + acc1;
        }
    }
}
