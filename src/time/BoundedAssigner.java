package time;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class BoundedAssigner {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<Tuple2<String, Long>> collectionInput = new ArrayList<>();
        Tuple2<String, Long> a = new Tuple2<>("first event", 1L);
        Tuple2<String, Long> b = new Tuple2<>("second event", 2L);
        collectionInput.add(a);
        collectionInput.add(b);

        DataStream<Tuple2<String, Long>> input = env.fromCollection(collectionInput);

        // use assignTimestampsAndWatermarks with watermark strategy
        // set max out of orderness to 10
        // generate watermark periodically
        input.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Long>>(Time.seconds(10)) {
            @Override
            public long extractTimestamp(Tuple2<String, Long> stringLongTuple2) {
                return stringLongTuple2.f1;
            }
        });

        env.execute();

    }
}
