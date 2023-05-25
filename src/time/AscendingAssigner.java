package time;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.util.ArrayList;
import java.util.List;

public class AscendingAssigner {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<Tuple2<String, Long>> collectionInput = new ArrayList<>();
        Tuple2<String, Long> a = new Tuple2<>("first event", 1L);
        Tuple2<String, Long> b = new Tuple2<>("second event", 2L);
        collectionInput.add(a);
        collectionInput.add(b);

        DataStream<Tuple2<String, Long>> input = env.fromCollection(collectionInput);

        // just use current max timestamp in data as watermark
        // assume it's ascending order

        input.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<String, Long>>() {
            @Override
            public long extractAscendingTimestamp(Tuple2<String, Long> stringLongTuple2) {
                return stringLongTuple2.f1;
            }
        });

        env.execute();


    }
}
