package ecommerce;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import ecommerce.model.UserBahavior;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

public class UvStatistics {
    private static final String path="resources/UserBehavior.csv";
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<UserBahavior> input = env.readTextFile(path)
                        .map(new MapFunction<String, UserBahavior>() {
                            @Override
                            public UserBahavior map(String s) throws Exception {
                                String[] data = s.split(",");

                                return new UserBahavior(data[0], data[1], data[2], data[3], Long.parseLong(data[4])*1000);
                            }
                        }).filter(r -> r.getBehaviour().equals("pv"))
                        .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBahavior>() {
                            @Override
                            public long extractAscendingTimestamp(UserBahavior userBahavior) {
                                return userBahavior.getTimestamp();
                            }
                        });

        input.map(new MapFunction<UserBahavior, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(UserBahavior userBahavior) throws Exception {
                        return new Tuple2<>("key", userBahavior.getUserId());
                    }
                })
                .keyBy(r -> r._1) // to group all events in the window
                        .timeWindow(Time.hours(1))
                                .aggregate(new DistinctCountAgg(), new ProcessWindowFunction<Long, String, String, TimeWindow>() {
                                    @Override
                                    public void process(String s, ProcessWindowFunction<Long, String, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<String> collector) throws Exception {
                                        collector.collect("window end at " + new Timestamp(context.window().getEnd()) + " uv is " + iterable.iterator().next());
                                    }


                                })
                .print();


        env.execute();
    }


    public static class DistinctCountAgg implements AggregateFunction<Tuple2<String, String>, Tuple2<Set<String>, Long>, Long> {


        @Override
        public Tuple2<Set<String>, Long> createAccumulator() {
            return new Tuple2<>(new HashSet<>(), 0L);
        }

        @Override
        public Tuple2<Set<String>, Long> add(Tuple2<String, String> userBahavior, Tuple2<Set<String>, Long> setLongTuple2) {
            if (!setLongTuple2._1.contains(userBahavior._2)) {
                setLongTuple2._1.add(userBahavior._2);
                return new Tuple2<>(setLongTuple2._1, setLongTuple2._2+1);
            }
            return setLongTuple2;
        }

        @Override
        public Long getResult(Tuple2<Set<String>, Long> setLongTuple2) {
            return setLongTuple2._2;
        }

        @Override
        public Tuple2<Set<String>, Long> merge(Tuple2<Set<String>, Long> setLongTuple2, Tuple2<Set<String>, Long> acc1) {
            return null;
        }
    }
}
