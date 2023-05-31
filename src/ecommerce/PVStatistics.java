package ecommerce;

import ecommerce.model.UserBahavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class PVStatistics {
    private static final String path = "/Users/qcat/xiaotong_dev/java/flink-practice/src/resources/UserBehavior.csv";
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
                }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBahavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBahavior userBahavior) {
                        return userBahavior.getTimestamp();
                    }
                });

        input.filter(r -> r.getBehaviour().equals("pv"))
                .map(new MapFunction<UserBahavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBahavior userBahavior) throws Exception {
                        return new Tuple2<>("pv", 1L);
                    }
                })
                .keyBy(r -> r.f0)
                .timeWindow(Time.hours(1))
                .sum(1)
                .print();

        env.execute();
    }
}
