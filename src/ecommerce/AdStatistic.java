package ecommerce;

import ecommerce.model.AdClickLog;
import ecommerce.model.CountByProvince;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AdStatistic {
    private static final String path = "resources/AdClickLog.csv";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<AdClickLog> input = env.readTextFile(path)
                        .map(new MapFunction<String, AdClickLog>() {
                            @Override
                            public AdClickLog map(String s) throws Exception {
                                String[] data = s.split(",");

                                return new AdClickLog(
                                        Long.parseLong(data[0]),
                                        Long.parseLong(data[1]),
                                        data[2],
                                        data[3],
                                        Long.parseLong(data[4])*1000);
                            }
                        })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickLog>() {
                    @Override
                    public long extractAscendingTimestamp(AdClickLog adClickLog) {
                        return adClickLog.getTimestamp();
                    }
                });

//        input.print();

        input.keyBy(r -> r.getProvince())
            .timeWindow(Time.hours(1), Time.seconds(5))
            .process(new CountAgg())
            .print();


        env.execute();
    }

    public static class CountAgg extends ProcessWindowFunction<AdClickLog, CountByProvince, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<AdClickLog, CountByProvince, String, TimeWindow>.Context context, Iterable<AdClickLog> iterable, Collector<CountByProvince> collector) throws Exception {
            collector.collect(new CountByProvince(s, context.window().getEnd(), (long) Iterables.size(iterable)));
        }
    }
}
