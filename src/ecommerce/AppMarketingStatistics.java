package ecommerce;

import ecommerce.model.MarketingUserBehavior;
import ecommerce.model.UserBahavior;
import ecommerce.source.SimulatedEventSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.text.SimpleDateFormat;

public class AppMarketingStatistics {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<MarketingUserBehavior> input = env.addSource(new SimulatedEventSource())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MarketingUserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(MarketingUserBehavior marketingUserBehavior) {
                        return marketingUserBehavior.getTs();
                    }
                });

//        input.print();
        DataStream<MarketingUserBehavior> actions = input.filter(r -> !r.getBehavior().equals("UNINSTALL"));
        actions
                        .map(new MapFunction<MarketingUserBehavior, Tuple2<String, Long>>() {
                            @Override
                            public Tuple2<String, Long> map(MarketingUserBehavior marketingUserBehavior) throws Exception {
                                return new Tuple2<>("key", 1L);
                            }
                        })
                                .keyBy(r -> r._1)
                                        .timeWindow(Time.seconds(5), Time.seconds(1))
                                                .process(new CountTotal()).print();

        actions
//                .map(new MapFunction<MarketingUserBehavior, Tuple2<String, String>>() {
//            @Override
//            public Tuple2<String, String> map(MarketingUserBehavior marketingUserBehavior) throws Exception {
//                return new Tuple2<>(marketingUserBehavior.getChannel(), marketingUserBehavior.getBehavior());
////                return new Tuple2<Tuple2<String, String>, Long>(new Tuple2<>(marketingUserBehavior.getChannel(), marketingUserBehavior.getBehavior()), 1L);
//            }
//        })
            .keyBy(new KeySelector<MarketingUserBehavior, Tuple2<String, String>>() {
                @Override
                public Tuple2<String, String> getKey(MarketingUserBehavior r) throws Exception {
                    return new Tuple2<String, String>(r.getChannel(), r.getBehavior());
                }
            })
            .timeWindow(Time.seconds(5), Time.seconds(1))
            .process(new CountTotalByChannelAndBehavior())
            .print();

        env.execute();
    }

    public static class CountTotalByChannelAndBehavior extends ProcessWindowFunction<MarketingUserBehavior, String, Tuple2<String, String>, TimeWindow> {

        @Override
        public void process(Tuple2<String, String> stringStringTuple2, ProcessWindowFunction<MarketingUserBehavior, String, Tuple2<String, String>, TimeWindow>.Context context, Iterable<MarketingUserBehavior> iterable, Collector<String> collector) throws Exception {
            StringBuilder res = new StringBuilder();
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

            res.append("window end at ").append(simpleDateFormat.format(context.window().getEnd()))
                    .append(" channel: ").append(stringStringTuple2._1)
                    .append(" action: ").append(stringStringTuple2._2)
                    .append(" n count is: ").append(Iterables.size(iterable));

            collector.collect(res.toString());
        }
    }

    public static class CountTotal extends ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {
            StringBuilder res = new StringBuilder();
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");

            res.append("window end at ").append(simpleDateFormat.format(context.window().getEnd()))
                    .append(" non-uninstall action count is: ").append(Iterables.size(iterable));

            collector.collect(res.toString());
        }
    }
}
