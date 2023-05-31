package ecommerce;

import ecommerce.model.AdClickLog;
import ecommerce.model.BlacklistWarn;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.xml.crypto.Data;

public class AdClickBlackList {

    private static final String path = "/Users/qcat/xiaotong_dev/java/flink-practice/src/resources/AdClickLog.csv";
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
                                Long.parseLong(data[4]) * 1000);
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<AdClickLog>() {
                    @Override
                    public long extractAscendingTimestamp(AdClickLog adClickLog) {
                        return adClickLog.getTimestamp();
                    }
                });

        DataStream<BlacklistWarn> filteredStream = input
            .keyBy(new KeySelector<AdClickLog, Tuple2<Long, Long>>() {
                @Override
                public Tuple2<Long, Long> getKey(AdClickLog r) throws Exception {
                    return new Tuple2<Long, Long>(r.getUserId(), r.getAdId());
                }
            })
            .process(new FilterBlackLists(100L))
            .getSideOutput(new OutputTag<BlacklistWarn>("blackList"){});

        filteredStream.print();

        env.execute();
    }

    public static class FilterBlackLists extends KeyedProcessFunction<Tuple2<Long, Long>, AdClickLog, AdClickLog> {
        private long maxClickCount;
        private ValueState<Long> countState;
        private ValueState<Boolean> blackedState;

        public FilterBlackLists(long maxClickCount) {
            this.maxClickCount = maxClickCount;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            ValueStateDescriptor<Long> counDesc = new ValueStateDescriptor<>("count-state", Long.class);
            ValueStateDescriptor<Boolean> blackDesc = new ValueStateDescriptor<Boolean>("blacked-state", Boolean.class);

            countState = getRuntimeContext().getState(counDesc);
            blackedState = getRuntimeContext().getState(blackDesc);
        }

        @Override
        public void processElement(AdClickLog adClickLog, KeyedProcessFunction<Tuple2<Long, Long>, AdClickLog, AdClickLog>.Context context, Collector<AdClickLog> collector) throws Exception {
            long cnt = 0;

            if (countState.value() == null) {
                context.timerService().registerEventTimeTimer(adClickLog.getTimestamp() + Time.days(1).toMilliseconds());
            } else {
                cnt = countState.value();
                if (cnt > maxClickCount) {
                    if (blackedState.value() == null) {
                        // tag abnormal data, send them to blacklist stream
                        context.output(new OutputTag<BlacklistWarn>("blackList"){}, new BlacklistWarn(adClickLog.getUserId(), adClickLog.getAdId(), "too mang clicks: " + cnt + " exceeding limit " + maxClickCount));
                        blackedState.update(true);
                    }

                    return;
                }
            }

            countState.update(cnt+1);
            collector.collect(adClickLog);
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Tuple2<Long, Long>, AdClickLog, AdClickLog>.OnTimerContext ctx, Collector<AdClickLog> out) throws Exception {
            countState.clear();
            blackedState.clear();
        }
    }
}
