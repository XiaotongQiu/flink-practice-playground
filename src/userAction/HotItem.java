package userAction;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import userAction.model.ItemViewCount;
import userAction.model.UserBahavior;
import window.function.AggregationFunction;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class HotItem {
    private static final String path="/resources/UserBehavior.csv";

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> input = env.readTextFile(path);

        DataStream<ItemViewCount> output = input.map(new MapFunction<String, UserBahavior>() {
            @Override
            public UserBahavior map(String s) throws Exception {
                String[] data = s.split(",");

                return new UserBahavior(data[0], data[1], data[2], data[3], Long.parseLong(data[4])*1000);
            }
        })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBahavior>() { // use ascending ts for example only
            @Override
            public long extractAscendingTimestamp(UserBahavior userBahavior) {
                return userBahavior.getTimestamp();
            }
        })
                .filter(u -> u.getBehaviour().equals("pv"))
                .keyBy((KeySelector<UserBahavior, String>) userBahavior -> userBahavior.getItemId())
                .timeWindow(Time.hours(1), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResult());

        DataStream<String> topItems = output.keyBy(i -> i.getWindowEnd())
                        .process(new TopHotItems(3));

//        output.print()

        topItems.print();


        env.execute();
    }

    public static class CountAgg implements AggregateFunction<UserBahavior, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBahavior userBahavior, Long aLong) {
            return aLong+1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return aLong+acc1;
        }
    }

    public static class WindowResult extends ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<Long, ItemViewCount, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<ItemViewCount> collector) throws Exception {
            collector.collect(new ItemViewCount(s, context.window().getEnd(), iterable.iterator().next()));
        }
    }

    public static class TopHotItems extends ProcessFunction<ItemViewCount, String> {
        private final int topSize;

        public TopHotItems(int sz) {
            this.topSize = sz;
        }

        private ListState<ItemViewCount> itemState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            ListStateDescriptor<ItemViewCount> itemViewCountListStateDescriptor = new ListStateDescriptor<ItemViewCount>("itemState-state", ItemViewCount.class);
            itemState = getRuntimeContext().getListState(itemViewCountListStateDescriptor);
        }

        @Override
        public void processElement(ItemViewCount itemViewCount, ProcessFunction<ItemViewCount, String>.Context context, Collector<String> collector) throws Exception {
            itemState.add(itemViewCount);

            context.timerService().registerEventTimeTimer(itemViewCount.getWindowEnd()+1);
        }

        @Override
        public void onTimer(long timestamp, ProcessFunction<ItemViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            List<ItemViewCount> allItems = new ArrayList<>();
            for (ItemViewCount itemViewCount: itemState.get()) {
                allItems.add(itemViewCount);
            }

            itemState.clear();

            allItems.sort(new Comparator<ItemViewCount>() {
                @Override
                public int compare(ItemViewCount o1, ItemViewCount o2) {
                    return o2.getCount().compareTo(o1.getCount());
                }
            });

            // 将排名信息格式化成 String, 便于打印
            StringBuilder result = new StringBuilder();
            result.append("====================================\n");
            result.append("时间: ").append(new Timestamp(timestamp-1)).append("\n");
            for (int i=0;i<topSize;i++) {
                ItemViewCount currentItem = allItems.get(i);
                // No1:  商品ID=12224  浏览量=2413
                result.append("No").append(i).append(":")
                        .append("  商品ID=").append(currentItem.getItemId())
                        .append("  浏览量=").append(currentItem.getCount())
                        .append("\n");
            }
            result.append("====================================\n\n");

            out.collect(result.toString());
        }
    }
}
