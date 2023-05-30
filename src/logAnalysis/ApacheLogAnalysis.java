package logAnalysis;

import logAnalysis.model.ApacheLogEvent;
import logAnalysis.model.UrlViewCount;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
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

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 1. calculate pv of each url every minute
 * 2. find the top 5 visited url
 * 3. updated every 5 sec
 * */
public class ApacheLogAnalysis {
    private static final String path = "resources/apachelog.txt";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<ApacheLogEvent> input = env.readTextFile(path)
                        .map(new MapFunction<String, ApacheLogEvent>() {
                            @Override
                            public ApacheLogEvent map(String s) throws Exception {
                                String[] data = s.split(" ");

                                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                                Long ts = simpleDateFormat.parse(data[3]).getTime();

                                return new ApacheLogEvent(data[0], data[2], ts, data[5], data[6]);
                            }
                        })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ApacheLogEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ApacheLogEvent apacheLogEvent) {
                        return apacheLogEvent.getEventTime();
                    }
                });

        DataStream<UrlViewCount> windowedData = input.keyBy(e -> e.getUrl())
                        .timeWindow(Time.minutes(1), Time.seconds(5))
                        .aggregate(new CountAgg(), new WindowProcessFunc());

//        windowedData.print();

        windowedData
                .keyBy(UrlViewCount::getWindowEnd)
                .process(new TopVisitedUrl(5))
                        .print();

        env.execute();
    }

    public static class CountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ApacheLogEvent apacheLogEvent, Long aLong) {
            return aLong+1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }

    public static class WindowProcessFunc extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<UrlViewCount> collector) throws Exception {
            collector.collect(new UrlViewCount(s, context.window().getEnd(), iterable.iterator().next()));
        }
    }

    public static class TopVisitedUrl extends ProcessFunction<UrlViewCount, String> {
        private final int TOP_SIZE;
        private ListState<UrlViewCount> listState;

        public TopVisitedUrl(int size) {
            this.TOP_SIZE = size;
        }


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            ListStateDescriptor<UrlViewCount> listStateDescriptor = new ListStateDescriptor<>("url_click_state", UrlViewCount.class);
            listState = getRuntimeContext().getListState(listStateDescriptor);
        }

        @Override
        public void processElement(UrlViewCount urlViewCount, ProcessFunction<UrlViewCount, String>.Context context, Collector<String> collector) throws Exception {
            listState.add(urlViewCount);

            context.timerService().registerEventTimeTimer(urlViewCount.getWindowEnd()+1);
        }

        @Override
        public void onTimer(long timestamp, ProcessFunction<UrlViewCount, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            List<UrlViewCount> allUrls = new ArrayList<>();
            for (UrlViewCount urlViewCount : listState.get()) {
                allUrls.add(urlViewCount);
            }

            listState.clear();

            allUrls.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.getCount().compareTo(o1.getCount());
                }
            });

            StringBuilder result = new StringBuilder();
            result.append("====================================\n");
            result.append("时间: ").append(new Timestamp(timestamp-1)).append("\n");
            for (int i=0; i < Math.min(TOP_SIZE, allUrls.size()); i++) {
                UrlViewCount currentItem = allUrls.get(i);
                // No1:  商品ID=12224  浏览量=2413
                result.append("No").append(i).append(":")
                        .append("  url=").append(currentItem.getUrl())
                        .append("  pv=").append(currentItem.getCount())
                        .append("\n");
            }
            result.append("====================================\n\n");

            out.collect(result.toString());
        }
    }
}
