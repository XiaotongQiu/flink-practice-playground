package ecommerce;

import ecommerce.model.LoginEvent;
import ecommerce.model.WarnMsg;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LoginFailDetect {
    private static final int MAX_FAIL_TIMES = 2;
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<LoginEvent> input = env.fromElements(
                new LoginEvent("user_1", "0.0.0.0", "fail", 1000L),
                new LoginEvent("user_1", "0.0.0.0", "fail", 2000L),
                new LoginEvent("user_1", "0.0.0.1", "fail", 3000L),
                new LoginEvent("user_1", "0.0.0.4", "success", 3400L),
                new LoginEvent("user_1", "0.0.0.1", "fail", 3500L),
                new LoginEvent("user_1", "0.0.0.1", "fail", 3600L),
                new LoginEvent("user_1", "0.0.0.2", "fail", 4000L),
                new LoginEvent("user_1", "0.0.0.2", "fail", 7000L),
                new LoginEvent("user_1", "0.0.0.2", "fail", 7001L),
                new LoginEvent("user_1", "0.0.0.2", "fail", 7002L)

        )
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LoginEvent>() {
                    @Override
                    public long extractAscendingTimestamp(LoginEvent loginEvent) {
                        return loginEvent.getEventTime();
                    }
                });

        input.keyBy(r -> r.getUserId())
                .process(new LoginFailedWarning())
                .print();

        env.execute();
    }

    public static class LoginFailedWarning extends KeyedProcessFunction<String, LoginEvent, WarnMsg> {
        private ListState<LoginEvent> failedEvent;
        private ValueState<Long> timerStartTime;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            ListStateDescriptor<LoginEvent> listStateDescriptor = new ListStateDescriptor<>("failed-login-states", LoginEvent.class);
            ValueStateDescriptor<Long> valueStateDescriptor = new ValueStateDescriptor<Long>("timer-start-start", Long.class);

            failedEvent = getRuntimeContext().getListState(listStateDescriptor);
            timerStartTime = getRuntimeContext().getState(valueStateDescriptor);
        }

        @Override
        public void processElement(LoginEvent loginEvent, KeyedProcessFunction<String, LoginEvent, WarnMsg>.Context context, Collector<WarnMsg> collector) throws Exception {
            // already failed for more than threshold
            // return warning in advance
            if (Iterables.size(failedEvent.get()) > MAX_FAIL_TIMES) {
                List<LoginEvent> allFailed = new ArrayList<>();
                for (LoginEvent e : failedEvent.get()) {
                    allFailed.add(e);
                }
                System.out.println("before triggered! " + "ts " + loginEvent.getEventTime() +" user " + context.getCurrentKey() + " failed " + allFailed.size());
                if (allFailed.size() > MAX_FAIL_TIMES) {
                    collector.collect(new WarnMsg(allFailed.get(0).getUserId(), allFailed.get(0).getEventTime(), allFailed.get(allFailed.size()-1).getEventTime(), "user failed for " + allFailed.size() + " times"));
                }

                failedEvent.clear();
                context.timerService().deleteEventTimeTimer(timerStartTime.value());
                timerStartTime.clear();
            }

            if (loginEvent.getEventType().equals("fail")) {
                if (!failedEvent.get().iterator().hasNext()) {
                    context.timerService().registerEventTimeTimer(loginEvent.getEventTime() + 2000);
                    timerStartTime.update(loginEvent.getEventTime() + 2000);
                }
                failedEvent.add(loginEvent);
            } else {
                if (failedEvent.get().iterator().hasNext()) {
                    failedEvent.clear();
                    context.timerService().deleteEventTimeTimer(timerStartTime.value());
                    timerStartTime.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, LoginEvent, WarnMsg>.OnTimerContext ctx, Collector<WarnMsg> out) throws Exception {
            List<LoginEvent> allFailed = new ArrayList<>();
            for (LoginEvent loginEvent : failedEvent.get()) {
                allFailed.add(loginEvent);
            }
            System.out.println("ts " + timestamp +" user " + ctx.getCurrentKey() + " failed " + allFailed.size());
            if (allFailed.size() > MAX_FAIL_TIMES) {
                out.collect(new WarnMsg(ctx.getCurrentKey(), allFailed.get(0).getEventTime(), allFailed.get(allFailed.size()-1).getEventTime(), "user failed for " + allFailed.size() + " times"));
            }

            failedEvent.clear();
        }
    }
}
