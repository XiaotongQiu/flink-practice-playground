package ecommerce;

import ecommerce.model.OrderEvent;
import ecommerce.model.OrderResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;

public class OrderTimeout {
    private static final String path = "/resources/OrderLog.csv";
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<OrderEvent> input = env.readTextFile(path)
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String s) throws Exception {
                        String[] data = s.split(",");

                        return new OrderEvent(data[0], data[1],data[2], Long.parseLong(data[3])*1000);
                    }
                })
                        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.seconds(1)) {

                            @Override
                            public long extractTimestamp(OrderEvent orderEvent) {
                                return orderEvent.getEventTime();
                            }


                        });

        input.addSink(new RichSinkFunction<OrderEvent>() {
            @Override
            public void invoke(OrderEvent value, Context context) throws Exception {
                long currentWatermark = context.currentWatermark();
                System.out.println("Current Watermark: " + currentWatermark);
            }
        });

        SingleOutputStreamOperator<OrderResult> output = input.keyBy(new KeySelector<OrderEvent, String>() {
            @Override
            public String getKey(OrderEvent orderEvent) throws Exception {
                return orderEvent.getOrderId();
            }
        }).process(new TimeOutWarn());

        output.print();

        output.getSideOutput(new OutputTag<OrderResult>("timeout"){}).print();



        env.execute();
    }

    public static class TimeOutWarn extends KeyedProcessFunction<String, OrderEvent, OrderResult> {
        private ValueState<String> eventState;
        private ValueState<Long> timerStartState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("event-state", String.class);
            eventState = getRuntimeContext().getState(valueStateDescriptor);

            ValueStateDescriptor<Long> valueStateDescriptor1 = new ValueStateDescriptor<>("timer-start-state", Long.class);
            timerStartState = getRuntimeContext().getState(valueStateDescriptor1);

        }

        @Override
        public void processElement(OrderEvent orderEvent, KeyedProcessFunction<String, OrderEvent, OrderResult>.Context context, Collector<OrderResult> collector) throws Exception {
            System.out.println("watermark = " + context.timerService().currentWatermark());
            if (orderEvent.getEventType().equals("create")) {
                if (eventState.value() == null) { // 1.1 nothing happened before
                    eventState.update(orderEvent.getEventType());
                    long ts = orderEvent.getEventTime() + Time.minutes(15).toMilliseconds();
                    context.timerService().registerEventTimeTimer(ts);
                    timerStartState.update(ts);
                    System.out.println("set ts to " + ts + " for order " + orderEvent.getOrderId() + " eventTime="+orderEvent.getEventTime());
                } else if (eventState.value().equals("pay")) { // 1.2 pay event arrived earlier
                    collector.collect(new OrderResult(orderEvent.getOrderId(), "successful order!"));
                    context.timerService().deleteEventTimeTimer(timerStartState.value());
                    eventState.clear();
                    timerStartState.clear();
                }
            } else if (orderEvent.getEventType().equals("pay")) {
                if (eventState.value() == null) { // 2.1 create event has not arrived
                    long ts = orderEvent.getEventTime() + Time.minutes(15).toMilliseconds();
                    context.timerService().registerEventTimeTimer(ts);
                    timerStartState.update(ts);
                    eventState.update(orderEvent.getEventType());
                } else if (eventState.value().equals("create")) { // 2.2 successful order with create event
                    if (orderEvent.getEventTime() >= timerStartState.value()) {
                        System.out.println("there is a bug! " + orderEvent.getOrderId() + " pay event at " + orderEvent.getEventTime() + " watermark="+ context.timerService().currentWatermark() + " should be triggered at " + timerStartState.value());
                    }
                    collector.collect(new OrderResult(orderEvent.getOrderId(), "successful order!"));
                    context.timerService().deleteEventTimeTimer(timerStartState.value());
                    eventState.clear();
                    timerStartState.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, OrderEvent, OrderResult>.OnTimerContext ctx, Collector<OrderResult> out) throws Exception {
            System.out.println("triggered time for " + ctx.getCurrentKey() + " at " + timestamp + " cur watermark: " + ctx.timerService().currentWatermark());
            if (eventState.value() == null) {
                ctx.output(new OutputTag<OrderResult>("timeout"){}, new OrderResult(ctx.getCurrentKey(), "order not valid"));
            } else if (Objects.equals(eventState.value(), "create")) {
                ctx.output(new OutputTag<OrderResult>("timeout"){}, new OrderResult(ctx.getCurrentKey(), "Timeout! no payment made, cancel!"));
            } else if (eventState.value().equals("pay")) {
                ctx.output(new OutputTag<OrderResult>("timeout"){}, new OrderResult(ctx.getCurrentKey(), "order not valid because no record shows order created"));
            }

            eventState.clear();
            timerStartState.clear();
        }
    }
}
