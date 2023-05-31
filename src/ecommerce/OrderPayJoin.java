package ecommerce;

import ecommerce.model.OrderEvent;
import ecommerce.model.ReceiptEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Tuple2;

public class OrderPayJoin {
    private static final String orderPath = "resources/OrderLog.csv";
    private static final String payPath = "resources/ReceiptLog.csv";
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<OrderEvent> order = env.readTextFile(orderPath)
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String s) throws Exception {
                        String[] data = s.split(",");

                        return new OrderEvent(data[0], data[1],data[2], Long.parseLong(data[3])*1000);
                    }
                })
                .filter(r -> r.getEventType().equals("pay") && !r.getTxId().isEmpty())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<OrderEvent>(Time.seconds(1)) {

                    @Override
                    public long extractTimestamp(OrderEvent orderEvent) {
                        return orderEvent.getEventTime();
                    }


                }).keyBy(r -> r.getTxId());
        DataStream<ReceiptEvent> pay = env.readTextFile(payPath)
                .map(new MapFunction<String, ReceiptEvent>() {
                    @Override
                    public ReceiptEvent map(String s) throws Exception {
                        String[] data = s.split(",");

                        return new ReceiptEvent(data[0], data[1], Long.parseLong(data[2])*1000);
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<ReceiptEvent>() {
                    @Override
                    public long extractAscendingTimestamp(ReceiptEvent receiptEvent) {
                        return receiptEvent.getTimestamp();
                    }
                }).keyBy(r -> r.getTxId());

        ConnectedStreams<OrderEvent, ReceiptEvent> connectedStream = order.connect(pay);

        SingleOutputStreamOperator<Tuple2<OrderEvent, ReceiptEvent>> output = connectedStream.process(new OrderPayDetect());

        output.print("matched");
        output.getSideOutput(new OutputTag<ReceiptEvent>("unmatched-pay"){}).print("unmatched-pay");
        output.getSideOutput(new OutputTag<ReceiptEvent>("unmatched-order"){}).print("unmatched-order");

        env.execute();
    }

    public static class OrderPayDetect extends CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>> {
        private ValueState<OrderEvent> orderEventValueState;
        private ValueState<ReceiptEvent> receiptEventValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            ValueStateDescriptor<OrderEvent> orderEventValueStateDescriptor = new ValueStateDescriptor<OrderEvent>("order-state", OrderEvent.class);
            ValueStateDescriptor<ReceiptEvent> receiptEventValueStateDescriptor = new ValueStateDescriptor<ReceiptEvent>("pay-state", ReceiptEvent.class);

            orderEventValueState = getRuntimeContext().getState(orderEventValueStateDescriptor);
            receiptEventValueState = getRuntimeContext().getState(receiptEventValueStateDescriptor);
        }

        @Override
        public void processElement1(OrderEvent orderEvent, CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>.Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            if (receiptEventValueState.value() != null) {
                collector.collect(new Tuple2<>(orderEvent, receiptEventValueState.value()));
                receiptEventValueState.clear();
            } else {
                orderEventValueState.update(orderEvent);
                context.timerService().registerEventTimeTimer(orderEvent.getEventTime() + Time.seconds(5).toMilliseconds());
            }
        }

        @Override
        public void processElement2(ReceiptEvent receiptEvent, CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>.Context context, Collector<Tuple2<OrderEvent, ReceiptEvent>> collector) throws Exception {
            if (orderEventValueState.value() != null) {
                collector.collect(new Tuple2<>(orderEventValueState.value(), receiptEvent));
                orderEventValueState.clear();
            } else {
                receiptEventValueState.update(receiptEvent);
                context.timerService().registerProcessingTimeTimer(receiptEvent.getTimestamp() + Time.seconds(3).toMilliseconds());
            }
        }

        @Override
        public void onTimer(long timestamp, CoProcessFunction<OrderEvent, ReceiptEvent, Tuple2<OrderEvent, ReceiptEvent>>.OnTimerContext ctx, Collector<Tuple2<OrderEvent, ReceiptEvent>> out) throws Exception {
            if (receiptEventValueState.value() != null) {
                ctx.output(new OutputTag<ReceiptEvent>("unmatched-pay"){}, receiptEventValueState.value());
            }
            if (orderEventValueState.value() != null) {
                ctx.output(new OutputTag<OrderEvent>("unmatched-order"){}, orderEventValueState.value());
            }

            orderEventValueState.clear();
            receiptEventValueState.clear();
        }
    }
}
