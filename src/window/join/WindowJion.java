package window.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import window.assigner.TumblingWindow;

public class WindowJion {
    public static void main(String[] args) throws Exception{
        final long windowSize = 200L;
        final long rate = 3L;

        System.out.println("Using windowSize=" + windowSize + ", data rate=" + rate);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        DataStream<Tuple2<String, Integer>> grades = WindowJoinData.GradeSource.getSource(env, rate);
        DataStream<Tuple2<String, Integer>> salaries = WindowJoinData.SalarySource.getSource(env, rate);

        DataStream<Tuple3<String, Integer, Integer>> joinedStream = grades.join(salaries)
                .where(new KeySelector<Tuple2<String, Integer>, Object>() {
                @Override
                public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                    return stringIntegerTuple2.f0;
                }
                })
                .equalTo(new KeySelector<Tuple2<String, Integer>, Object>() {
                    @Override
                    public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return stringIntegerTuple2.f0;
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> join(Tuple2<String, Integer> stringIntegerTuple2, Tuple2<String, Integer> stringIntegerTuple22) throws Exception {
                        return new Tuple3<>(stringIntegerTuple2.f0, stringIntegerTuple2.f1, stringIntegerTuple22.f1);
                    }
                });

        joinedStream.print().setParallelism(1);

        env.execute();

    }
}
