package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        // Stream env
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // input
        DataStream<String> text = env.fromElements(WordCountData.WORDS);

        DataStream<Tuple2<String, Integer>> count = text
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] strs = s.toLowerCase().split(" ");
                for (String word : strs) {
                    if (word.length() > 0) {
                        collector.collect(new Tuple2<>(word, 1));
                    }
                }
            }
        })
                .keyBy(0).sum(1);

        count.print();

        env.execute("word count");


    }
}
