package time;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import wordcount.WordCountData;

import javax.sql.DataSource;

public class SourceWithWatermark {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 1. assign timestamp and generate watermarks in the source
        // feel kind of wired to emit watermark this way..
        // will be replace by TimestampAssigner
        String[] elementInput = new String[]{"hello Flink, 17788900", "Second Line, 17788923"};
        DataStream<String> source = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                for (String s : elementInput) {
                    String[] input = s.split(" ");
                    Long ts = Long.parseLong(input[1]);

                    // generate event time ts
                    sourceContext.collectWithTimestamp(s, ts);

                    // emit watermark to each record, set max out of orderness to 2
                    // i.e. when ts=1000 arrives,
                    // we assume all data before 1000-2=998 should have arrived
                    sourceContext.emitWatermark(new Watermark(ts-2));
                }
            }

            @Override
            public void cancel() {

            }
        });

        source.print();


        env.execute();
    }
}
