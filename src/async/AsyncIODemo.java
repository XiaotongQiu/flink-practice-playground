package async;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class AsyncIODemo {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(2);

        DataStream<String> input = env.fromElements(AsyncIOData.WORDS);
        System.out.println(env.getParallelism());

        SingleOutputStreamOperator<String> output = input.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                System.out.println(" input data: " + s + " current time: " + System.currentTimeMillis());
                return s;
            }
        });

        DataStream<String> asyncStream = AsyncDataStream.unorderedWait(output, new RichAsyncFunction<String, String>() {

            @Override
            public void asyncInvoke(String s, ResultFuture<String> resultFuture) throws Exception {
                System.out.println("Starting async... " + s);

                Random random = new Random();
                long duration = random.nextInt(10);

                Thread.sleep(duration * 1000);

                String out = s + s;
                resultFuture.complete(Collections.singleton(out));

                System.out.println("Completed async " + s + " sleep for " + duration);
            }
        }, 20000L, TimeUnit.MILLISECONDS);

        output.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                System.out.println("output " + s + " current time " + System.currentTimeMillis());
                return s;
            }
        });
        asyncStream.map(new MapFunction<String, Object>() {
            @Override
            public Object map(String s) throws Exception {
                System.out.println("After async: " + s + " current time " + System.currentTimeMillis());
                return s;
            }
        });

        env.execute();
    }
}
