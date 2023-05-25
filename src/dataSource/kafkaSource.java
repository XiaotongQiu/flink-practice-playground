package dataSource;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;


public class kafkaSource {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties consumerConfig = new Properties();
        try (InputStream stream = kafkaSource.class.getClassLoader().getResourceAsStream("ccloud.properties")) {
            consumerConfig.load(stream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("flink-test", new SimpleStringSchema(), consumerConfig);

        DataStreamSource<String> source = env.addSource(consumer);

        source.print();

        env.execute();
    }
}
