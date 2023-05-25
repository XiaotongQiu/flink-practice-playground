package dataSource;

import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class localDataGen {
    public static void main(String[] agrs) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // use String[]
        String[] arrInput = new String[]{"hello word", "get hands dirty", "with flink"};
        DataStream<String> text = env.fromElements(arrInput);

        // use List
        List<String> listInput = new ArrayList<>(Arrays.asList(arrInput));
        DataStream<String> text2 = env.fromCollection(listInput);

//        // use socket
//        DataStream<String> text3 = env.socketTextStream("localhost", 9998, "\n", 4);
//
//        // from local dir
//        String path = "/resources/some.log";
//        DataStream<String> text4 = env.readTextFile(path);
//        DataStream<String> text5 = env.readFile(new CsvInputFormat<String>(new Path(path)) {
//            @Override
//            protected String fillRecord(String s, Object[] objects) {
//                return null;
//            }
//        }, path, FileProcessingMode.PROCESS_CONTINUOUSLY, 10);

        text.print();

        env.execute();

    }
}
