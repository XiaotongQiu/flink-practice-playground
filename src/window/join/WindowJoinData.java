package window.join;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Serializable;
import utils.ThrottledIterator;

import java.util.Iterator;
import java.util.Random;

public class WindowJoinData {
    static final String[] NAMES = {"tom", "jerry", "alice", "bob", "john", "grace"};
    static final int GRADE_COUNT = 5;
    static final int SALARY_MAX = 10000;

    /**
     * Continuously generates (name, grade).
     */
    public static class GradeSource implements Iterator<Tuple2<String, Integer>>, Serializable {
        private final Random random = new Random(hashCode());


        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Tuple2<String, Integer> next() {
            return new Tuple2<>(NAMES[random.nextInt(NAMES.length)], random.nextInt(GRADE_COUNT)+1);
        }

        public static DataStream<Tuple2<String, Integer>> getSource(StreamExecutionEnvironment env, long rate) {
            return env.fromCollection(new ThrottledIterator<>(new GradeSource(), rate), TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
            }));
        }
    }

    /**
     * Continuously generates (name, salary).
     */
    public static class SalarySource implements Iterator<Tuple2<String, Integer>>, Serializable {

        private final Random rnd = new Random(hashCode());

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Tuple2<String, Integer> next() {
            return new Tuple2<>(NAMES[rnd.nextInt(NAMES.length)], rnd.nextInt(SALARY_MAX) + 1);
        }

        public static DataStream<Tuple2<String, Integer>> getSource(StreamExecutionEnvironment env, long rate) {
            return env.fromCollection(new ThrottledIterator<>(new SalarySource(), rate),
                    TypeInformation.of(new TypeHint<Tuple2<String, Integer>>(){}));
        }
    }
}
