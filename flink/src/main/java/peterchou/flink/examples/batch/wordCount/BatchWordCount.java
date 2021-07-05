package peterchou.flink.examples.batch.wordCount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSet<String> texts = env.readTextFile("../data/wordcount");

    DataSet<Tuple2<String, Integer>> results = texts.flatMap(new Tokenizer()).groupBy(0).sum(1);

    results.print();
  }

  public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
      String[] tokens = value.split("\\W+");

      for (String token : tokens) {
        if (token.length() > 0) {
          out.collect(new Tuple2<String, Integer>(token, 1));
        }
      }
    }
  }
}
