package peterchou.flink.examples.stream.wordCount;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import peterchou.flink.examples.batch.wordCount.BatchWordCount.Tokenizer;

public class StreamWordCount {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    ParameterTool params = ParameterTool.fromArgs(args);
    String host = params.get("host");
    int port = params.getInt("port");

    DataStreamSource<String> inputData = env.socketTextStream(host, port);

    SingleOutputStreamOperator<Tuple2<String, Integer>> results = inputData.flatMap(new Tokenizer()).keyBy(0).sum(1);

    results.print().setParallelism(1);

    env.execute("stream word count");
  }
}
