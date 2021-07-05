package peterchou.flink.stream.source.sourceFile;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import peterchou.flink.stream.beams.sensor.SensorData;

public class SourceFile {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<String> data = env.readTextFile("../data/sensor.txt");
    SingleOutputStreamOperator<SensorData> sensorData = data.map(new MyMapper());
    sensorData.print();

    env.execute();
  }

  public static class MyMapper implements MapFunction<String, SensorData> {

    @Override
    public SensorData map(String value) throws Exception {
      String[] args = value.split(",");
      return new SensorData(args[0], Long.parseLong(args[1]), Double.parseDouble(args[2]));
    }

  }
}
