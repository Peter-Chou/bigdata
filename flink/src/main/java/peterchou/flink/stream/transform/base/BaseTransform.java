package peterchou.flink.stream.transform.base;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import peterchou.flink.stream.beams.sensor.SensorData;
import peterchou.flink.stream.source.sourceFile.SourceFile.MyMapper;

public class BaseTransform {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<String> data = env.readTextFile("../data/sensor.txt");
    SingleOutputStreamOperator<SensorData> sensorData = data.map(new MyMapper());
    // sensorData.filter(new FilterFunction<SensorData>() {

    // });
    SingleOutputStreamOperator<SensorData> filtered = sensorData.filter((SensorData value) -> {
      return value.getId().equals("sensor_1");
    });

    filtered.print();

    env.execute();
  }

  // public static class SensorFilter implements FilterFunction<SensorData> {

  // @Override
  // public boolean filter(SensorData value) throws Exception {
  // return value.getId().startsWith("sensor_1");
  // }

  // }
}
