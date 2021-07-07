package peterchou.flink.stream.transform.rollingAggreagtion;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import peterchou.flink.stream.beams.sensor.SensorData;
import peterchou.flink.stream.source.sourceFile.SourceFile.MyMapper;

public class RollingAggregation {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<String> data = env.readTextFile("../data/sensor.txt");
    DataStream<SensorData> sensorData = data.map(new MyMapper());

    KeyedStream<SensorData, String> keySensorData = sensorData.keyBy(sensor -> sensor.getId());
    SingleOutputStreamOperator<SensorData> results = keySensorData.maxBy("temperature");

    results.print("result");

    env.execute();
  }
}
