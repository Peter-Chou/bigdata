package peterchou.flink.window.watermark;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import peterchou.flink.stream.beams.sensor.SensorData;
import peterchou.flink.stream.source.sourceFile.SourceFile.MyMapper;

public class Watermark {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().setAutoWatermarkInterval(500);

    DataStreamSource<String> inputData = env.socketTextStream("localhost", 7777);

    DataStream<SensorData> sensorData = inputData.map(new MyMapper());
    sensorData.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorData>(Time.seconds(1)) {

      @Override
      public long extractTimestamp(SensorData element) {
        return element.getTimeStamp() * 1000L;
      }

    });
  }
}
