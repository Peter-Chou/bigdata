package peterchou.flink.window.watermark;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import peterchou.flink.stream.beams.sensor.SensorData;
import peterchou.flink.stream.source.sourceFile.SourceFile.MyMapper;

public class Watermark {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    env.getConfig().setAutoWatermarkInterval(500);

    OutputTag<SensorData> outputTag = new OutputTag<SensorData>("late") {

    };

    DataStreamSource<String> inputData = env.socketTextStream("localhost", 7777);

    DataStream<SensorData> eventData = inputData.map(new MyMapper())
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorData>(Time.seconds(1)) {

          @Override
          public long extractTimestamp(SensorData element) {
            return element.getTimeStamp() * 1000L;
          }

        });
    // 基于事件时间的开窗聚合，统计15秒内的温度最小值
    SingleOutputStreamOperator<SensorData> minTempStream = eventData.keyBy("id").timeWindow(Time.seconds(15))
        .allowedLateness(Time.minutes(1)).sideOutputLateData(outputTag).minBy("temperature");

    minTempStream.print("min temp:");
    // minTempStream.getSideOutput(outputTag).print()
    minTempStream.getSideOutput(outputTag).print("late");

    env.execute();
  }
}
