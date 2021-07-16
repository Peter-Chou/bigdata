package peterchou.flink.window.timeWindow;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import peterchou.flink.stream.beams.sensor.SensorData;
import peterchou.flink.stream.source.sourceFile.SourceFile.MyMapper;

public class TimeWindow {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStreamSource<String> inputData = env.socketTextStream("localhost", 7777);
    SingleOutputStreamOperator<SensorData> sensorData = inputData.map(new MyMapper());

    // KeyedStream<SensorData, String> keyedSensorData = sensorData.keyBy(sensor ->
    // sensor.getId());

    // tumbling 即增量， Process即批处理
    // 全局窗口： global window
    // pass
    // 滚动窗口： tumbling window
    // keyedSensorData.timeWindow(Time.seconds(15));
    // 滑动窗口： sliding window
    // keyedSensorData.timeWindow(Time.seconds(15), Time.seconds(5));
    // 绘画窗口： session window
    // keyedSensorData.window(EventTimeSessionWindows.withGap(Time.minutes(10)));
    DataStream<Integer> results = sensorData.keyBy(sensor -> sensor.getId())
        .window(TumblingProcessingTimeWindows.of(Time.seconds(15))).aggregate(new CountAggregateFunction()); // aggregate
                                                                                                             // 增强，
                                                                                                             // apply是全窗口

    results.print();

    env.execute();

  }
}

class CountAggregateFunction implements AggregateFunction<SensorData, Integer, Integer> {

  @Override
  public Integer createAccumulator() {
    return 0;
  }

  @Override
  public Integer add(SensorData value, Integer accumulator) {
    return accumulator + 1;
  }

  // 不用设置，主要在sessionWindow中用
  @Override
  public Integer merge(Integer a, Integer b) {
    return a + b;
  }

  @Override
  public Integer getResult(Integer accumulator) {
    return accumulator;
  }
}
