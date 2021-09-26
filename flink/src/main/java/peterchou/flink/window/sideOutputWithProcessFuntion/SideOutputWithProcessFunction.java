package peterchou.flink.window.sideOutputWithProcessFuntion;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import peterchou.flink.stream.beams.sensor.SensorData;
import peterchou.flink.stream.source.sourceFile.SourceFile.MyMapper;

public class SideOutputWithProcessFunction {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStreamSource<String> inputData = env.socketTextStream("localhost", 7777);
    SingleOutputStreamOperator<SensorData> sensorData = inputData.map(new MyMapper());

    // 定义一个OutputTag，用来表示侧输出流——低温流
    OutputTag<SensorData> lowTempTag = new OutputTag<SensorData>("low-temp") {
    };

    // 自定义侧输出实现分流操作
    SingleOutputStreamOperator<SensorData> highTempStream = sensorData
        .process(new ProcessFunction<SensorData, SensorData>() {

          @Override
          public void processElement(SensorData value, ProcessFunction<SensorData, SensorData>.Context ctx,
              Collector<SensorData> out) throws Exception {

            if (value.getTemperature() > 30) {
              out.collect(value);
            } else {
              ctx.output(lowTempTag, value);
            }
          }

        });
    highTempStream.print("high-temp");
    highTempStream.getSideOutput(lowTempTag).print("low-temp");

  }
}
