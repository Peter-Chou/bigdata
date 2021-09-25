package peterchou.flink.examples.warningWithProcessFunction;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import peterchou.flink.stream.beams.sensor.SensorData;
import peterchou.flink.stream.source.sourceFile.SourceFile.MyMapper;

public class TimerProcess {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStreamSource<String> inputData = env.socketTextStream("localhost", 7777);
    SingleOutputStreamOperator<SensorData> sensorData = inputData.map(new MyMapper());

    sensorData.keyBy("id").process(new TempContinuousIncreaseWarning(10)).print();

    env.execute();
  }

  // 检测一段时间内的温度持续上升，输出报警
  public static class TempContinuousIncreaseWarning extends KeyedProcessFunction<Tuple, SensorData, String> {

    private Integer interval;

    public TempContinuousIncreaseWarning(Integer interval) {
      this.interval = interval;
    }

    private ValueState<Double> lastTempState;
    private ValueState<Long> timerTsState;

    @Override
    public void open(Configuration parameters) throws Exception {
      // TODO Auto-generated method stub
      lastTempState = getRuntimeContext()
          .getState(new ValueStateDescriptor<Double>("last-temp", Double.class, Double.MIN_VALUE));
      timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts", Long.class))
    }

    @Override
    public void processElement(SensorData value, KeyedProcessFunction<Tuple, SensorData, String>.Context ctx,
        Collector<String> out) throws Exception {

      Double lastTemp = lastTempState.value();
      Long timerTs = timerTsState.value();

      // 如果温度上升且没有定时器，注册10秒后的定时器，开始等待
      if (value.getTemperature() > lastTemp && timerTs == null) {
        Long ts = ctx.timerService().currentProcessingTime() + interval * 1000L;
        ctx.timerService().registerProcessingTimeTimer(ts);
        timerTsState.update(ts);
      }
      // 如果温度下降，删除定时器
      else if (value.getTemperature() < lastTemp && timerTs != null) {
        ctx.timerService().deleteProcessingTimeTimer(timerTs);
        timerTsState.clear();
      }
      lastTempState.update(value.getTemperature());
    }

    @Override
    public void onTimer(long timestamp, KeyedProcessFunction<Tuple, SensorData, String>.OnTimerContext ctx,
        Collector<String> out) throws Exception {
      // TODO Auto-generated method stub
      out.collect("传感器" + ctx.getCurrentKey().getField(0) + "温度值连续" + interval + "秒上升。");
      timerTsState.clear();
    }

    @Override
    public void close() throws Exception {
      lastTempState.clear();
    }
  }

}
