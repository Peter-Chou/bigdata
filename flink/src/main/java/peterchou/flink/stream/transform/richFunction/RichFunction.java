package peterchou.flink.stream.transform.richFunction;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import peterchou.flink.stream.beams.sensor.SensorData;
import peterchou.flink.stream.source.sourceFile.SourceFile.MyMapper;

public class RichFunction {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<String> data = env.readTextFile("../data/sensor.txt");
    DataStream<SensorData> sensorData = data.map(new MyMapper());

    SingleOutputStreamOperator<Tuple2<String, Integer>> results = sensorData.map(new MyRichMapper());

    results.print();

    env.execute();
  }

  public static class MyRichMapper extends RichMapFunction<SensorData, Tuple2<String, Integer>> {

    @Override
    public Tuple2<String, Integer> map(SensorData value) throws Exception {
      return new Tuple2<>(value.getId(), getRuntimeContext().getIndexOfThisSubtask() + 1);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
      // 初始化工作 （定义状态，建立数据库链接）
      System.out.println("open: " + (getRuntimeContext().getIndexOfThisSubtask() + 1));
    }

    @Override
    public void close() throws Exception {
      // 收尾操作（清空状态， 断开连接）
      System.out.println("close: " + (getRuntimeContext().getIndexOfThisSubtask() + 1));
    }
  }
}
