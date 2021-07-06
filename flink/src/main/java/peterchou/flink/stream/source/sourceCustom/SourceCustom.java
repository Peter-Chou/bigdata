package peterchou.flink.stream.source.sourceCustom;

import java.util.HashMap;
import java.util.Random;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import peterchou.flink.stream.beams.sensor.SensorData;

public class SourceCustom {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    DataStreamSource<SensorData> dataStream = env.addSource(new MySensorSource());

    dataStream.print();

    env.execute();
  }

  public static class MySensorSource implements SourceFunction<SensorData> {

    private boolean running = true;

    @Override
    public void cancel() {
      running = false;
    }

    @Override
    public void run(SourceContext<SensorData> ctx) throws Exception {
      Random rand = new Random();

      HashMap<String, Double> sensorMap = new HashMap<String, Double>();
      for (int i = 0; i < 10; i++) {
        sensorMap.put("sensor_" + (i + 1), 60 + rand.nextGaussian() * 20);
      }

      while (running) {
        for (String sensorId : sensorMap.keySet()) {
          Double newTemp = sensorMap.get(sensorId) + rand.nextGaussian();
          sensorMap.put(sensorId, newTemp);
          ctx.collect(new SensorData(sensorId, System.currentTimeMillis() / 1000, newTemp));
        }
        Thread.sleep(1000L);
      }

    }
  }
}
