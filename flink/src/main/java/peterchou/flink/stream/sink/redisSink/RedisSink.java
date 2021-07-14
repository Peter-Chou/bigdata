package peterchou.flink.stream.sink.redisSink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import peterchou.flink.stream.beams.sensor.SensorData;
import peterchou.flink.stream.source.sourceFile.SourceFile.MyMapper;

public class RedisSink {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStream<String> data = env.readTextFile("../data/sensor.txt");
    SingleOutputStreamOperator<SensorData> sensorData = data.map(new MyMapper());

    FlinkJedisPoolConfig redisConfig = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build();

    sensorData.addSink(new SensorDataRedisSink(redisConfig, new SensorDataRedisMapper()));

    env.execute();
  }
}

class SensorDataRedisSink extends org.apache.flink.streaming.connectors.redis.RedisSink<SensorData> {

  public SensorDataRedisSink(FlinkJedisConfigBase flinkJedisConfigBase, RedisMapper<SensorData> redisSinkMapper) {
    super(flinkJedisConfigBase, redisSinkMapper);
  }

  @Override
  public void invoke(SensorData input) throws Exception {
    super.invoke(input);
  }
}

class SensorDataRedisMapper implements RedisMapper<SensorData> {

  // 定义保存数据到redis的命令, 这里存为hashmap: hset sensor_temp id temperature
  @Override
  public RedisCommandDescription getCommandDescription() {
    return new RedisCommandDescription(RedisCommand.HSET, "sensor_temp");
  }

  @Override
  public String getKeyFromData(SensorData data) {
    return data.getId();
  }

  @Override
  public String getValueFromData(SensorData data) {
    return data.getTemperature().toString();
  }

}
