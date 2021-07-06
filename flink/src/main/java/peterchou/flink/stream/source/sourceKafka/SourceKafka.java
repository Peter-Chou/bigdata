package peterchou.flink.stream.source.sourceKafka;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import peterchou.flink.stream.beams.sensor.SensorData;
import peterchou.flink.stream.source.sourceFile.SourceFile.MyMapper;

public class SourceKafka {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    DataStream<String> dataStream = env
        .addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties));

    SingleOutputStreamOperator<SensorData> sensorData = dataStream.map(new MyMapper());
    sensorData.print();

    env.execute();
  }
}
