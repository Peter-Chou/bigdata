package peterchou.flink.stream.sink.kafkaSink;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import peterchou.flink.stream.beams.sensor.SensorData;
import peterchou.flink.stream.source.sourceFile.SourceFile.MyMapper;

public class KafkaSink {
  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    Properties properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    DataStream<String> dataStream = env
        .addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties));

    DataStream<SensorData> sensorData = dataStream.map(new MyMapper());

    properties = new Properties();
    properties.setProperty("bootstrap.servers", "localhost:9092");
    String topic = "KafkaSink";

    sensorData.addSink(new FlinkKafkaProducer<SensorData>(topic, new SensorDataSerializationSchema(topic), properties,
        FlinkKafkaProducer.Semantic.EXACTLY_ONCE));

    env.execute();
  }
}

class SensorDataSerializationSchema implements KafkaSerializationSchema<SensorData> {
  private String topic;

  public SensorDataSerializationSchema(String topic) {
    super();
    this.topic = topic;
  }

  @Override
  public ProducerRecord<byte[], byte[]> serialize(SensorData element, Long timestamp) {
    return new ProducerRecord<byte[], byte[]>(topic, element.toString().getBytes(StandardCharsets.UTF_8));
  }
}
