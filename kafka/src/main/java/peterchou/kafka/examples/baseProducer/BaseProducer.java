package peterchou.kafka.examples.baseProducer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class BaseProducer {
  public static void main(String[] args) throws InterruptedException, ExecutionException {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

    for (int i = 0; i < 100; ++i) {
      Future<RecordMetadata> future = kafkaProducer.send(new ProducerRecord<String, String>("test", null, i + ""));

      // 调用future的get方法等待响应
      future.get();
      System.out.println("第" + i + "条消息写入成功！");
    }
    kafkaProducer.close();

  }
}
