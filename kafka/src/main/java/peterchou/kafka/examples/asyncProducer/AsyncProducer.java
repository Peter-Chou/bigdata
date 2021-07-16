package peterchou.kafka.examples.asyncProducer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class AsyncProducer {
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

    for (int i = 0; i < 100; i++) {
      // 使用异步回调的方式发送消息
      ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", null, i + "");
      producer.send(record, new Callback() {

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
          // 判断消息发送是否成功,
          if (exception == null) {
            String topic = metadata.topic();
            int partition = metadata.partition();
            long offset = metadata.offset();
            System.out.println("topic: " + topic + " partition: " + partition + " offset: " + offset);

          } else {
            System.out.println("生产消息出现错误");
            System.out.println(exception.getMessage());
            System.out.println(exception.getStackTrace());
          }
        }
      });

    }
    producer.close();
  }
}
