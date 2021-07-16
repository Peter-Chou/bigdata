package peterchou.kafka.examples.baseConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class BaseConsumer {
  public static void main(String[] args) {

    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:9092");
    // 消费者组(将多个消费者组织在一起，) 共同消费kafak中topic中的数据
    // 每一个消费者需要指定一个消费者组，如果消费者的组名是一样的，表示这几个消费者在一个组中。
    props.setProperty("group.id", "test");
    // 自动提交offset
    props.setProperty("enable.auto.commit", "true");
    props.setProperty("auto.commit.interval.ms", "1000");
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
    // 订阅消费的主题
    consumer.subscribe(Arrays.asList("test"));

    // 不断拉取数据
    while (true) {
      ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(5));
      for (ConsumerRecord<String, String> record : consumerRecords) {
        String topic = record.topic();
        // 这条信息出于kafak分区中的哪个位置
        long offset = record.offset();

        String key = record.key();
        String value = record.value();
        System.out.println("topic: " + topic + " offset: " + offset + " key: " + key + " value: " + value);
      }
    }
  }
}
