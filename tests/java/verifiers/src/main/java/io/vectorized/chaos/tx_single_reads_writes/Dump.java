package io.vectorized.chaos.tx_single_reads_writes;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class Dump {
  public static void main(String[] args) throws Exception {
    String topic = args[0];
    String brokers = args[1];

    var tp = new TopicPartition(topic, 0);
    var tps = Collections.singletonList(tp);

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    // default value: 540000
    props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 60000);
    // default value: 60000
    props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
    // default value: 500
    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
    // default value: 300000
    props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 10000);
    // default value: 1000
    props.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000);
    // default value: 50
    props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 50);
    // defaut value: 30000
    props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
    // default value: 100
    props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 100);

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.assign(tps);
    consumer.seekToEnd(tps);
    long end = consumer.position(tp);
    consumer.seekToBeginning(tps);

    while (consumer.position(tp) < end) {
      ConsumerRecords<String, String> records
          = consumer.poll(Duration.ofMillis(10000));
      var it = records.iterator();
      while (it.hasNext()) {
        var record = it.next();
        long offset = record.offset();
        long oid = Long.parseLong(record.value());
        System.out.println("" + oid + "@" + offset);
      }
    }

    consumer.close();
  }
}
