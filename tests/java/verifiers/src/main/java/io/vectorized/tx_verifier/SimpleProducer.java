package io.vectorized.tx_verifier;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class SimpleProducer {
  public Producer<String, String> producer;

  public SimpleProducer(String connection) {
    Properties pprops = new Properties();
    pprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connection);
    pprops.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
    pprops.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    pprops.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");

    this.producer = new KafkaProducer<>(pprops);
  }

  public void close() { this.producer.close(); }

  public long send(String topic, String key, String value) throws Exception {
    var future
        = producer.send(new ProducerRecord<String, String>(topic, key, value));
    return future.get().offset();
  }
}
