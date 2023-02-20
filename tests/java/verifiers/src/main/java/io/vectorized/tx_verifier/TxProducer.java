package io.vectorized.tx_verifier;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TxProducer {
  public Producer<String, String> producer;

  public TxProducer(String connection, String txId) {
    Properties pprops = new Properties();
    pprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connection);
    pprops.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    pprops.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId);
    pprops.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    pprops.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");

    this.producer = new KafkaProducer<>(pprops);
  }

  public TxProducer(String connection, String txId, int transactionTimeoutMs) {
    Properties pprops = new Properties();
    pprops.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs);
    pprops.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connection);
    pprops.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    pprops.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId);
    pprops.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    pprops.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");

    this.producer = new KafkaProducer<>(pprops);
  }

  public void close() { this.producer.close(); }

  public void initTransactions() { this.producer.initTransactions(); }

  public void beginTransaction() throws Exception {
    this.producer.beginTransaction();
  }

  public void commitTransaction() throws Exception {
    this.producer.commitTransaction();
  }

  public void abortTransaction() throws Exception {
    this.producer.abortTransaction();
  }

  public long send(String topic, String key, String value) throws Exception {
    var future
        = producer.send(new ProducerRecord<String, String>(topic, key, value));
    return future.get().offset();
  }

  public long commitTx(String topic, String key, String value)
      throws Exception {
    producer.beginTransaction();
    var future
        = producer.send(new ProducerRecord<String, String>(topic, key, value));
    long offset = future.get().offset();
    producer.commitTransaction();
    return offset;
  }

  public long abortTx(String topic, String key, String value) throws Exception {
    producer.beginTransaction();
    var future
        = producer.send(new ProducerRecord<String, String>(topic, key, value));
    long offset = future.get().offset();
    producer.abortTransaction();
    return offset;
  }
}
