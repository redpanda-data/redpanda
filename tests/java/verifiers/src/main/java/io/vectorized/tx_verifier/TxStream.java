package io.vectorized.tx_verifier;

import java.lang.Math;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

public class TxStream {
  String connection;

  Producer<String, String> producer;

  TopicPartition tp;
  List<TopicPartition> tps;
  String topic;
  Consumer<String, String> consumer;
  String groupId;

  public TxStream(String connection) { this.connection = connection; }

  public void initProducer(String txId) throws Exception {
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
    this.producer.initTransactions();
  }

  public void
  initConsumer(String topic, String groupId, boolean isReadCommittedIsolation) {
    this.topic = topic;
    this.tp = new TopicPartition(topic, 0);
    this.tps = Collections.singletonList(new TopicPartition(topic, 0));

    var isolation
        = isReadCommittedIsolation ? "read_committed" : "read_uncommitted";

    Properties cprops = new Properties();
    cprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connection);
    cprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    cprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    cprops.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolation);
    cprops.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    cprops.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    cprops.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");

    this.groupId = groupId;
    this.consumer = new KafkaConsumer<>(cprops);
    this.consumer.subscribe(Collections.singleton(topic));
  }

  public void close() {
    if (this.producer != null) this.producer.close();
    if (this.consumer != null) this.consumer.close();
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

  public void setGroupStartOffset(long start) throws Exception {
    Map<TopicPartition, OffsetAndMetadata> offsets;

    producer.beginTransaction();
    offsets = new HashMap<>();
    offsets.put(tp, new OffsetAndMetadata(start));
    producer.sendOffsetsToTransaction(offsets, groupId);
    producer.commitTransaction();
  }

  public long getGroupOffset() throws Exception {
    return this.consumer.committed(new TopicPartition(topic, 0)).offset();
  }

  public Map<Long, Long> process(
      long end, Function<String, String> transform, int batchSize,
      String target_topic) throws Exception {
    Map<TopicPartition, OffsetAndMetadata> offsets;
    Map<Long, Long> mapping = new HashMap<>();

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(batchSize);
      var it = records.iterator();
      while (it.hasNext()) {
        var record = it.next();
        long source_offset = record.offset();

        String key = record.key();
        String value = record.value();

        producer.beginTransaction();
        var future = producer.send(new ProducerRecord<String, String>(
            target_topic, key, transform.apply(value)));
        long target_offset = future.get().offset();
        offsets = new HashMap<>();
        offsets.put(tp, new OffsetAndMetadata(source_offset + 1));
        producer.sendOffsetsToTransaction(offsets, groupId);
        producer.commitTransaction();

        mapping.put(target_offset, source_offset);

        if (source_offset >= end) {
          return mapping;
        }
      }
    }
  }
}
