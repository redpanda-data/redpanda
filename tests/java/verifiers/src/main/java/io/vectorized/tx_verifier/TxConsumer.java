package io.vectorized.tx_verifier;

import java.lang.Math;
import java.lang.Thread;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class TxConsumer {
  String topic;
  TopicPartition tp;
  List<TopicPartition> tps;
  Consumer<String, String> consumer;

  public TxConsumer(
      String connection, String topic, boolean isReadCommittedIsolation) {
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
    cprops.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    cprops.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");

    this.consumer = new KafkaConsumer<>(cprops);
    this.consumer.assign(this.tps);
  }

  public void seekToEnd() throws Exception { consumer.seekToEnd(tps); }

  public long position() throws Exception { return consumer.position(tp); }

  public void close() { this.consumer.close(); }

  public List<TxRecord>
  read(long start_offset, long last_offset, int timeout_ms, int attempts)
      throws Exception {
    List<TxRecord> result = new ArrayList<>();
    var attempt = 0;

    consumer.seek(tp, start_offset);

    while (attempt < attempts) {
      attempt++;
      ConsumerRecords<String, String> records = consumer.poll(timeout_ms);
      var it = records.iterator();
      while (it.hasNext()) {
        var record = it.next();

        var txRecord = new TxRecord();
        txRecord.key = record.key();
        txRecord.value = record.value();
        txRecord.offset = record.offset();

        if (record.offset() <= last_offset) {
          result.add(txRecord);
        }

        if (record.offset() >= last_offset) {
          return result;
        }
      }
    }

    throw new RetryableException(
        "can't read up to " + last_offset + " in " + attempts + " tries");
  }

  public List<TxRecord>
  readN(long start_offset, int n, int timeout_ms, int attempts)
      throws Exception {
    List<TxRecord> result = new ArrayList<>();
    var attempt = 0;

    consumer.seek(tp, start_offset);

    int read = 0;

    while (attempt < attempts) {
      attempt++;
      ConsumerRecords<String, String> records = consumer.poll(timeout_ms);
      var it = records.iterator();
      while (it.hasNext()) {
        read++;
        var record = it.next();

        var txRecord = new TxRecord();
        txRecord.key = record.key();
        txRecord.value = record.value();
        txRecord.offset = record.offset();

        result.add(txRecord);

        if (read == n) {
          return result;
        }
      }
    }

    throw new RetryableException(
        "can't read " + n + " records in " + attempts + " tries");
  }
}
