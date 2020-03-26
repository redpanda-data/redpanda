package io.vectorized.kafka;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer implements ConsumerRebalanceListener {

  static final Logger logger = LoggerFactory.getLogger(Consumer.class);

  Consumer(
      final String topic, long numberOfRecords,
      final Properties consumerProperties, Expectations ex) {
    consumerProperties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    consumerProperties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    this.consumer = new KafkaConsumer<>(consumerProperties);
    this.topic = topic;
    this.stats = new ConsumerStats(numberOfRecords, 2000);
    this.ex = ex;
  }

  CompletableFuture<Void> startConsumer() {
    consumer.subscribe(Collections.singletonList(topic), this);
    consumer.poll(Duration.ofMillis(1000));
    consumer.partitionsFor(topic).forEach((pi) -> {
      long pos = consumer.position(new TopicPartition(topic, pi.partition()));
      logger.info("Partition {} pos: {}", pi.partition(), pos);
    });
    return CompletableFuture.runAsync(() -> {
      try {
        while (!stats.finished()) {
          ConsumerRecords<byte[], byte[]> records
              = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
          onConsumedRecords(records);
        }
      } catch (Throwable t) {
        logger.error(
            "Error during processing, terminating consumer process: ", t);
      } finally {
        consumer.close();
        ToolsUtils.printMetrics(consumer.metrics());
      }
    });
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    partitions.stream().forEach((TopicPartition tp) -> {
      logger.info("Partition {}:{} assigned", tp.topic(), tp.partition());
    });
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    partitions.stream().forEach((TopicPartition tp) -> {
      logger.info("Partition {}:{} revoked", tp.topic(), tp.partition());
    });
  }

  private void onConsumedRecords(ConsumerRecords<byte[], byte[]> records) {
    long now = Instant.now().toEpochMilli();
    for (ConsumerRecord<byte[], byte[]> rec : records) {
      ByteBuffer buffer = ByteBuffer.wrap(rec.value());
      Long publishedAt = buffer.getLong();
      stats.record(
          cnt++, (int)(now - publishedAt),
          rec.serializedKeySize() + rec.serializedValueSize(), now);
      ex.consumed(rec);
    }
  }

  private final KafkaConsumer<byte[], byte[]> consumer;
  private final String topic;
  private final ConsumerStats stats;
  private int cnt = 0;
  private final Expectations ex;
}
