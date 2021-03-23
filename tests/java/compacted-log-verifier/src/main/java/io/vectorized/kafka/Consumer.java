package io.vectorized.kafka;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer implements ConsumerRebalanceListener {

  static class PartitionMeta {
    long consumerPosition;
    Instant lastUpdate;
  }

  static final Logger logger = LoggerFactory.getLogger(Consumer.class);

  Consumer(io.vectorized.kafka.configuration.ConsumerConfig consumerConfig) {
    this.config = consumerConfig;
    this.properties = consumerConfig.parseProperties();
    properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, config.consumerGroup());
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokers());
    consumerConfig.securityProperties().ifPresent(properties::putAll);

    this.stats = new Stats(100_000_000, 2000, "consumer");
    this.consumerPartitions = new HashMap<>();
  }

  void startConsumer() {
    logger.info("Starting Kafka consumer with configuration: {}", properties);
    try (
        KafkaConsumer<byte[], byte[]> consumer
        = new KafkaConsumer<>(properties)) {

      consumer.subscribe(Collections.singletonList(config.topic()), this);

      // we always read up to the end of topic
      final StopWatch sw = StopWatch.createStarted();
      while (true) {
        StopWatch watch = StopWatch.createStarted();
        final ConsumerRecords<byte[], byte[]> records
            = consumer.poll(Duration.ofMillis(500));
        onConsumedRecords(records, watch.getTime(TimeUnit.MILLISECONDS));
        Set<TopicPartition> assignedPartitions = consumer.assignment();
        if (records.isEmpty()) {
          final Map<TopicPartition, Long> partitionEnds
              = consumer.endOffsets(assignedPartitions);
          boolean finished = true;
          /// empty subscriptions, retry consumer.poll()
          if (partitionEnds.isEmpty()
              && emptySubscriptionsCnt < MAX_EMPTY_SUB_RETRIES) {
            emptySubscriptionsCnt++;
            continue;
          }
          for (Map.Entry<TopicPartition, Long> partitionLastOffset :
               partitionEnds.entrySet()) {

            long position = consumer.position(partitionLastOffset.getKey());
            logger.info(
                "Partition '{}' max offset {}, current consumer position {}",
                partitionLastOffset.getKey(), partitionLastOffset.getValue(),
                position);
            if (position < partitionLastOffset.getValue()) {
              consumerPartitions.compute(
                  partitionLastOffset.getKey(),
                  (TopicPartition tp, PartitionMeta meta) -> {
                    if (meta == null || meta.consumerPosition < position) {
                      meta = new PartitionMeta();
                      meta.consumerPosition = position;
                      meta.lastUpdate = Instant.now();
                    }
                    return meta;
                  });
              finished = false;
            }
          }
          // Heuristic for stopping consumer when there when there were no
          // position updates in last 5 seconds - fix for last records being not
          // consumable.
          if (!finished) {
            finished = true;
            for (Map.Entry<TopicPartition, PartitionMeta> meta :
                 consumerPartitions.entrySet()) {
              if (meta.getValue().lastUpdate.toEpochMilli()
                  > Instant.now().minus(Duration.ofSeconds(5)).toEpochMilli()) {
                finished = false;
              }
            }
          }
          if (finished) {
            break;
          }
        }
      }
      logger.info(
          "Consumed {} records in {} ms", consumedState.getTotalRecords(),
          sw.getTime(TimeUnit.MILLISECONDS));
    } catch (Throwable t) {
      logger.error(
          "Error during processing, terminating consumer process: ", t);
    }
  }

  /**
   * Validates the expected and currently consumed state
   *
   * @return true when there were no validation errors or state file wasn't
   *     available
   */
  public boolean maybeValidateState() {
    return config.statePath()
        .map(Paths::get)
        .map(p -> {
          if (Files.exists(p)) {
            try {
              return StateMap.readFrom(p);
            } catch (IOException e) {
              logger.error("Unable to read state file from {}", p);
            }
          }
          return null;
        })
        .map(expected -> StateMap.compareStates(expected, consumedState))
        .orElse(true);
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    partitions.forEach((TopicPartition tp) -> {
      logger.info("Partition {}:{} assigned", tp.topic(), tp.partition());
    });
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    partitions.forEach((TopicPartition tp) -> {
      logger.info("Partition {}:{} revoked", tp.topic(), tp.partition());
    });
  }

  private void
  onConsumedRecords(ConsumerRecords<byte[], byte[]> records, long latency) {
    long now = Instant.now().toEpochMilli();
    for (ConsumerRecord<byte[], byte[]> rec : records) {
      consumedState.updateRecord(rec.key(), rec.value(), rec.offset());
      stats.record(
          cnt++, (int)latency,
          rec.serializedKeySize() + rec.serializedValueSize(), now);
    }
  }

  io.vectorized.kafka.configuration.ConsumerConfig config;
  Properties properties;
  private final Stats stats;
  final StateMap consumedState = new StateMap();
  final Map<TopicPartition, PartitionMeta> consumerPartitions;
  private int cnt = 0;
  int emptySubscriptionsCnt = 0;
  private static final int MAX_EMPTY_SUB_RETRIES = 10;
}
