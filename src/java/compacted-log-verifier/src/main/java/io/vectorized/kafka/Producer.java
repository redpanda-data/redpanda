package io.vectorized.kafka;

import io.vectorized.kafka.configuration.ProducerConfig;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {

  private static final Logger logger = LoggerFactory.getLogger(Producer.class);

  Producer(final ProducerConfig config) {
    this.config = config;
    this.properties = config.parseProperties();
    properties.put(
        org.apache.kafka.clients.producer.ProducerConfig
            .COMPRESSION_TYPE_CONFIG,
        config.compression());
    properties.put(
        org.apache.kafka.clients.producer.ProducerConfig
            .KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    properties.put(
        org.apache.kafka.clients.producer.ProducerConfig
            .VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    properties.put(
        org.apache.kafka.clients.producer.ProducerConfig
            .BOOTSTRAP_SERVERS_CONFIG,
        config.brokers());

    config.securityProperties().ifPresent(properties::putAll);
    this.stats = new Stats(config.recordsCount(), 2000, "producer");
    this.generator = new RecordsGenerator(
        config.keyCardinality(), config.payloadSize(), config.keySize());
    this.state = new StateMap();
  }

  private void createTopic() throws ExecutionException, InterruptedException {

    final Properties adminProperties = new Properties();
    adminProperties.setProperty(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokers());
    config.securityProperties().ifPresent(adminProperties::putAll);

    try (AdminClient admin = AdminClient.create(adminProperties)) {
      admin.listTopics()
          .names()
          .thenApply((topics) -> {
            if (topics.contains(config.topic())) {
              logger.info("Topic '{}' already exists", config.topic());
              return KafkaFuture.<Void>completedFuture(null);
            }
            final NewTopic nTopic = new NewTopic(
                config.topic(), config.partitions(),
                config.replicationFactor());
            nTopic.configs(Map.of(
                TopicConfig.CLEANUP_POLICY_CONFIG,
                TopicConfig.CLEANUP_POLICY_COMPACT,
                TopicConfig.SEGMENT_BYTES_CONFIG,
                String.valueOf(config.segmentSize())));
            logger.info(
                "Creating topic {} with RF: {} and {} partitions",
                nTopic.name(), nTopic.replicationFactor(),
                nTopic.numPartitions());
            return admin.createTopics(Collections.singletonList(nTopic));
          })
          .get();

      Set<String> topics = admin.listTopics().names().get();
      while (!topics.contains(config.topic())) {
        topics = admin.listTopics().names().get();
      }
    }
  }

  void startProducer() throws Exception {
    logger.info("Starting Kafka consumer with configuration: {}", config);
    createTopic();
    // produce
    final StopWatch sw = StopWatch.createStarted();
    try (
        KafkaProducer<byte[], byte[]> producer
        = new KafkaProducer<>(properties)) {

      LongStream.range(0, config.recordsCount()).forEach(i -> {
        var rec = generator.nextRecord(config.topic());
        var ts = Instant.now().toEpochMilli();
        producer.send(rec, (RecordMetadata metadata, Exception exception) -> {
          stats.record(
              i, (int)(Instant.now().toEpochMilli() - ts),
              metadata.serializedKeySize() + metadata.serializedValueSize(),
              ts);
          state.updateRecord(rec.key(), rec.value(), metadata.offset());
        });
      });
    } finally {
      logger.info(
          "Produced {} records in {} ms", state.getTotalRecords(),
          sw.getTime(TimeUnit.MILLISECONDS));
      String path = config.statePath().orElse("topic.state");
      logger.info("Saving state to {}", path);
      state.write(Paths.get(path));
      stats.printTotal();
    }
  }
  private final Stats stats;
  private final RecordsGenerator generator;
  private final ProducerConfig config;
  private final Properties properties;
  private final StateMap state;
}
