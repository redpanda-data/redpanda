package io.vectorized.kafka;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer {
  Producer(
      final String topic, final long numberOfRecords, final int recordSize,
      final long throughput, final Properties producerProperties,
      ProducerStats stats) {
    producerProperties.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    producerProperties.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.ByteArraySerializer");
    this.producer = new KafkaProducer<>(producerProperties);
    this.numberOfRecords = numberOfRecords;
    this.recordSize = recordSize;
    this.throughput = throughput;
    this.topic = topic;
    this.stats = stats;
  }

  CompletableFuture<Void> startProducer() {
    final Random random = new Random(0);
    final Instant now = Instant.now();
    final long now_ts = now.toEpochMilli();
    final byte[] payload = new byte[recordSize - 16];
    IntStream.range(0, payload.length).forEach(i -> {
      payload[i] = (byte)random.nextInt(255);
    });

    final long startMs = System.currentTimeMillis();

    final ThroughputThrottler throttler
        = new ThroughputThrottler(throughput, startMs);
    final ByteBuffer bytes = ByteBuffer.allocate(recordSize);
    final byte[] recordPayload = new byte[recordSize];
    return CompletableFuture.runAsync(() -> {
      ProducerRecord<byte[], byte[]> record;
      for (long i = 0; i < numberOfRecords; i++) {
        long record_ts = Instant.now().toEpochMilli();
        bytes.rewind();
        bytes.putLong(record_ts); // 8 bytes
        bytes.putLong(cnt);       // 8 bytes
        bytes.put(payload);       // recordSize - 16 bytes;
        bytes.rewind();
        bytes.get(recordPayload);
        record = new ProducerRecord<>(topic, recordPayload);
        final long sendStartMs = System.currentTimeMillis();
        final Callback cb = stats.nextCompletion(
            sendStartMs, payload.length, stats, record_ts, cnt);
        producer.send(record, cb);
        cnt++;
        if (throttler.shouldThrottle(i, sendStartMs)) {
          throttler.throttle();
        }
      }
    });
  }

  void finish() {
    producer.flush();
    stats.printTotal();
    ToolsUtils.printMetrics(producer.metrics());
    producer.close();
  }

  private final KafkaProducer<byte[], byte[]> producer;
  private final ProducerStats stats;
  private long cnt = 0;
  String topic;
  long numberOfRecords;
  int recordSize;
  long throughput;
}
