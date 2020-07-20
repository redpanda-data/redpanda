package io.vectorized.kafka;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.ProducerRecord;

public class RecordsGenerator {

  private Object ProducerRecord;

  public RecordsGenerator(int keySetCardinality, int payloadSize, int keySize) {
    this.keySetCardinality = keySetCardinality;
    this.payloadSize = payloadSize;
    this.keySize = keySize;
    this.random = new Random(0);
  }

  byte[] padLongWithZeros(long value, int size) {
    final ByteBuffer buf = ByteBuffer.allocate(size);
    buf.putLong(value);
    byte[] keyBytes = new byte[size - Long.BYTES];
    Arrays.fill(keyBytes, (byte)0);
    buf.put(keyBytes);
    return buf.array();
  }

  public ProducerRecord<byte[], byte[]> nextRecord(final String topic) {

    return new ProducerRecord<>(
        topic, padLongWithZeros(random.nextInt(keySetCardinality), keySize),
        padLongWithZeros(cnt++, payloadSize));
  }

  int keySetCardinality;
  int payloadSize;
  int keySize;
  long cnt = 0;
  Random random;
}
