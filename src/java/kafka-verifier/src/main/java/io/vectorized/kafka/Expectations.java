package io.vectorized.kafka;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class Expectations {
  static final Logger logger = LoggerFactory.getLogger(Expectations.class);

  private class RecordExpectations {
    RecordMetadata md;
    long timestamp;
    long cnt;
  };

  private final ConcurrentHashMap<
      Integer, ConcurrentLinkedDeque<RecordExpectations>> partitionRecords
      = new ConcurrentHashMap<>();

  private final ConcurrentLinkedDeque<ConsumerRecord<byte[], byte[]>> records
      = new ConcurrentLinkedDeque<>();

  private long cnt = 0;
  private long error_count = 0;
  private long verified_count = 0;

  Expectations() {}

  void put(RecordMetadata md, long ts, long cnt) {
    logger.trace("P: {}:{}", md.partition(), md.offset());
    partitionRecords.compute(
        md.partition(),
        (Integer partition,
         ConcurrentLinkedDeque<RecordExpectations> queue) -> {
          if (queue == null) {
            queue = new ConcurrentLinkedDeque<>();
          }

          RecordExpectations expectations = new RecordExpectations();
          expectations.cnt = cnt;
          expectations.timestamp = ts;
          expectations.md = md;
          queue.add(expectations);
          return queue;
        });
  };

  void consumed(ConsumerRecord<byte[], byte[]> record) {
    logger.trace("C: {}:{}", record.partition(), record.offset());
    records.add(record);
    cnt++;
    verify();
  }

  void verify() {
    ConsumerRecord<byte[], byte[]> consumedRecord = records.peekFirst();
    if (consumedRecord == null) {
      logger.info("No new records");
      return;
    }

    int partition = consumedRecord.partition();
    ConcurrentLinkedDeque<RecordExpectations> queue
        = partitionRecords.get(partition);
    if (queue == null || queue.isEmpty()) {
      logger.debug("No queue for {} partition", partition);
      return;
    }
    records.pollFirst();
    RecordExpectations ex = queue.removeFirst();
    cnt--;
    ByteBuffer buffer = ByteBuffer.wrap(consumedRecord.value());
    long consumedTs = buffer.getLong();
    long consumedCnt = buffer.getLong();
    logger.debug(
        "Offset: consumed: {}, expected: {}, partition: {}",
        consumedRecord.offset(), ex.md.offset(), partition);
    logger.debug("Counter: consumed: {}, expected: {}", consumedCnt, ex.cnt);
    logger.debug(
        "Timestamp: consumed: {}, expected: {}", consumedTs, ex.timestamp);

    if (consumedCnt != ex.cnt || consumedTs != ex.timestamp
        || consumedRecord.offset() != ex.md.offset()) {
      logger.error(
          "Counter mismatch: consumed: {}, expected: {}, partition: {}",
          consumedCnt, ex.cnt, partition);
      logger.error(
          "Timestamp mismatch: consumed: {}, expected: {}", consumedTs,
          ex.timestamp);
      logger.error(
          "Offset mismatch: consumed: {}, expected: {}, partition: {}",
          consumedRecord.offset(), ex.md.offset(), partition);
      error_count++;
    }
    verified_count++;
  }

  void printQueues() { logger.debug("Consumed records in queue: {}", cnt); }

  void finish() {
    while (!records.isEmpty()) {
      verify();
    }
    printQueues();
    logger.info(
        "SUMMARY: Verified {} records, {} errors", verified_count, error_count);
  }
}
