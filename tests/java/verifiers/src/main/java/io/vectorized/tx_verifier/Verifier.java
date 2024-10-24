package io.vectorized.tx_verifier;

import static java.util.Map.entry;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;

class Verifier {
  public static interface StringAction {
    void run(String connection) throws Exception;
  }

  final static int RETRY_TIMEOUT_SEC = 5;
  final static int MAX_TRANSACTION_TIMEOUT_MS = 900000;
  final static String txId1 = "tx1";
  final static String txId2 = "tx2";
  final static String topic1 = "topic1";
  final static String topic2 = "topic2";
  final static String groupId = "groupId";

  final static Map<String, StringAction> tests = Map.ofEntries(
      entry("init", Verifier::initPasses), entry("tx", Verifier::txPasses),
      entry("txes", Verifier::txesPasses),
      entry("abort", Verifier::abortPasses),
      entry("commuting-txes", Verifier::commutingTxesPass),
      entry("conflicting-tx", Verifier::conflictingTxFails),
      entry("read-committed-seek", Verifier::readCommittedSeekTest),
      entry("read-uncommitted-seek", Verifier::readUncommittedSeekTest),
      entry("read-committed-tx-seek", Verifier::readCommittedTxSeekTest),
      entry("read-uncommitted-tx-seek", Verifier::readUncommittedTxSeekTest),
      entry("fetch-reads-committed-txs", Verifier::fetchReadsCommittedTxsTest),
      entry("fetch-skips-aborted-txs", Verifier::fetchDoesntReadAbortedTxsTest),
      entry(
          "read-committed-seek-waits-ongoing-tx",
          Verifier::readCommittedSeekRespectsOngoingTx),
      entry(
          "read-committed-seek-waits-long-hanging-tx",
          Verifier::readCommittedSeekRespectsLongHangingTx),
      entry(
          "read-committed-seek-reads-short-hanging-tx",
          Verifier::readCommittedSeekDoesntRespectShortHangingTx),
      entry(
          "read-uncommitted-seek-reads-ongoing-tx",
          Verifier::readUncommittedSeekDoesntRespectOngoingTx),
      entry("set-group-start-offset", Verifier::setGroupStartOffsetPasses),
      entry("read-process-write", Verifier::readProcessWrite),
      entry("concurrent-reads-writes", Verifier::txReadsWritesPasses));

  public static void main(final String[] args) throws Exception {
    if (args.length != 2) {
      throw new Exception("Verifier expects two args");
    }
    if (!tests.containsKey(args[0])) {
      throw new Exception("Unknown test: " + args[0]);
    }
    retry(args[0], tests.get(args[0]), args[1]);
  }

  static void retry(String name, StringAction action, String connection)
      throws Exception {
    var retries = 3;
    while (retries > 0) {
      retries--;
      try {
        System.out.println(
            "Executing " + name + " test, retries left: " + retries);
        action.run(connection);
        initPasses(connection, txId1);
        initPasses(connection, txId2);
        return;
      } catch (RetryableException e) {
        if (retries > 0) {
          e.printStackTrace();
          Thread.sleep(Duration.ofSeconds(RETRY_TIMEOUT_SEC).toMillis());
          continue;
        }
        throw e;
      } catch (KafkaException e) {
        if (retries > 0) {
          e.printStackTrace();
          Thread.sleep(Duration.ofSeconds(RETRY_TIMEOUT_SEC).toMillis());
          continue;
        }
        throw e;
      } catch (ExecutionException e) {
        var cause = e.getCause();
        if (cause != null && cause instanceof KafkaException) {
          if (retries > 0) {
            e.printStackTrace();
            Thread.sleep(Duration.ofSeconds(RETRY_TIMEOUT_SEC).toMillis());
            continue;
          }
        }
        throw e;
      }
    }
  }

  static void txReadsWritesPasses(String connection) throws Exception {
    var test = new TxReadsWritesTest(connection, topic1);
    test.run();
  }

  static void initPasses(String connection) throws Exception {
    initPasses(connection, txId1);
  }

  static void initPasses(String connection, String txId) throws Exception {
    TxProducer producer = null;
    try {
      producer = new TxProducer(connection, txId);
      producer.initTransactions();
    } finally {
      if (producer != null) {
        producer.close();
      }
    }
  }

  static void txPasses(String connection) throws Exception {
    TxProducer producer = null;
    try {
      producer = new TxProducer(connection, txId1);
      producer.initTransactions();
      producer.commitTx(topic1, "key1", "value1");
    } finally {
      if (producer != null) {
        producer.close();
      }
    }
  }

  static void txesPasses(String connection) throws Exception {
    TxProducer producer = null;
    try {
      producer = new TxProducer(connection, txId1);
      producer.initTransactions();
      producer.commitTx(topic1, "key1", "value1");
      producer.commitTx(topic1, "key2", "value2");
    } finally {
      if (producer != null) {
        producer.close();
      }
    }
  }

  static void abortPasses(String connection) throws Exception {
    TxProducer producer = null;
    try {
      producer = new TxProducer(connection, txId1);
      producer.initTransactions();
      producer.abortTx(topic1, "key1", "value1");
    } finally {
      if (producer != null) {
        producer.close();
      }
    }
  }

  static void commutingTxesPass(String connection) throws Exception {
    TxProducer p1 = null, p2 = null;
    try {
      p1 = new TxProducer(connection, txId1);
      p2 = new TxProducer(connection, txId2);
      p1.initTransactions();
      p1.beginTransaction();
      p1.send(topic1, "key1", "p1:value1");
      p2.initTransactions();
      p2.beginTransaction();
      p2.send(topic1, "key1", "p2:value1");
      p1.commitTransaction();
      p2.commitTransaction();
    } finally {
      if (p1 != null) {
        p1.close();
      }
      if (p2 != null) {
        p2.close();
      }
    }
  }

  static void conflictingTxFails(String connection) throws Exception {
    TxProducer p1 = null, p2 = null;
    try {
      p1 = new TxProducer(connection, txId1);
      p2 = new TxProducer(connection, txId1);
      p1.initTransactions();
      p1.beginTransaction();
      p1.send(topic1, "key1", "p1:value1");
      p2.initTransactions();
      p2.beginTransaction();
      p2.send(topic1, "key1", "p2:value1");
      try {
        p1.commitTransaction();
        throw new Exception("commit must throw ProducerFencedException");
      } catch (ProducerFencedException e) {
        // eating ProducerFencedException
      }
      p2.commitTransaction();
    } finally {
      if (p1 != null) {
        p1.close();
      }
      if (p2 != null) {
        p2.close();
      }
    }
  }

  static void seekTest(String connection, boolean isReadComitted)
      throws Exception {
    SimpleProducer producer = null;
    TxConsumer consumer = null;

    try {
      producer = new SimpleProducer(connection);
      long offset = producer.send(topic1, "key1", "value1");
      producer.close();
      producer = null;

      consumer = new TxConsumer(connection, topic1, isReadComitted);
      consumer.seekToEnd();

      int retries = 8;
      while (offset >= consumer.position() && retries > 0) {
        // partitions lag behind a coordinator
        // we can't avoid sleep :(
        Thread.sleep(500);
        consumer.seekToEnd();
        retries--;
      }
      assertLess(offset, consumer.position());
    } finally {
      if (producer != null) {
        producer.close();
      }
      if (consumer != null) {
        consumer.close();
      }
    }
  }

  static void readCommittedSeekTest(String connection) throws Exception {
    seekTest(connection, true);
  }

  static void readUncommittedSeekTest(String connection) throws Exception {
    seekTest(connection, false);
  }

  static void txSeekTest(String connection, boolean isReadComitted)
      throws Exception {

    TxProducer producer = null;
    TxConsumer consumer = null;

    try {
      producer = new TxProducer(connection, txId1);
      producer.initTransactions();
      long offset = producer.commitTx(topic1, "key1", "value1");
      producer.close();
      producer = null;

      consumer = new TxConsumer(connection, topic1, isReadComitted);
      long position = seekOver(consumer, offset, 500, 8);
      should(
          offset < position,
          "data partition hasn't caught up with committed tx - offset:" + offset
              + ", position: " + position);
    } finally {
      if (producer != null) {
        producer.close();
      }
      if (consumer != null) {
        consumer.close();
      }
    }
  }

  static void readCommittedTxSeekTest(String connection) throws Exception {
    txSeekTest(connection, true);
  }

  static void readUncommittedTxSeekTest(String connection) throws Exception {
    txSeekTest(connection, false);
  }

  static void fetchReadsCommittedTxsTest(String connection) throws Exception {
    TxProducer producer = null;
    TxConsumer consumer = null;

    try {
      Map<String, Long> offsets = new HashMap<>();
      producer = new TxProducer(connection, txId1);
      producer.initTransactions();
      long first_offset = producer.commitTx(topic1, "key1", "value1");
      offsets.put("key1", first_offset);
      for (int i = 2; i < 10; i++) {
        long offset = producer.commitTx(topic1, "key" + i, "value" + i);
        offsets.put("key" + i, offset);
      }
      long last_offset = producer.commitTx(topic1, "key10", "value10");
      offsets.put("key10", last_offset);
      producer.close();
      producer = null;

      consumer = new TxConsumer(connection, topic1, true);
      long position = seekOver(consumer, last_offset, 500, 8);
      should(
          last_offset < position,
          "data partition hasn't caught up with committed tx - offset:"
              + last_offset + ", position: " + position);

      var records = consumer.read(first_offset, last_offset, 500, 10);
      consumer.close();
      consumer = null;
      assertEquals(records.size(), offsets.size());
      for (var record : records) {
        assertTrue(offsets.containsKey(record.key));
        assertEquals(offsets.get(record.key), record.offset);
      }
    } finally {
      if (producer != null) {
        producer.close();
      }
      if (consumer != null) {
        consumer.close();
      }
    }
  }

  static void fetchDoesntReadAbortedTxsTest(String connection)
      throws Exception {

    TxProducer producer = null;
    TxConsumer consumer = null;
    try {
      producer = new TxProducer(connection, txId1);
      producer.initTransactions();
      long first_offset = producer.commitTx(topic1, "key1", "value1");
      producer.abortTx(topic1, "key2", "value2");
      long last_offset = producer.commitTx(topic1, "key3", "value3");
      producer.close();
      producer = null;

      consumer = new TxConsumer(connection, topic1, true);
      long position = seekOver(consumer, last_offset, 500, 8);
      should(
          last_offset < position,
          "data partition hasn't caught up with committed tx - offset:"
              + last_offset + ", position: " + position);

      var records = consumer.read(first_offset, last_offset, 500, 10);
      consumer.close();
      consumer = null;
      assertEquals(records.size(), 2);
      for (var record : records) {
        if (record.key.equals("key1")) {
          assertEquals(first_offset, record.offset);
        } else if (record.key.equals("key3")) {
          assertEquals(last_offset, record.offset);
        } else {
          fail("Unexpected key: " + record.key);
        }
      }
    } finally {
      if (producer != null) {
        producer.close();
      }
      if (consumer != null) {
        consumer.close();
      }
    }
  }

  static void readCommittedSeekRespectsOngoingTx(String connection)
      throws Exception {
    TxProducer producer = null;
    TxConsumer consumer = null;
    try {
      producer = new TxProducer(connection, txId1);
      producer.initTransactions();
      producer.beginTransaction();
      long offset = producer.send(topic1, "key1", "value1");

      consumer = new TxConsumer(connection, topic1, true);
      consumer.seekToEnd();
      assertLessOrEqual(consumer.position(), offset);

      producer.commitTransaction();
    } finally {
      if (producer != null) {
        producer.close();
      }
      if (consumer != null) {
        consumer.close();
      }
    }
  }

  static void readCommittedSeekRespectsLongHangingTx(String connection)
      throws Exception {
    TxProducer producer = null;
    TxConsumer consumer = null;
    try {
      producer = new TxProducer(connection, txId1, MAX_TRANSACTION_TIMEOUT_MS);
      producer.initTransactions();
      producer.beginTransaction();
      long offset = producer.send(topic1, "key1", "value1");

      consumer = new TxConsumer(connection, topic1, true);
      long position = seekOver(consumer, offset, 500, 8);
      should(
          position <= offset,
          "read committed seek can't jump beyond an inflight tx; tx offset:"
              + offset + ", position: " + position);

      producer.commitTransaction();
    } finally {
      if (producer != null) {
        producer.close();
      }
      if (consumer != null) {
        consumer.close();
      }
    }
  }

  static void readCommittedSeekDoesntRespectShortHangingTx(String connection)
      throws Exception {
    TxProducer producer = null;
    TxConsumer consumer = null;
    try {
      producer = new TxProducer(connection, txId1, 100);
      producer.initTransactions();
      producer.beginTransaction();
      long offset = producer.send(topic1, "key1", "value1");

      consumer = new TxConsumer(connection, topic1, true);
      long position = seekOver(consumer, offset, 500, 8);
      should(
          offset < position, "read committed seek should jump beyond an "
                                 + "(auto)aborted tx; tx offset: " + offset
                                 + ", position: " + position);

      try {
        producer.commitTransaction();
        fail("commit must fail because tx is already aborted");
      } catch (KafkaException e) {
      }
    } finally {
      if (producer != null) {
        producer.close();
      }
      if (consumer != null) {
        consumer.close();
      }
    }
  }

  static void readUncommittedSeekDoesntRespectOngoingTx(String connection)
      throws Exception {
    TxProducer producer = null;
    TxConsumer consumer = null;
    try {
      producer = new TxProducer(connection, txId1);
      producer.initTransactions();
      producer.beginTransaction();
      long offset = producer.send(topic1, "key1", "value1");

      consumer = new TxConsumer(connection, topic1, false);
      long position = seekOver(consumer, offset, 500, 8);
      should(
          offset < position,
          "read uncommitted seek should jump beyond an ongoing tx; tx offset: "
              + offset + ", position: " + position);

      producer.commitTransaction();
    } finally {
      if (producer != null) {
        producer.close();
      }
      if (consumer != null) {
        consumer.close();
      }
    }
  }

  static void setGroupStartOffsetPasses(String connection) throws Exception {
    TxStream stream = null;
    try {
      stream = new TxStream(connection);
      stream.initProducer(txId1);
      stream.initConsumer(topic1, groupId, true);
      stream.setGroupStartOffset(0);
    } finally {
      if (stream != null) {
        stream.close();
      }
    }
  }

  static long seekOver(TxConsumer consumer, long offset, int delay, int retries)
      throws Exception {
    consumer.seekToEnd();
    long position = consumer.position();
    while (position <= offset && retries > 0) {
      // partitions lag behind a coordinator we can't avoid sleep
      Thread.sleep(delay);
      consumer.seekToEnd();
      position = consumer.position();
      retries--;
    }
    return position;
  }

  static void readProcessWrite(String connection) throws Exception {
    SimpleProducer producer = null;
    TxStream stream = null;
    TxConsumer consumer = null;

    try {
      producer = new SimpleProducer(connection);
      long target_offset = producer.send(topic2, "noop-1", "noop");
      producer.close();
      producer = null;

      Map<Long, TxRecord> input = new HashMap<>();
      stream = new TxStream(connection);
      stream.initProducer(txId1);

      long first_offset = Long.MAX_VALUE;
      long last_offset = 0;
      for (int i = 0; i < 3; i++) {
        TxRecord record = new TxRecord();
        record.key = "key" + i;
        record.value = "value" + i;
        record.offset = stream.commitTx(topic1, record.key, record.value);
        first_offset = Math.min(first_offset, record.offset);
        last_offset = record.offset;
        input.put(record.offset, record);
      }

      stream.initConsumer(topic1, groupId, true);
      stream.setGroupStartOffset(first_offset);

      int retries = 8;
      long group_offset = stream.getGroupOffset();
      while (first_offset > group_offset && retries > 0) {
        // consumer groups lag behind a coordinator
        // we can't avoid sleep :(
        Thread.sleep(500);
        group_offset = stream.getGroupOffset();
        retries--;
      }
      should(
          first_offset == group_offset,
          "consumer should observe committed offset; committed offset: "
              + first_offset + ", group offset: " + group_offset);

      var mapping
          = stream.process(last_offset, x -> x.toUpperCase(), 1, topic2);
      stream.close();
      stream = null;

      consumer = new TxConsumer(connection, topic2, true);
      var transformed = consumer.readN(target_offset + 1, 3, 500, 10);

      for (var target : transformed) {
        assertTrue(mapping.containsKey(target.offset));
        long source_offset = mapping.get(target.offset);
        assertTrue(input.containsKey(source_offset));
        var source = input.get(source_offset);
        assertEquals(source.key, target.key);
        assertEquals(source.value.toUpperCase(), target.value);
      }
    } finally {
      if (stream != null) {
        stream.close();
      }
      if (producer != null) {
        producer.close();
      }
      if (consumer != null) {
        consumer.close();
      }
    }
  }

  static void assertLessOrEqual(long lesser, long sameOrGreater)
      throws Exception {
    if (lesser > sameOrGreater) {
      throw new Exception(
          "Expected " + lesser + " to be less than or equal to "
          + sameOrGreater);
    }
  }

  static void assertTrue(boolean x) throws Exception {
    if (!x) {
      throw new Exception("Expected true got false");
    }
  }

  static void assertLess(long lesser, long greater) throws Exception {
    if (lesser >= greater) {
      throw new Exception("Expected " + lesser + " to be less than " + greater);
    }
  }

  static void should(boolean cond, String msg) throws Exception {
    if (!cond) {
      throw new RetryableException(msg);
    }
  }

  static void assertEquals(long a, long b) throws Exception {
    if (a != b) {
      throw new Exception("Expected " + a + " to be equal to " + b);
    }
  }

  static void assertEquals(String a, String b) throws Exception {
    if (!a.equals(b)) {
      throw new Exception("Expected " + a + " to be equal to " + b);
    }
  }

  static void fail(String message) throws Exception {
    throw new Exception(message);
  }
}
