package io.vectorized.kafka;

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

  final static String txId1 = "tx1";
  final static String txId2 = "tx2";
  final static String topic1 = "topic1";
  final static String topic2 = "topic2";
  final static String groupId = "groupId";

  public static void main(final String[] args) throws Exception {
    retry("initPasses", Verifier::initPasses, args[0]);
    retry("txPasses", Verifier::txPasses, args[0]);
    retry("txesPasses", Verifier::txesPasses, args[0]);
    retry("abortPasses", Verifier::abortPasses, args[0]);
    retry("commutingTxesPass", Verifier::commutingTxesPass, args[0]);
    retry("conflictingTxFails", Verifier::conflictingTxFails, args[0]);
    retry("readCommittedSeekTest", Verifier::readCommittedSeekTest, args[0]);
    retry(
        "readUncommittedSeekTest", Verifier::readUncommittedSeekTest, args[0]);
    retry(
        "readCommittedTxSeekTest", Verifier::readCommittedTxSeekTest, args[0]);
    retry(
        "readUncommittedTxSeekTest", Verifier::readUncommittedTxSeekTest,
        args[0]);
    retry(
        "fetchReadsCommittedTxsTest", Verifier::fetchReadsCommittedTxsTest,
        args[0]);
    retry(
        "fetchDoesntReadAbortedTxsTest",
        Verifier::fetchDoesntReadAbortedTxsTest, args[0]);
    retry(
        "readCommittedSeekRespectsOngoingTx",
        Verifier::readCommittedSeekRespectsOngoingTx, args[0]);
    retry(
        "readCommittedSeekRespectsLongHangingTx",
        Verifier::readCommittedSeekRespectsLongHangingTx, args[0]);
    retry(
        "readCommittedSeekDoesntRespectShortHangingTx",
        Verifier::readCommittedSeekDoesntRespectShortHangingTx, args[0]);
    retry(
        "readUncommittedSeekDoesntRespectOngoingTx",
        Verifier::readUncommittedSeekDoesntRespectOngoingTx, args[0]);
    retry(
        "setGroupStartOffsetPasses", Verifier::setGroupStartOffsetPasses,
        args[0]);
    retry("readProcessWrite", Verifier::readProcessWrite, args[0]);
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
        return;
      } catch (TxConsumer.TimeoutException e) {
        if (retries > 0) {
          e.printStackTrace();
          Thread.sleep(Duration.ofSeconds(1).toMillis());
          continue;
        }
        throw e;
      } catch (KafkaException e) {
        if (retries > 0) {
          e.printStackTrace();
          Thread.sleep(Duration.ofSeconds(1).toMillis());
          continue;
        }
        throw e;
      } catch (ExecutionException e) {
        var cause = e.getCause();
        if (cause != null && cause instanceof KafkaException) {
          if (retries > 0) {
            e.printStackTrace();
            Thread.sleep(Duration.ofSeconds(1).toMillis());
            continue;
          }
        }
        throw e;
      }
    }
  }

  static void initPasses(String connection) throws Exception {
    TxProducer producer = null;
    try {
      producer = new TxProducer(connection, txId1);
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
      consumer.seekToEnd();
      int retries = 8;
      while (last_offset >= consumer.position() && retries > 0) {
        // partitions lag behind a coordinator
        // we can't avoid sleep :(
        Thread.sleep(500);
        consumer.seekToEnd();
        retries--;
      }
      assertLess(last_offset, consumer.position());

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
      consumer.seekToEnd();
      int retries = 8;
      while (last_offset >= consumer.position() && retries > 0) {
        // partitions lag behind a coordinator
        // we can't avoid sleep :(
        Thread.sleep(500);
        consumer.seekToEnd();
        retries--;
      }
      assertLess(last_offset, consumer.position());

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
      producer = new TxProducer(connection, txId1, Integer.MAX_VALUE);
      producer.initTransactions();
      producer.beginTransaction();
      long offset = producer.send(topic1, "key1", "value1");

      consumer = new TxConsumer(connection, topic1, true);
      int retries = 8;
      while (offset >= consumer.position() && retries > 0) {
        // partitions lag behind a coordinator
        // we can't avoid sleep :(
        Thread.sleep(500);
        consumer.seekToEnd();
        retries--;
      }
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
      int retries = 8;
      while (offset >= consumer.position() && retries > 0) {
        // partitions lag behind a coordinator
        // we can't avoid sleep :(
        Thread.sleep(500);
        consumer.seekToEnd();
        retries--;
      }
      assertLess(offset, consumer.position());

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

      int retries = 8;
      while (offset >= consumer.position() && retries > 0) {
        // partitions lag behind a coordinator
        // we can't avoid sleep :(
        Thread.sleep(500);
        consumer.seekToEnd();
        retries--;
      }
      assertLess(offset, consumer.position());

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
      while (first_offset > stream.getGroupOffset() && retries > 0) {
        // consumer groups lag behind a coordinator
        // we can't avoid sleep :(
        Thread.sleep(500);
        retries--;
      }
      assertEquals(first_offset, stream.getGroupOffset());

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
