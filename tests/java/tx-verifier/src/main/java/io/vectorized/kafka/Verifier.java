package io.vectorized.kafka;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ProducerFencedException;

class Verifier {
  final static String txId1 = "tx1";
  final static String txId2 = "tx2";
  final static String topic1 = "topic1";
  final static String topic2 = "topic2";
  final static String groupId = "groupId";

  public static void main(final String[] args) throws Exception {
    initPasses(args[0]);
    txPasses(args[0]);
    txesPasses(args[0]);
    abortPasses(args[0]);
    commutingTxesPass(args[0]);
    conflictingTxFails(args[0]);
    readCommittedSeekTest(args[0]);
    readUncommittedSeekTest(args[0]);
    readCommittedTxSeekTest(args[0]);
    readUncommittedTxSeekTest(args[0]);
    fetchReadsCommittedTxsTest(args[0]);
    fetchDoesntReadAbortedTxsTest(args[0]);
    readCommittedSeekRespectsOngoingTx(args[0]);
    readCommittedSeekRespectsLongHangingTx(args[0]);
    readCommittedSeekDoesntRespectShortHangingTx(args[0]);
    readUncommittedSeekDoesntRespectOngoingTx(args[0]);
  }

  static void initPasses(String connection) throws Exception {
    var producer = new TxProducer(connection, txId1);
    producer.initTransactions();
    producer.close();
  }

  static void txPasses(String connection) throws Exception {
    var producer = new TxProducer(connection, txId1);
    producer.initTransactions();
    producer.commitTx(topic1, "key1", "value1");
    producer.close();
  }

  static void txesPasses(String connection) throws Exception {
    var producer = new TxProducer(connection, txId1);
    producer.initTransactions();
    producer.commitTx(topic1, "key1", "value1");
    producer.commitTx(topic1, "key2", "value2");
    producer.close();
  }

  static void abortPasses(String connection) throws Exception {
    var producer = new TxProducer(connection, txId1);
    producer.initTransactions();
    producer.abortTx(topic1, "key1", "value1");
    producer.close();
  }

  static void commutingTxesPass(String connection) throws Exception {
    var p1 = new TxProducer(connection, txId1);
    var p2 = new TxProducer(connection, txId2);
    p1.initTransactions();
    p1.beginTransaction();
    p1.send(topic1, "key1", "p1:value1");
    p2.initTransactions();
    p2.beginTransaction();
    p2.send(topic1, "key1", "p2:value1");
    p1.commitTransaction();
    p2.commitTransaction();
  }

  static void conflictingTxFails(String connection) throws Exception {
    var p1 = new TxProducer(connection, txId1);
    var p2 = new TxProducer(connection, txId1);
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
    p2.close();
    p1.close();
  }

  static void seekTest(String connection, boolean isReadComitted)
      throws Exception {
    var producer = new SimpleProducer(connection);
    long offset = producer.send(topic1, "key1", "value1");
    producer.close();

    var consumer = new TxConsumer(connection, topic1, isReadComitted);
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

    consumer.close();
  }

  static void readCommittedSeekTest(String connection) throws Exception {
    seekTest(connection, true);
  }

  static void readUncommittedSeekTest(String connection) throws Exception {
    seekTest(connection, false);
  }

  static void txSeekTest(String connection, boolean isReadComitted)
      throws Exception {
    var producer = new TxProducer(connection, txId1);
    producer.initTransactions();
    long offset = producer.commitTx(topic1, "key1", "value1");
    producer.close();

    var consumer = new TxConsumer(connection, topic1, isReadComitted);
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

    consumer.close();
  }

  static void readCommittedTxSeekTest(String connection) throws Exception {
    txSeekTest(connection, true);
  }

  static void readUncommittedTxSeekTest(String connection) throws Exception {
    txSeekTest(connection, false);
  }

  static void fetchReadsCommittedTxsTest(String connection) throws Exception {
    Map<String, Long> offsets = new HashMap<>();
    var producer = new TxProducer(connection, txId1);
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

    var consumer = new TxConsumer(connection, topic1, true);
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

    var records = consumer.read(first_offset, last_offset, 1);
    consumer.close();
    assertEquals(records.size(), offsets.size());
    for (var record : records) {
      assertTrue(offsets.containsKey(record.key));
      assertEquals(offsets.get(record.key), record.offset);
    }
  }

  static void fetchDoesntReadAbortedTxsTest(String connection)
      throws Exception {
    var producer = new TxProducer(connection, txId1);
    producer.initTransactions();
    long first_offset = producer.commitTx(topic1, "key1", "value1");
    producer.abortTx(topic1, "key2", "value2");
    long last_offset = producer.commitTx(topic1, "key3", "value3");
    producer.close();

    var consumer = new TxConsumer(connection, topic1, true);
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

    var records = consumer.read(first_offset, last_offset, 1);
    consumer.close();
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
  }

  static void readCommittedSeekRespectsOngoingTx(String connection)
      throws Exception {
    var producer = new TxProducer(connection, txId1);
    producer.initTransactions();
    producer.beginTransaction();
    long offset = producer.send(topic1, "key1", "value1");

    var consumer = new TxConsumer(connection, topic1, true);
    consumer.seekToEnd();
    assertLessOrEqual(consumer.position(), offset);

    producer.commitTransaction();
    producer.close();
    consumer.close();
  }

  static void readCommittedSeekRespectsLongHangingTx(String connection)
      throws Exception {
    var producer = new TxProducer(connection, txId1, Integer.MAX_VALUE);
    producer.initTransactions();
    producer.beginTransaction();
    long offset = producer.send(topic1, "key1", "value1");

    var consumer = new TxConsumer(connection, topic1, true);
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
    producer.close();
    consumer.close();
  }

  static void readCommittedSeekDoesntRespectShortHangingTx(String connection)
      throws Exception {
    var producer = new TxProducer(connection, txId1, 100);
    producer.initTransactions();
    producer.beginTransaction();
    long offset = producer.send(topic1, "key1", "value1");

    var consumer = new TxConsumer(connection, topic1, true);
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

    producer.close();
    consumer.close();
  }

  static void readUncommittedSeekDoesntRespectOngoingTx(String connection)
      throws Exception {
    var producer = new TxProducer(connection, txId1);
    producer.initTransactions();
    producer.beginTransaction();
    long offset = producer.send(topic1, "key1", "value1");

    var consumer = new TxConsumer(connection, topic1, false);

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
    producer.close();
    consumer.close();
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
