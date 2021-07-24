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
}
