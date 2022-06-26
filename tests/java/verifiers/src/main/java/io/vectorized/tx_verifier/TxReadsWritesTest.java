package io.vectorized.tx_verifier;

import java.io.*;
import java.lang.Thread;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidTxnStateException;

public class TxReadsWritesTest {
  private Properties tx_producer_properties;
  private Properties tx_consumer_properties;
  private String topic;

  private volatile String error;
  private volatile long last_known_offset;
  private volatile boolean has_errors = false;
  private volatile boolean has_finished = false;
  private volatile long finished_at_ms = 0;

  public TxReadsWritesTest(String connection, String topic) {
    this.topic = topic;

    this.tx_producer_properties = new Properties();
    this.tx_producer_properties.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connection);
    this.tx_producer_properties.put(ProducerConfig.ACKS_CONFIG, "all");
    this.tx_producer_properties.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    this.tx_producer_properties.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    this.tx_producer_properties.put(
        ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 10000);
    this.tx_producer_properties.put(
        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);
    this.tx_producer_properties.put(ProducerConfig.LINGER_MS_CONFIG, 0);
    this.tx_producer_properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
    this.tx_producer_properties.put(
        ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000);
    this.tx_producer_properties.put(
        ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 50);
    this.tx_producer_properties.put(
        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
    this.tx_producer_properties.put(
        ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
    this.tx_producer_properties.put(
        ProducerConfig.METADATA_MAX_AGE_CONFIG, 10000);
    this.tx_producer_properties.put(
        ProducerConfig.METADATA_MAX_IDLE_CONFIG, 10000);
    this.tx_producer_properties.put(ProducerConfig.RETRIES_CONFIG, 5);
    this.tx_producer_properties.put(
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    this.tx_producer_properties.put(
        ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-id-1");

    this.tx_consumer_properties = new Properties();
    this.tx_consumer_properties.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, connection);
    this.tx_consumer_properties.put(
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    this.tx_consumer_properties.put(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    this.tx_consumer_properties.put(
        ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    this.tx_consumer_properties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    this.tx_consumer_properties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    this.tx_consumer_properties.put(
        ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 10000);
    this.tx_consumer_properties.put(
        ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
    this.tx_consumer_properties.put(
        ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
    this.tx_consumer_properties.put(
        ConsumerConfig.METADATA_MAX_AGE_CONFIG, 10000);
    this.tx_consumer_properties.put(
        ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000);
    this.tx_consumer_properties.put(
        ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 50);
    this.tx_consumer_properties.put(
        ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
    this.tx_consumer_properties.put(
        ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
  }

  public void run() throws Exception {
    Producer<String, String> producer = null;
    long first_offset = -1;
    try {
      producer = new KafkaProducer<>(this.tx_producer_properties);
      producer.initTransactions();
      producer.beginTransaction();
      first_offset = producer
                         .send(new ProducerRecord<String, String>(
                             topic, "tx:0", "commit:record:0"))
                         .get()
                         .offset();
      producer.commitTransaction();
    } finally {
      if (producer != null) {
        try {
          producer.close();
        } catch (Exception e) {
        }
      }
    }

    last_known_offset = -1;

    final long offset = first_offset;

    var reader = new Thread(() -> {
      try {
        readProcess(offset);
      } catch (Exception e) {
        e.printStackTrace();
        error = e.getMessage();
        has_errors = true;
      }
    });
    reader.start();

    var writer = new Thread(() -> {
      try {
        writeProcess();
      } catch (Exception e) {
        e.printStackTrace();
        error = e.getMessage();
        has_errors = true;
      }
    });
    writer.start();

    writer.join();
    reader.join();

    if (has_errors) {
      throw new Exception(error);
    }
  }

  private void readProcess(long first_offset) throws Exception {
    var tp = new TopicPartition(topic, 0);
    var tps = Collections.singletonList(tp);

    KafkaConsumer<String, String> consumer = null;
    try {
      consumer = new KafkaConsumer<>(tx_consumer_properties);
      consumer.assign(tps);
      consumer.seek(tp, first_offset);

      var is_active = true;
      var last_observed_offset = -1L;

      while (is_active) {
        if (has_errors) {
          break;
        }

        if (has_finished) {
          if (last_observed_offset == last_known_offset) {
            break;
          }

          if (System.currentTimeMillis() - finished_at_ms > 10000) {
            error = "can't catchup with offset: " + last_known_offset
                    + " last observed offset: " + last_observed_offset;
            has_errors = true;
            break;
          }
        }

        ConsumerRecords<String, String> records
            = consumer.poll(Duration.ofMillis(10000));
        var it = records.iterator();
        while (it.hasNext()) {
          var record = it.next();

          last_observed_offset = record.offset();
          String key = record.key();
          String value = record.value();

          if (value.startsWith("commit")) {
            continue;
          }

          error = "observed a record of an aborted transaction: " + key + "="
                  + value + "@" + last_observed_offset;
          has_errors = true;
          break;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      error = e.getMessage();
      has_errors = true;
    } finally {
      if (consumer != null) {
        try {
          consumer.close();
        } catch (Exception e) {
        }
      }
    }
  }

  private void writeProcess() throws Exception {
    Producer<String, String> producer
        = new KafkaProducer<>(tx_producer_properties);
    producer.initTransactions();

    for (int j = 0; j < 4; j++) {
      try {
        long last_offset = -1;
        producer.beginTransaction();
        var pending = new ArrayList<Future<RecordMetadata>>();
        for (int i = 0; i < 10; i++) {
          pending.add(producer.send(new ProducerRecord<String, String>(
              topic, "tx:" + j, "abort:record:" + i)));
        }
        for (int i = 0; i < pending.size(); i++) {
          pending.get(i).get();
        }
        producer.abortTransaction();
      } catch (Exception e1) {
        try {
          producer.close();
        } catch (Exception e2) {
        }

        if (e1 instanceof InvalidTxnStateException
            || (e1.getCause() != null
                && e1.getCause() instanceof InvalidTxnStateException)) {
          producer = new KafkaProducer<>(tx_producer_properties);
          producer.initTransactions();
          continue;
        }

        e1.printStackTrace();
        error = e1.getMessage();
        has_errors = true;
        break;
      }
    }

    if (!has_errors) {
      try {
        producer.beginTransaction();
        last_known_offset = producer
                                .send(new ProducerRecord<String, String>(
                                    topic, "tx:" + 5, "commit:record:0"))
                                .get()
                                .offset();
        producer.commitTransaction();
      } catch (Exception e) {
        e.printStackTrace();
        error = e.getMessage();
        has_errors = true;
      }
    }

    try {
      producer.close();
    } catch (Exception e1) {
    }

    if (!has_errors) {
      finished_at_ms = System.currentTimeMillis();
      has_finished = true;
    }
  }
}
