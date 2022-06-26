package io.vectorized.reads_writes;
import java.io.*;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.Thread;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.TimeoutException;

public class Workload {
  static enum OpStatus { WRITING, UNKNOWN, WRITTEN, SEEN, SKIPPED }

  static class OpInfo {
    public long id;
    public long started_us;
    public OpStatus status;
    public int partition;
    public long offset;
  }

  static class WriterStat {
    public long countWritten = 0;
    public long countRead = 0;
    public long lastOffset = -1;
    public int inflights = 0;
    public boolean draining = false;
    public Semaphore drained = new Semaphore(0);
  }

  class SendCallback implements Callback {
    private OpInfo op;
    private WriterStat stat;

    SendCallback(OpInfo op, WriterStat stat) {
      this.op = op;
      this.stat = stat;
    }

    public void onCompletion(RecordMetadata meta, Exception e) {
      synchronized (Workload.this) {
        if (e == null) {
          log(op.partition, "d\t" + op.id + "\t" + meta.offset());
          switch (op.status) {
          case SKIPPED:
          case UNKNOWN:
          case WRITTEN:
            violation(
                op.partition,
                "just written record can't have status: " + op.status.name());
            break;
          case WRITING:
            op.status = OpStatus.WRITTEN;
            op.offset = meta.offset();
            break;
          case SEEN:
            if (op.offset != meta.offset()) {
              violation(
                  op.partition,
                  "oid:" + op.id + " was seen with conflicting offsets; seen: "
                      + op.offset + " written: " + meta.offset());
              break;
            }
            break;
          }

          stat.countWritten++;
          stat.lastOffset = Math.max(stat.lastOffset, op.offset);
        } else {
          log(op.partition, "e\t" + op.id);
          System.out.println(e);
          e.printStackTrace();
          switch (op.status) {
          case UNKNOWN:
            Workload.this.violation(
                op.partition,
                "impossible situation: failed write can't already have unknown status");
            break;
          case WRITING:
            op.status = OpStatus.UNKNOWN;
            break;
          default:
            break;
          }
        }
        stat.inflights--;
        if (stat.draining && stat.inflights == 0) {
          stat.drained.release();
        }
      }
    }
  }

  private synchronized void log(int partition, String msg) {
    long ts = System.currentTimeMillis() / 1000 - started_s;
    System.out.println(
        "" + (count++) + "\t" + ts + "\t" + partition + "\t" + msg);
  }

  private synchronized void violation(int partition, String msg) {
    long ts = System.currentTimeMillis() / 1000 - started_s;
    System.out.println(
        "" + (count++) + "\t" + ts + "\t" + partition + "\tviolation\t" + msg);
    System.exit(1);
  }

  public volatile boolean is_active = false;
  public volatile boolean are_writers_stopped = false;
  public volatile boolean are_readers_stopped = false;

  private volatile App.InitBody args;
  private volatile ArrayList<Thread> write_threads;
  private volatile ArrayList<Thread> read_threads;
  private long last_oid = 0;
  private long count = 0;
  private long started_s = -1;

  HashMap<Integer, Queue<Long>> oids;
  HashMap<Long, OpInfo> ops;
  HashMap<Integer, Semaphore> rsems;
  HashMap<Integer, WriterStat> wstat;

  int READ_WRITE_LOOP_SLACK = 1000;

  synchronized long get_oid() { return ++this.last_oid; }

  public Workload(App.InitBody args) { this.args = args; }

  public App.Metrics getMetrics() {
    var metrics = new App.Metrics();
    metrics.min_reads = Long.MAX_VALUE;
    metrics.total_reads = 0;
    metrics.min_writes = Long.MAX_VALUE;
    metrics.total_writes = 0;
    synchronized (this) {
      for (int partition = 0; partition < args.partitions; partition++) {
        var stat = wstat.get(partition);
        metrics.total_reads += stat.countRead;
        metrics.min_reads = Math.min(metrics.min_reads, stat.countRead);
        metrics.total_writes += stat.countWritten;
        metrics.min_writes = Math.min(metrics.min_writes, stat.countWritten);
      }
    }
    return metrics;
  }

  public void start() throws Exception {
    is_active = true;

    started_s = System.currentTimeMillis() / 1000;

    System.out.println(
        "#logical_time\tseconds_since_start\tpartition\tlog_type\tdetails");

    write_threads = new ArrayList<>();
    read_threads = new ArrayList<>();

    oids = new HashMap<>();
    ops = new HashMap<>();
    rsems = new HashMap<>();
    wstat = new HashMap<>();

    for (int partition = 0; partition < args.partitions; partition++) {
      oids.put(partition, new LinkedList<>());
      rsems.put(partition, new Semaphore(0));
      wstat.put(partition, new WriterStat());

      final int p = partition;
      write_threads.add(new Thread(() -> {
        try {
          writeProcess(p);
        } catch (Exception e) {
          synchronized (this) {
            System.out.println("=== Error on write");
            System.out.println(e);
            e.printStackTrace();
            System.exit(1);
          }
        }
      }));

      read_threads.add(new Thread(() -> {
        try {
          readProcess(p);
        } catch (Exception e) {
          synchronized (this) {
            System.out.println("=== Error on read");
            System.out.println(e);
            e.printStackTrace();
            System.exit(1);
          }
        }
      }));
    }

    for (var th : write_threads) {
      th.start();
    }

    for (var th : read_threads) {
      th.start();
    }
  }

  public void stop() throws Exception {
    is_active = false;

    synchronized (this) {
      for (int partition = 0; partition < args.partitions; partition++) {
        rsems.get(partition).release();
      }
    }
  }

  public void waitStopped() throws Exception {
    if (is_active) {
      throw new Exception("wasn't asked to stop");
    }
    if (are_readers_stopped) {
      return;
    }

    for (var th : write_threads) {
      th.join();
    }
    are_writers_stopped = true;
    for (var th : read_threads) {
      th.join();
    }

    are_readers_stopped = true;
  }

  private void writeProcess(int partition) throws Exception {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args.brokers);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
    props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

    Producer<String, String> producer = new KafkaProducer<>(props);

    Semaphore rsem;
    WriterStat stat = null;
    synchronized (this) {
      rsem = rsems.get(partition);
      stat = wstat.get(partition);
    }

    boolean is_feedback_started = false;

    while (is_active) {
      if (stat.countWritten >= READ_WRITE_LOOP_SLACK) {
        if (!is_feedback_started) {
          is_feedback_started = true;
          log(partition, "feedback");
        }
        rsem.acquire();
      }

      OpInfo op = new OpInfo();
      op.id = get_oid();
      op.started_us = System.nanoTime() / 1000;
      op.status = OpStatus.WRITING;
      op.partition = partition;
      op.offset = -1;
      synchronized (this) {
        stat.inflights++;
        ops.put(op.id, op);
        oids.get(partition).add(op.id);
        log(partition, "w\t" + op.id);
      }

      var callback = new SendCallback(op, stat);
      ProducerRecord<String, String> record
          = new ProducerRecord<>(args.topic, partition, "" + op.id, "" + op.id);
      try {
        producer.send(record, callback);
      } catch (Exception e) {
        callback.onCompletion(null, e);
      }
    }

    synchronized (this) {
      stat.draining = true;
      if (stat.inflights == 0) {
        return;
      }
    }

    stat.drained.acquire();
  }

  private void readProcess(int partition) throws Exception {
    var tp = new TopicPartition(args.topic, partition);
    var tps = Collections.singletonList(tp);

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args.brokers);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    props.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");

    Queue<Long> oids;
    Semaphore rsem;
    WriterStat stat;
    synchronized (this) {
      oids = this.oids.get(partition);
      rsem = rsems.get(partition);
      stat = wstat.get(partition);
    }

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.assign(tps);

    long offset = -1;

    while (true) {
      if (are_writers_stopped) {
        synchronized (this) {
          if (stat.lastOffset <= offset) {
            break;
          }
        }
      }

      log(partition, "r");
      ConsumerRecords<String, String> records
          = consumer.poll(Duration.ofMillis(10000));
      var it = records.iterator();
      while (it.hasNext()) {
        var record = it.next();

        offset = record.offset();
        long oid = Long.parseLong(record.value());

        synchronized (this) {
          log(partition, "o\t" + oid + "\t" + offset);
          if (!ops.containsKey(oid)) {
            violation(partition, "can't read what isn't written id:" + oid);
          }
          var op = ops.get(oid);
          switch (op.status) {
          case SEEN:
          case SKIPPED:
            violation(
                partition, "unexpected status: " + op.status.name() + " " + oid
                               + "@" + offset);
            break;
          case UNKNOWN:
          case WRITING:
            op.status = OpStatus.SEEN;
            op.offset = offset;
            break;
          case WRITTEN:
            if (op.offset != offset) {
              violation(
                  partition, "read " + oid + "@" + offset + " while expecting @"
                                 + op.offset);
            }
            break;
          }
          rsem.release();
          while (oids.size() > 0 && oids.element() < oid) {
            var skipped_id = oids.element();
            oids.remove();
            var skipped = ops.get(skipped_id);
            switch (skipped.status) {
            case SEEN:
            case SKIPPED:
            case WRITTEN:
              violation(
                  partition, "unexpected status for skipped record: "
                                 + skipped.status.name()
                                 + " oid:" + skipped.id);
              break;
            case UNKNOWN:
            case WRITING:
              op.status = OpStatus.SKIPPED;
              break;
            }
            ops.remove(skipped_id);
          }
          if (oids.size() == 0) {
            violation(partition, "len of pending reads can't be 0");
          }
          if (oids.element() != oid) {
            violation(
                partition,
                "unexpected element in pending reads oid:" + oids.element());
          }
          oids.remove();
          ops.remove(oid);
          stat.countRead++;
        }
      }
    }
  }
}
