package io.vectorized.compaction.tx;
import com.google.gson.Gson;
import io.vectorized.compaction.App;
import io.vectorized.compaction.GatedWorkload;
import java.lang.Thread;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;

public class TxUniqueKeysWorkload extends GatedWorkload {
  public static class InitBody {
    public String brokers;
    public String topic;
    public int partitions;
    public float abort_probability;
  }

  static class Partition {
    public int id;

    public long countWritten = 0;
    public long countRead = 0;

    public long writtenOffset = -1;
    public long endOffset = -1;
    public long readOffset = -1;
    public long readPosition = -1;
    public boolean consumed = false;

    public Partition(int id) { this.id = id; }
  }

  private static final int BATCH_SIZE = 10;

  HashMap<Integer, Partition> partitions;
  private volatile InitBody args;

  public TxUniqueKeysWorkload(String params) {
    Gson gson = new Gson();
    this.args = gson.fromJson(params, InitBody.class);
  }

  @Override
  public void startProducer() throws Exception {
    super.startProducer();

    System.out.println(
        "#logical_time\tseconds_since_start\tpartition\tlog_type\tdetails");

    log(-1, "start\tproducer");

    this.write_threads = new ArrayList<>();
    this.partitions = new HashMap<>();

    for (int partition = 0; partition < args.partitions; partition++) {
      this.partitions.put(partition, new Partition(partition));

      final int p = partition;
      this.write_threads.add(new Thread(() -> {
        try {
          writeProcess(p);
        } catch (Exception e) {
          synchronized (this) {
            System.out.println("=== write process error");
            System.out.println(e);
            e.printStackTrace(System.out);
            System.exit(1);
          }
        }
      }));
    }

    for (var th : write_threads) {
      th.start();
    }
  }

  @Override
  public void startConsumer() throws Exception {
    super.startConsumer();

    this.read_threads = new ArrayList<>();

    for (int partition = 0; partition < args.partitions; partition++) {
      final int p = partition;
      this.read_threads.add(new Thread(() -> {
        try {
          readProcess(p);
        } catch (Exception e) {
          synchronized (this) {
            System.out.println("=== read process error");
            System.out.println(e);
            e.printStackTrace(System.out);
            System.exit(1);
          }
        }
      }));
    }

    for (var th : read_threads) {
      th.start();
    }
  }

  private void writeProcess(int pid) throws Exception {
    var random = new Random();

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args.brokers);
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(
        ProducerConfig.TRANSACTIONAL_ID_CONFIG, UUID.randomUUID().toString());
    props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.LINGER_MS_CONFIG, 0);

    var partition = this.partitions.get(pid);
    Producer<String, String> producer = null;

    long id = 0;
    boolean shouldReset = true;
    boolean shouldRetry = false;
    long lastOffset = -1;

    while (state == State.PRODUCING) {
      if (shouldReset) {
        if (producer != null) {
          try {
            producer.close();
          } catch (Exception e1) {
          }
        }
        producer = null;
        shouldReset = false;
        Thread.sleep(1000);
      }

      if (producer == null) {
        producer = new KafkaProducer<>(props);
        try {
          log(pid, "init");
          producer.initTransactions();
        } catch (Exception e1) {
          synchronized (this) {
            log(pid, "err\tinit");
            System.out.println("=== Error on initTransactions");
            System.out.println(e1);
            e1.printStackTrace(System.out);
          }
          shouldReset = true;
          continue;
        }
      }

      Future<RecordMetadata> f = null;

      log(pid, "tx");
      producer.beginTransaction();

      boolean shouldAbort = false;

      if (!shouldRetry) {
        shouldAbort
            = random.nextInt(100) < Math.round(args.abort_probability * 100);
      }

      var txOpId = id;

      try {
        for (int i = 0; i < BATCH_SIZE; i++) {
          var opid = txOpId++;

          var type = "";
          if (shouldAbort) {
            type = "a";
          } else {
            type = "w";
          }
          log(pid, type + "\t" + opid);

          if (!shouldAbort && i == 0) {
            if (shouldRetry) {
              type = "<";
            } else {
              type = "!";
            }
          }

          String value
              = "" + opid + "\t" + type + "\t" + randomString(random, 1024);
          ProducerRecord<String, String> record
              = new ProducerRecord<>(args.topic, pid, "key" + opid, value);
          f = producer.send(record);
        }
      } catch (Exception e1) {
        shouldAbort = true;
        shouldRetry = true;

        synchronized (this) {
          log(pid, "err\tsend");
          System.out.println("=== Error on send");
          System.out.println(e1);
          e1.printStackTrace(System.out);
        }
      }

      try {
        if (shouldAbort) {
          log(pid, "abort");
          producer.flush();
          producer.abortTransaction();
        } else {
          log(pid, "commit");
          producer.commitTransaction();
          shouldRetry = false;
          id = txOpId;
          var offset = f.get().offset();
          if (lastOffset >= offset) {
            violation(
                pid, "offsets must me monotonic, written " + offset + " while "
                         + lastOffset + " was already known");
            return;
          }
          lastOffset = offset;
          synchronized (this) {
            partition.countWritten += BATCH_SIZE;
            partition.writtenOffset = lastOffset;
          }
        }
      } catch (Exception e1) {
        synchronized (this) {
          log(pid, "err");
          System.out.println("=== Error on ending tx");
          System.out.println(e1);
          e1.printStackTrace(System.out);
        }
        shouldReset = true;
        shouldRetry = true;
        continue;
      }
    }

    producer.close();
  }

  public App.Metrics getMetrics() {
    var metrics = new App.Metrics();
    metrics.min_reads = Long.MAX_VALUE;
    metrics.total_reads = 0;
    metrics.min_writes = Long.MAX_VALUE;
    metrics.total_writes = 0;
    synchronized (this) {
      for (int pid = 0; pid < args.partitions; pid++) {
        var partition = partitions.get(pid);

        metrics.total_reads += partition.countRead;
        metrics.min_reads = Math.min(metrics.min_reads, partition.countRead);
        metrics.total_writes += partition.countWritten;
        metrics.min_writes
            = Math.min(metrics.min_writes, partition.countWritten);

        var info = new App.PartitionMetrics();
        info.partition = partition.id;
        info.end_offset = partition.endOffset;
        info.read_offset = partition.readOffset;
        info.read_position = partition.readPosition;
        info.written_offset = partition.writtenOffset;
        info.consumed = partition.consumed;

        metrics.partitions.add(info);
      }
    }
    return metrics;
  }

  private void readProcess(int pid) throws Exception {
    var partition = partitions.get(pid);

    var tp = new TopicPartition(args.topic, pid);
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

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.assign(tps);
    consumer.seekToEnd(tps);
    long end = consumer.position(tp);
    synchronized (this) { partition.endOffset = end; }
    consumer.seekToBeginning(tps);

    long lastOffset = -1;
    long lastOpId = -1;
    long lastTxId = -1;

    while (consumer.position(tp) < end) {
      synchronized (this) { partition.readPosition = consumer.position(tp); }
      ConsumerRecords<String, String> records
          = consumer.poll(Duration.ofMillis(10000));
      var it = records.iterator();
      int read = 0;
      while (it.hasNext()) {
        read += 1;
        var record = it.next();

        var offset = record.offset();
        if (offset <= lastOffset) {
          violation(pid, "read offset " + offset + " after " + lastOffset);
        }
        lastOffset = offset;

        synchronized (this) { partition.readOffset = lastOffset; }

        var parts = record.value().split("\t");

        long opId = Long.parseLong(parts[0]);
        log(pid, "g\t" + parts[1] + "\t" + opId + "\t" + offset);
        if (parts[1].equals("a")) {
          violation(
              pid, "read aborted " + record.key() + "=" + opId + "@" + offset);
        } else if (parts[1].equals("!")) {
          if (opId != lastOpId + 1) {
            violation(
                pid, "detected gap before " + record.key() + "=" + opId + "@"
                         + offset + "; last op:" + lastOpId);
          }
          lastTxId = opId;
          lastOpId = opId;
        } else if (parts[1].equals("<")) {
          if (opId == lastOpId + 1) {
            // this a retried tx & the original tx wasn't committed
          } else if (opId != lastTxId) {
            violation(
                pid,
                "retied tx " + record.key() + "=" + opId + "@" + offset
                    + " should start where the original starts: " + lastTxId);
          }
          lastTxId = opId;
          lastOpId = opId;
        } else if (parts[1].equals("w")) {
          if (opId != lastOpId + 1) {
            violation(
                pid, "detected gap before " + record.key() + "=" + opId + "@"
                         + offset + "; last op:" + lastOpId);
          }
          lastOpId = opId;
        } else {
          violation(
              pid, "unknown type " + parts[1] + " at " + record.key() + "="
                       + opId + "@" + offset + "; last op:" + lastOpId);
        }
      }
      synchronized (this) { partition.countRead += read; }
    }
    synchronized (this) { partition.readPosition = consumer.position(tp); }
    consumer.close();

    synchronized (this) { partition.consumed = true; }

    log(pid, "partition is validated");
  }
}
