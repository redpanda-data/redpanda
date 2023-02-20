package io.vectorized.compaction.tx;
import com.google.gson.Gson;
import io.vectorized.compaction.App;
import io.vectorized.compaction.GatedWorkload;
import java.lang.Thread;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

public class TxWorkload extends GatedWorkload {
  public static class InitBody {
    public String brokers;
    public String topic;
    public int partitions;
    public int key_set_cardinality;
  }

  static class WriteInfo {
    public long id;
    public String key;
    public int partition;
    public long offset;
  }

  static class LatestValue {
    public WriteInfo confirmed;
    public Queue<WriteInfo> attempts;

    public LatestValue() {
      this.confirmed = null;
      this.attempts = new LinkedList<>();
    }
  }

  static class Value {
    public long id;
    public long offset;

    public Value(long id, long offset) {
      this.id = id;
      this.offset = offset;
    }
  }

  static class Partition {
    public int id;
    public HashMap<Long, WriteInfo> writes;       // hashed by WriteInfo.id
    public HashMap<String, LatestValue> latestKV; // hashed by key

    public long countWritten = 0;
    public long countRead = 0;

    public long writtenOffset = -1;
    public long endOffset = -1;
    public long readOffset = -1;
    public long readPosition = -1;
    public boolean consumed = false;

    public Partition(int id) {
      this.id = id;
      this.writes = new HashMap<>();
      this.latestKV = new HashMap<>();
    }
  }

  private static final int BATCH_SIZE = 10;

  HashMap<Integer, Partition> partitions;
  private volatile InitBody args;

  public TxWorkload(String params) {
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

  private String getKey(Random random) {
    return "key" + random.nextInt(args.key_set_cardinality);
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

      producer.beginTransaction();

      boolean shouldAbort = random.nextInt(3) == 0;

      HashMap<String, WriteInfo> batch = new HashMap<>();

      try {
        for (int i = 0; i < BATCH_SIZE; i++) {
          var op = new WriteInfo();
          op.id = id++;
          op.key = getKey(random);
          op.partition = pid;
          op.offset = -1;

          batch.put(op.key, op);

          if (shouldAbort) {
            log(pid, "a\t" + op.id);
          } else {
            log(pid, "w\t" + op.id);
          }
          String value = "" + op.id + "\t" + (shouldAbort ? "a" : "c") + "\t"
                         + randomString(random, 1024);
          ProducerRecord<String, String> record
              = new ProducerRecord<>(args.topic, pid, op.key, value);
          op.offset = producer.send(record).get().offset();

          if (lastOffset >= op.offset) {
            violation(
                pid, "offsets must me monotonic, written " + op.offset
                         + " while " + lastOffset + " was already known");
            return;
          }
          lastOffset = op.offset;
        }
      } catch (Exception e1) {
        shouldAbort = true;
        synchronized (this) {
          log(pid, "err\tsend");
          System.out.println("=== Error on send");
          System.out.println(e1);
          e1.printStackTrace(System.out);
        }
      }

      if (!shouldAbort) {
        synchronized (this) {
          for (var op : batch.values()) {
            if (!partition.latestKV.containsKey(op.key)) {
              partition.latestKV.put(op.key, new LatestValue());
            }
            var latestValue = partition.latestKV.get(op.key);
            if (!shouldAbort) {
              latestValue.attempts.add(op);
            }
          }
        }
      }

      try {
        if (shouldAbort) {
          log(pid, "abort");
          producer.abortTransaction();
        } else {
          log(pid, "commit");
          producer.commitTransaction();
          synchronized (this) {
            partition.writtenOffset = lastOffset;
            partition.countWritten += BATCH_SIZE;
            for (var op : batch.values()) {
              if (!partition.latestKV.containsKey(op.key)) {
                partition.latestKV.put(op.key, new LatestValue());
              }
              var latestValue = partition.latestKV.get(op.key);
              latestValue.confirmed = op;
              while (latestValue.attempts.size() > 0) {
                var attempt = latestValue.attempts.remove();
                if (attempt.id > op.id) {
                  violation(
                      op.partition,
                      "attempts log jumps over " + op.id + " to " + attempt.id);
                }
                if (attempt.id == op.id) {
                  break;
                }
              }
            }
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
        info.consumed = partition.consumed;
        info.written_offset = partition.writtenOffset;

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

    HashMap<String, Value> kv = new HashMap<>();

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.assign(tps);
    consumer.seekToEnd(tps);
    long end = consumer.position(tp);
    long written = -1;
    synchronized (this) {
      partition.endOffset = end;
      written = partition.writtenOffset;
    }
    consumer.seekToBeginning(tps);

    long lastOffset = -1;
    long lastOpId = -1;

    while (consumer.position(tp) < end && consumer.position(tp) <= written) {
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
        log(pid, "r\t" + opId + "\t" + offset);
        if (opId <= lastOpId) {
          violation(pid, "read opId " + opId + " after " + lastOpId);
        }
        lastOpId = opId;
        if (!parts[1].equals("c")) {
          violation(
              pid, "read aborted " + record.key() + "=" + opId + "@" + offset);
        }
        kv.put(record.key(), new Value(opId, offset));
      }
      synchronized (this) { partition.countRead += read; }
    }
    synchronized (this) { partition.readPosition = consumer.position(tp); }
    consumer.close();

    var writtenKV = partition.latestKV;

    for (var key : kv.keySet()) {
      if (!writtenKV.containsKey(key)) {
        violation(pid, "read key " + key + " wasn't written");
      }
      var readValue = kv.get(key);
      var writtenValue = writtenKV.get(key);
      if (writtenValue.confirmed != null
          && writtenValue.confirmed.id == readValue.id) {
        if (writtenValue.confirmed.offset != readValue.offset) {
          violation(
              pid, "last observed offset=" + readValue.offset + " for key="
                       + key + " doesn't match last written offset="
                       + writtenValue.confirmed.offset
                       + " for the same value=" + readValue.id);
        }
      } else {
        WriteInfo match = null;
        for (var attempt : writtenValue.attempts) {
          if (attempt.id == readValue.id) {
            match = attempt;
          }
        }
        if (match == null) {
          violation(
              pid, "can't find an attempt matching read value" + readValue.id);
        }
      }
      writtenKV.remove(key);
    }

    for (var key : writtenKV.keySet()) {
      var writtenValue = writtenKV.get(key);
      if (writtenValue.confirmed != null) {
        violation(
            pid, "confirmed write " + key + "=" + writtenValue.confirmed.id
                     + "@" + writtenValue.confirmed.offset
                     + " can't be found in final read");
      }
    }

    synchronized (this) { partition.consumed = true; }

    log(pid, "partition is validated");
  }
}
