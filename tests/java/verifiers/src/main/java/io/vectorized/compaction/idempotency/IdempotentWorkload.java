package io.vectorized.compaction.idempotency;
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
import java.util.concurrent.CountDownLatch;
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
import org.apache.kafka.common.TopicPartition;

public class IdempotentWorkload extends GatedWorkload {
  public static class InitBody {
    public String brokers;
    public String topic;
    public int partitions;
    public int key_set_cardinality;
  }

  static enum OpStatus { WRITING, UNKNOWN, WRITTEN, SEEN }

  static class WriteInfo {
    public long id;
    public String key;
    public int partition;

    public OpStatus status;
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
    public Queue<Long> writeLog;
    public Semaphore feedback;
    public HashMap<Long, WriteInfo> writes;       // hashed by WriteInfo.id
    public HashMap<String, LatestValue> latestKV; // hashed by key

    public boolean draining = false;
    public int inflights = 0;
    public long countWritten = 0;
    public long countRead = 0;
    public CountDownLatch drained = new CountDownLatch(1);

    public long writtenOffset = -1;
    public long endOffset = -1;
    public long readOffset = -1;
    public long readPosition = -1;
    public boolean consumed = false;

    public Partition(int id) {
      this.id = id;
      this.writeLog = new LinkedList<>();
      this.feedback = new Semaphore(200);
      this.writes = new HashMap<>();
      this.latestKV = new HashMap<>();
    }
  }

  HashMap<Integer, Partition> partitions;
  private volatile InitBody args;

  public IdempotentWorkload(String params) {
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

  @Override
  public void waitConsumer() throws Exception {
    if (this.state == State.FINAL) {
      return;
    } else if (this.state != State.CONSUMING) {
      violation(-1, "can't wait consumer before starting it");
    }

    for (var th : read_threads) {
      th.join();
    }

    this.state = State.FINAL;
  }

  private String getKey(Random random) {
    return "key" + random.nextInt(args.key_set_cardinality);
  }

  private void writeProcess(int pid) throws Exception {
    var random = new Random();

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

    var partition = this.partitions.get(pid);

    long id = 0;

    while (state == State.PRODUCING) {
      partition.feedback.acquire();

      var op = new WriteInfo();
      op.id = id++;
      op.key = getKey(random);
      op.partition = pid;
      op.status = OpStatus.WRITING;
      op.offset = -1;

      synchronized (this) {
        partition.inflights++;
        partition.writes.put(op.id, op);
        partition.writeLog.add(op.id);
        if (!partition.latestKV.containsKey(op.key)) {
          partition.latestKV.put(op.key, new LatestValue());
        }
        var latestValue = partition.latestKV.get(op.key);
        latestValue.attempts.add(op);
        log(pid, "w\t" + op.id);
      }

      var callback = new SendCallback(partition, op.id);
      ProducerRecord<String, String> record = new ProducerRecord<>(
          args.topic, pid, op.key,
          "" + op.id + "\t" + GatedWorkload.randomString(random, 1024));
      try {
        producer.send(record, callback);
      } catch (Exception e) {
        callback.onCompletion(null, e);
      }
    }

    synchronized (this) {
      partition.draining = true;
      if (partition.inflights == 0) {
        producer.close();
        return;
      }
    }

    partition.drained.await();
    producer.close();
  }

  @Override
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
    synchronized (this) { partition.endOffset = end; }
    consumer.seekToBeginning(tps);

    long lastOffset = -1;
    long lastOpId = -1;

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
        log(pid, "r\t" + opId + "\t" + offset);
        if (opId <= lastOpId) {
          violation(pid, "read opId " + opId + " after " + lastOpId);
        }
        lastOpId = opId;
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

  class SendCallback implements Callback {
    private Partition partition;
    private long opId;

    SendCallback(Partition partition, long opId) {
      this.partition = partition;
      this.opId = opId;
    }

    public void onCompletion(RecordMetadata meta, Exception e) {
      synchronized (IdempotentWorkload.this) {
        if (!partition.writes.containsKey(opId)) {
          violation(partition.id, "can't find pending write with id=" + opId);
          return;
        }

        var op = partition.writes.get(opId);

        if (e == null) {
          log(op.partition, "ok\t" + op.id + "\t" + meta.offset());
          switch (op.status) {
          case UNKNOWN:
          case WRITTEN:
            violation(
                op.partition,
                "just written record can't have status: " + op.status.name());
            break;
          case SEEN:
          case WRITING:
            op.status = OpStatus.WRITTEN;
            op.offset = meta.offset();
            break;
          default:
            violation(op.partition, "unexpected status: " + op.status);
          }

          partition.countWritten++;

          var clean = true;
          while (partition.writeLog.size() > 0) {
            var prevOpId = partition.writeLog.remove();
            if (prevOpId > opId) {
              violation(
                  op.partition,
                  "write log jumps over " + opId + " to " + prevOpId);
              return;
            }
            if (prevOpId == opId) {
              break;
            }
            if (!partition.writes.containsKey(prevOpId)) {
              violation(
                  partition.id, "can't find write with id=" + prevOpId
                                    + " prev to id=" + opId);
              return;
            }
            var prevOp = partition.writes.get(prevOpId);

            switch (prevOp.status) {
            case WRITING:
              // (n+1)th is confirmed and since idempotency depends on
              // sequence numbers it means that all previous attempts
              // are confirmed too
              prevOp.status = OpStatus.SEEN;
              // we can't clean because we need to wait until prevOp
              // is written to check the invariant
              // some next confirmed write will clean for us
              clean = false;
              break;
            case SEEN:
              clean = false;
              break;
            case WRITTEN:
              break;
            case UNKNOWN:
              break;
            default:
              violation(
                  partition.id,
                  "an op with id=" + prevOpId + " prev to id=" + opId
                      + " has unexpected status " + prevOp.status);
              return;
            }
            if (clean) {
              partition.writes.remove(prevOpId);
            }
          }
          if (clean) {
            partition.writes.remove(opId);
          }

          var latestKV = partition.latestKV.get(op.key);
          latestKV.confirmed = op;
          while (latestKV.attempts.size() > 0) {
            var attempt = latestKV.attempts.remove();
            if (attempt.id > opId) {
              violation(
                  op.partition,
                  "attempts log jumps over " + opId + " to " + attempt.id);
            }
            if (attempt.id == opId) {
              break;
            }
          }

          partition.writtenOffset
              = Math.max(partition.writtenOffset, meta.offset());
        } else {
          log(op.partition, "err\t" + op.id);
          System.out.println("=== Error on write");
          System.out.println(e);
          e.printStackTrace(System.out);

          switch (op.status) {
          case SEEN:
            // actually it may happen but we need to differentiate
            // reject vs unknown errors and fail only on reject
          case UNKNOWN:
          case WRITTEN:
            IdempotentWorkload.this.violation(
                op.partition,
                "impossible situation: failed write can't already have status "
                    + op.status);
            break;
          case WRITING:
            op.status = OpStatus.UNKNOWN;
            break;
          default:
            violation(op.partition, "unexpected status: " + op.status);
            return;
          }
        }
        partition.feedback.release();
        partition.inflights--;
        if (partition.draining && partition.inflights == 0) {
          partition.drained.countDown();
        }
      }
    }
  }
}
