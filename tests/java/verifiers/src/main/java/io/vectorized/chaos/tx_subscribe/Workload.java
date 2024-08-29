package io.vectorized.chaos.tx_subscribe;
import java.io.*;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.Thread;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

public class Workload {
  private static class TrackingRebalanceListener
      implements ConsumerRebalanceListener {
    public final Workload workload;
    public final int sid;

    public TrackingRebalanceListener(Workload workload, int sid) {
      this.workload = workload;
      this.sid = sid;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      for (var tp : partitions) {
        try {
          workload.log(sid, "log\trevoke\t" + tp.partition());
        } catch (Exception e1) {
        }
      }
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
      for (var tp : partitions) {
        try {
          workload.log(sid, "log\tlost\t" + tp.partition());
        } catch (Exception e1) {
        }
      }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
      for (var tp : partitions) {
        try {
          workload.log(sid, "log\tassign\t" + tp.partition());
        } catch (Exception e1) {
        }
      }
    }
  }

  static enum OpProduceStatus { WRITING, UNKNOWN, WRITTEN, SKIPPED, SEEN }

  static class OpProduceRecord {
    public long oid;
    public int partition;
    public OpProduceStatus status;
  }

  public volatile boolean is_active = false;
  public volatile boolean is_paused = false;

  private volatile App.InitBody args;
  private BufferedWriter opslog;

  private HashMap<Integer, App.OpsInfo> ops_info;
  private synchronized void succeeded(int thread_id) {
    ops_info.get(thread_id).succeeded_ops += 1;
  }
  private synchronized void failed(int thread_id) {
    ops_info.get(thread_id).failed_ops += 1;
  }

  private HashMap<Integer, Boolean> should_reset;
  private HashMap<Integer, Long> last_success_us;
  private synchronized void progress(int thread_id) {
    last_success_us.put(thread_id, System.nanoTime() / 1000);
  }
  private synchronized void tick(int thread_id) {
    var now_us
        = Math.max(last_success_us.get(thread_id), System.nanoTime() / 1000);
    if (now_us - last_success_us.get(thread_id) > 10 * 1000 * 1000) {
      should_reset.put(thread_id, true);
      last_success_us.put(thread_id, now_us);
    }
  }

  private long past_us;
  private synchronized void log(int thread_id, String message)
      throws Exception {
    var now_us = System.nanoTime() / 1000;
    if (now_us < past_us) {
      throw new Exception(
          "Time cant go back, observed: " + now_us + " after: " + past_us);
    }
    opslog.write(
        "" + thread_id + "\t" + (now_us - past_us) + "\t" + message + "\n");
    past_us = now_us;
  }
  private synchronized void violation(int thread_id, String message)
      throws Exception {
    var now_us = System.nanoTime() / 1000;
    if (now_us < past_us) {
      throw new Exception(
          "Time cant go back, observed: " + now_us + " after: " + past_us);
    }
    opslog.write(
        "" + thread_id + "\t" + (now_us - past_us) + "\tviolation"
        + "\t" + message + "\n");
    opslog.flush();
    opslog.close();
    System.exit(1);
    past_us = now_us;
  }
  public void event(String name) throws Exception { log(-1, "event\t" + name); }

  private long last_op_id = 0;
  private synchronized long get_op_id() { return ++this.last_op_id; }

  private long last_error_id = 0;
  private synchronized long get_error_id() { return ++this.last_error_id; }

  HashMap<Integer, Semaphore> produce_limiter;
  volatile ArrayList<Thread> producing_threads;
  volatile Thread streaming_thread = null;
  volatile Thread consuming_thread = null;

  private HashMap<Long, OpProduceRecord> producing_records;
  private HashMap<Integer, Queue<Long>> producing_oids;

  public Workload(App.InitBody args) { this.args = args; }

  public void start() throws Exception {
    is_active = true;
    past_us = 0;
    opslog = new BufferedWriter(
        new FileWriter(new File(new File(args.results_dir), "workload.log")));

    should_reset = new HashMap<>();
    last_success_us = new HashMap<>();
    ops_info = new HashMap<>();
    produce_limiter = new HashMap<>();

    producing_records = new HashMap<>();
    producing_oids = new HashMap<>();
    producing_threads = new ArrayList<>();
    int thread_id = 0;

    for (int i = 0; i < args.partitions; i++) {
      produce_limiter.put(i, new Semaphore(10));
      final int j = i;
      producing_oids.put(j, new LinkedList<>());
      final var pid = thread_id++;
      last_success_us.put(pid, -1L);
      producing_threads.add(new Thread(() -> {
        try {
          producingProcess(pid, j);
        } catch (Exception e) {
          synchronized (this) {
            System.out.println(e);
            e.printStackTrace();
          }
          System.exit(1);
        }
      }));
    }

    {
      final var sid = thread_id++;
      ops_info.put(sid, new App.OpsInfo());
      last_success_us.put(sid, -1L);
      streaming_thread = new Thread(() -> {
        try {
          streamingProcess(sid);
        } catch (Exception e) {
          synchronized (this) {
            System.out.println(e);
            e.printStackTrace();
          }
          System.exit(1);
        }
      });
    }

    {
      final var rid = thread_id++;
      last_success_us.put(rid, -1L);
      should_reset.put(rid, false);
      consuming_thread = new Thread(() -> {
        try {
          consumingProcess(rid);
        } catch (Exception e) {
          synchronized (this) {
            System.out.println(e);
            e.printStackTrace();
          }
          System.exit(1);
        }
      });
    }

    for (var th : producing_threads) {
      th.start();
    }

    streaming_thread.start();
    consuming_thread.start();
  }

  public void stop() throws Exception {
    is_active = false;
    is_paused = false;
    synchronized (this) { this.notifyAll(); }

    Thread.sleep(1000);
    if (opslog != null) {
      opslog.flush();
    }

    System.out.println("waiting for streaming_thread");
    if (streaming_thread != null) {
      streaming_thread.join();
    }

    System.out.println("waiting for consuming_thread");
    if (consuming_thread != null) {
      consuming_thread.join();
    }

    for (int i = 0; i < args.partitions; i++) {
      produce_limiter.get(i).release(100);
    }
    System.out.println("waiting for producing_threads");
    for (var th : producing_threads) {
      th.join();
    }

    if (opslog != null) {
      opslog.flush();
      opslog.close();
    }
  }

  public synchronized HashMap<String, App.OpsInfo> get_ops_info() {
    HashMap<String, App.OpsInfo> result = new HashMap<>();
    for (Integer key : ops_info.keySet()) {
      result.put("" + key, ops_info.get(key).copy());
    }
    return result;
  }

  private void producingProcess(int pid, int partition) throws Exception {
    log(pid, "started\t" + args.hostname + "\tproducing\t" + partition);

    Producer<String, String> producer = null;

    while (is_active) {
      try {
        if (producer == null) {
          log(pid, "constructing");
          producer = createProducer(UUID.randomUUID().toString());
          log(pid, "constructed");
          continue;
        }
      } catch (Exception e1) {
        var eid = get_error_id();
        log(pid, "err\t" + eid);
        synchronized (this) {
          System.out.println(
              "=== " + eid + " error on KafkaProducer ctor pid:" + pid);
          System.out.println(e1);
          e1.printStackTrace();
        }
        try {
          if (producer != null) {
            producer.close();
          }
        } catch (Exception e2) {
        }
        producer = null;
        continue;
      }

      long offset = -1;

      var op = new OpProduceRecord();
      op.oid = get_op_id();
      op.partition = partition;
      op.status = OpProduceStatus.WRITING;

      synchronized (this) {
        producing_records.put(op.oid, op);
        producing_oids.get(partition).add(op.oid);
      }

      try {
        log(pid, "send\t" + op.oid);
        producer.beginTransaction();
        offset = producer
                     .send(new ProducerRecord<String, String>(
                         args.source, partition, args.hostname, "" + op.oid))
                     .get()
                     .offset();
      } catch (Exception e1) {
        var eid1 = get_error_id();
        synchronized (this) {
          System.out.println("=== " + eid1 + " error on send pid:" + pid);
          System.out.println(e1);
          e1.printStackTrace();
        }

        boolean should_reset = false;

        try {
          log(pid, "brt\t" + eid1);
          producer.abortTransaction();
          log(pid, "ok");
        } catch (Exception e2) {
          should_reset = true;
          var eid2 = get_error_id();
          log(pid, "err\t" + eid2);
          synchronized (this) {
            System.out.println("=== " + eid2 + " error on abort");
            System.out.println(e2);
            e2.printStackTrace();
          }
        }

        synchronized (this) {
          if (op.status == OpProduceStatus.SEEN) {
            producing_records.remove(op.oid);
          }
          if (op.status == OpProduceStatus.SKIPPED) {
            producing_records.remove(op.oid);
          }
          op.status = OpProduceStatus.UNKNOWN;
        }

        if (should_reset) {
          try {
            producer.close();
          } catch (Exception e3) {
          }
          producer = null;
        }

        continue;
      }

      try {
        log(pid, "cmt");
        producer.commitTransaction();
      } catch (Exception e1) {
        var eid = get_error_id();
        log(pid, "err\t" + eid);
        synchronized (this) {
          System.out.println("=== " + eid + " error on send pid:" + pid);
          System.out.println(e1);
          e1.printStackTrace();
        }

        synchronized (this) {
          if (op.status == OpProduceStatus.SEEN) {
            producing_records.remove(op.oid);
          }
          if (op.status == OpProduceStatus.SKIPPED) {
            producing_records.remove(op.oid);
          }
          op.status = OpProduceStatus.UNKNOWN;
        }

        try {
          producer.close();
        } catch (Exception e3) {
        }
        producer = null;

        continue;
      }

      synchronized (this) {
        if (op.status == OpProduceStatus.SEEN) {
          producing_records.remove(op.oid);
        }
        if (op.status == OpProduceStatus.SKIPPED) {
          violation(pid, "written message can't be skipped oid:" + op.oid);
        }
        op.status = OpProduceStatus.WRITTEN;
      }

      log(pid, "ok\t" + offset);

      produce_limiter.get(partition).acquire();
    }

    if (producer != null) {
      try {
        producer.close();
      } catch (Exception e) {
      }
    }
  }

  private void streamingProcess(int sid) throws Exception {
    Properties cprops = new Properties();
    cprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args.brokers);
    cprops.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    cprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    cprops.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    cprops.put(ConsumerConfig.GROUP_ID_CONFIG, args.group_id);
    cprops.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 10000);
    cprops.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
    // default value: 540000
    cprops.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 60000);
    // default value: 60000
    cprops.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
    // default value: 500
    cprops.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
    // default value: 300000
    cprops.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 10000);
    // default value: 1000
    cprops.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000);
    // default value: 50
    cprops.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 50);
    // defaut value: 30000
    cprops.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
    // default value: 100
    cprops.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
    cprops.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");
    cprops.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringDeserializer");

    log(sid, "started\t" + args.hostname + "\tstreaming");

    Consumer<String, String> consumer = null;
    TrackingRebalanceListener tracker = null;

    Producer<String, String> producer = null;

    boolean should_reset = false;
    HashMap<String, HashMap<Integer, Long>> processed_oids = new HashMap<>();

    while (is_active) {
      if (should_reset) {
        should_reset = false;

        log(sid, "log\tresetting");

        if (consumer != null) {
          try {
            consumer.close();
          } catch (Exception e) {
          }
          consumer = null;
        }

        if (producer != null) {
          try {
            producer.close();
          } catch (Exception e2) {
          }
          producer = null;
        }

        if (tracker != null) {
          tracker = null;
        }
      }

      synchronized (this) {
        if (is_paused) {
          if (consumer != null) {
            try {
              consumer.close();
            } catch (Exception e) {
            }
            consumer = null;
          }

          if (tracker != null) {
            tracker = null;
          }

          while (is_paused) {
            try {
              this.wait();
            } catch (Exception e) {
            }
          }
        }
      }

      try {
        if (consumer == null) {
          log(sid, "constructing\tstreaming-consumer");
          consumer = new KafkaConsumer<>(cprops);
          tracker = new TrackingRebalanceListener(this, sid);
          consumer.subscribe(Collections.singleton(args.source), tracker);
          log(sid, "constructed");
        }
      } catch (Exception e1) {
        var eid = get_error_id();
        log(sid, "err\t" + eid);
        synchronized (this) {
          System.out.println("=== " + eid + " error on KafkaConsumer ctor");
          System.out.println(e1);
          e1.printStackTrace();
        }

        if (consumer != null) {
          try {
            consumer.close();
          } catch (Exception e2) {
          }
          consumer = null;
          tracker = null;
        }

        continue;
      }

      try {
        if (producer == null) {
          log(sid, "constructing\tstreaming-producer");
          producer = createProducer("tx-consume-" + args.idx);
          log(sid, "constructed");
        }
      } catch (Exception e1) {
        var eid = get_error_id();
        log(sid, "err\t" + eid);
        synchronized (this) {
          System.out.println("=== " + eid + " error on KafkaProducer ctor");
          System.out.println(e1);
          e1.printStackTrace();
        }

        if (producer != null) {
          try {
            producer.close();
          } catch (Exception e2) {
          }
          producer = null;
        }

        continue;
      }

      log(sid, "log\ttick");
      ConsumerRecords<String, String> records = null;
      try {
        records = consumer.poll(Duration.ofMillis(10000));
      } catch (Exception e1) {
        var eid = get_error_id();
        log(sid, "log\terr\t" + eid);
        synchronized (this) {
          System.out.println("=== " + eid + " error on poll");
          System.out.println(e1);
          e1.printStackTrace();
        }

        try {
          consumer.close();
        } catch (Exception e2) {
        }
        consumer = null;
        tracker = null;

        continue;
      }
      log(sid, "log\ttack");

      var it = records.iterator();
      while (it.hasNext()) {
        var record = it.next();

        log(sid, "read\t" + record.offset() + "\t" + record.key() + "\t"
                     + record.partition() + "\t" + record.value());
        long oid = Long.parseLong(record.value());

        try {
          log(sid, "tx");
          producer.beginTransaction();
          var f = producer.send(new ProducerRecord<String, String>(
              args.target, args.hostname,
              "" + record.key() + "\t" + record.partition() + "\t" + oid));
          var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
          offsets.put(
              new TopicPartition(args.source, record.partition()),
              new OffsetAndMetadata(record.offset() + 1));
          producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
          f.get();
        } catch (Exception e1) {
          var eid1 = get_error_id();
          synchronized (this) {
            System.out.println(
                "=== " + eid1 + " error on send or sendOffsetsToTransaction");
            System.out.println(e1);
            e1.printStackTrace();
          }

          try {
            log(sid, "brt\t" + eid1);
            producer.abortTransaction();
            log(sid, "ok");
            failed(sid);
          } catch (Exception e2) {
            var eid2 = get_error_id();
            log(sid, "err\t" + eid2);
            synchronized (this) {
              System.out.println("=== " + eid2 + " error on abort");
              System.out.println(e2);
              e2.printStackTrace();
            }
          }

          should_reset = true;

          break;
        }

        produce_limiter.get(record.partition()).release();

        try {
          log(sid, "cmt");
          producer.commitTransaction();
          log(sid, "ok");
          succeeded(sid);
        } catch (Exception e1) {
          var eid = get_error_id();
          log(sid, "err\t" + eid);
          synchronized (this) {
            System.out.println("=== " + eid + " error on commit");
            System.out.println(e1);
            e1.printStackTrace();
          }

          should_reset = true;

          failed(sid);

          break;
        }

        if (!processed_oids.containsKey(record.key())) {
          processed_oids.put(record.key(), new HashMap<>());
        }
        var partition_oids = processed_oids.get(record.key());
        if (!partition_oids.containsKey(record.partition())) {
          partition_oids.put(record.partition(), -1L);
        }
        var processed_oid = partition_oids.get(record.partition());
        if (processed_oid >= oid) {
          violation(
              sid, "read " + record.key() + "=" + oid + "@" + record.offset()
                       + " in " + record.partition()
                       + " after already observed " + processed_oid
                       + " from same workload in same partition");
        }
        partition_oids.put(record.partition(), oid);
      }
    }

    if (consumer != null) {
      try {
        consumer.close();
      } catch (Exception e) {
      }
    }

    if (producer != null) {
      try {
        producer.close();
      } catch (Exception e) {
      }
    }
  }

  private void consumingProcess(int rid) throws Exception {
    var tp = new TopicPartition(args.target, 0);
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
    // default value: 540000
    props.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 60000);
    // default value: 60000
    props.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
    // default value: 500
    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 500);
    // default value: 300000
    props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 10000);
    // default value: 1000
    props.put(ConsumerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000);
    // default value: 50
    props.put(ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG, 50);
    // defaut value: 30000
    props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
    // default value: 100
    props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, 100);

    KafkaConsumer<String, String> consumer = null;

    log(rid, "started\t" + args.hostname + "\tconsuming");

    long prev_offset = -1;
    HashMap<String, HashMap<Integer, Long>> processed_oids = new HashMap<>();

    while (is_active) {
      tick(rid);

      synchronized (this) {
        if (should_reset.get(rid)) {
          should_reset.put(rid, false);
          consumer = null;
        }
      }

      try {
        if (consumer == null) {
          log(rid, "constructing");
          consumer = new KafkaConsumer<>(props);
          consumer.assign(tps);
          if (prev_offset == -1) {
            consumer.seekToBeginning(tps);
          } else {
            consumer.seek(tp, prev_offset + 1);
          }
          log(rid, "constructed");
          continue;
        }
      } catch (Exception e) {
        var eid = get_error_id();
        log(rid, "err\t" + eid);
        synchronized (this) {
          System.out.println(
              "=== " + eid + " error on KafkaConsumer ctor + seek");
          System.out.println(e);
          e.printStackTrace();
        }
        failed(rid);
        continue;
      }

      ConsumerRecords<String, String> records
          = consumer.poll(Duration.ofMillis(10000));
      var it = records.iterator();
      while (it.hasNext()) {
        progress(rid);
        var record = it.next();

        long offset = record.offset();
        var parts = record.value().split("\t");
        String server = parts[0];
        int partition = Integer.parseInt(parts[1]);
        long oid = Long.parseLong(parts[2]);

        if (offset <= prev_offset) {
          violation(
              rid, "reads must be monotonic but observed " + offset + " after "
                       + prev_offset);
        }
        prev_offset = offset;

        log(rid, "seen\t" + record.offset() + "\t" + record.key() + "\t"
                     + server + "\t" + partition + "\t" + oid);

        if (!processed_oids.containsKey(server)) {
          processed_oids.put(server, new HashMap<>());
        }
        var partition_oids = processed_oids.get(server);
        if (!partition_oids.containsKey(partition)) {
          partition_oids.put(partition, -1L);
        }
        var processed_oid = partition_oids.get(partition);
        if (processed_oid >= oid) {
          violation(
              rid, "seen " + server + "=" + oid + " in " + partition
                       + " after already observed " + processed_oid
                       + " from same workload in same partition");
        }
        partition_oids.put(partition, oid);

        if (!server.equals(args.hostname)) {
          continue;
        }

        synchronized (this) {
          if (!producing_records.containsKey(oid)) {
            violation(rid, "read an unknown oid:" + oid);
          }
          var op = producing_records.get(oid);
          if (op.status == OpProduceStatus.SEEN) {
            violation(rid, "can't read an already seen oid:" + oid);
          }
          if (op.status == OpProduceStatus.SKIPPED) {
            violation(rid, "can't read an already skipped oid:" + oid);
          }
          var oids = producing_oids.get(partition);

          while (oids.size() > 0 && oids.element() < oid) {
            var prev_oid = oids.remove();
            var prev_op = producing_records.get(prev_oid);
            if (prev_op.status == OpProduceStatus.SEEN) {
              violation(rid, "skipped op is already seen:" + prev_oid);
            }
            if (prev_op.status == OpProduceStatus.SKIPPED) {
              violation(rid, "skipped op is already skipped:" + prev_oid);
            }
            if (prev_op.status == OpProduceStatus.WRITTEN) {
              violation(rid, "skipped op is already acked:" + prev_oid);
            }
            if (prev_op.status == OpProduceStatus.WRITING) {
              prev_op.status = OpProduceStatus.SKIPPED;
            }
            if (prev_op.status == OpProduceStatus.UNKNOWN) {
              producing_records.remove(prev_oid);
            }
          }

          if (oids.size() == 0) {
            violation(rid, "attempted writes should include seen oid:" + oid);
          }
          if (oids.element() != oid) {
            violation(rid, "attempted writes should include seen oid:" + oid);
          }

          oids.remove();

          if (op.status == OpProduceStatus.WRITING) {
            op.status = OpProduceStatus.SEEN;
          } else if (op.status == OpProduceStatus.WRITTEN) {
            producing_records.remove(oid);
          } else if (op.status == OpProduceStatus.UNKNOWN) {
            producing_records.remove(oid);
          } else {
            violation(rid, "unknown status: " + op.status.name());
          }
        }
      }
    }
  }

  private Producer<String, String> createProducer(String txId) {
    Producer<String, String> producer = null;

    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args.brokers);
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");
    props.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        "org.apache.kafka.common.serialization.StringSerializer");

    // default value: 600000
    props.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, 60000);
    // default value: 120000
    props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 16000);
    // default value: 0
    props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
    // default value: 60000
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 16000);
    // default value: 1000
    props.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000);
    // default value: 50
    props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 50);
    // default value: 30000
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 16000);
    // default value: 100
    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
    // default value: 300000
    props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 5000);
    // default value: 300000
    props.put(ProducerConfig.METADATA_MAX_IDLE_CONFIG, 5000);

    props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 16000);

    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
    props.put(ProducerConfig.RETRIES_CONFIG, 5);
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId);

    producer = new KafkaProducer<>(props);
    producer.initTransactions();
    return producer;
  }
}