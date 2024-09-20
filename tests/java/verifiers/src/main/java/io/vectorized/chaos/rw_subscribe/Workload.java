package io.vectorized.chaos.rw_subscribe;
import java.io.*;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.Thread;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.Random;
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
    public final int rid;

    public final HashSet<Integer> lost_partitions = new HashSet<>();
    public final HashSet<Integer> added_partitions = new HashSet<>();

    public TrackingRebalanceListener(Workload workload, int rid) {
      this.workload = workload;
      this.rid = rid;
    }

    public void resetPartitions() {
      this.lost_partitions.clear();
      this.added_partitions.clear();
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      for (var tp : partitions) {
        try {
          workload.log(rid, "log\trevoke\t" + tp.partition());
        } catch (Exception e1) {
        }
        this.lost_partitions.add(tp.partition());
      }
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
      for (var tp : partitions) {
        try {
          workload.log(rid, "log\tlost\t" + tp.partition());
        } catch (Exception e1) {
        }
        this.lost_partitions.add(tp.partition());
      }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
      for (var tp : partitions) {
        try {
          workload.log(rid, "log\tassign\t" + tp.partition());
        } catch (Exception e1) {
        }
        this.added_partitions.add(tp.partition());
      }
    }
  }

  static enum OpProduceStatus { WRITING, UNKNOWN, WRITTEN, SEEN }

  static class OpProduceRecord {
    public long started_us;
    public long oid;
    public long offset;
    public int partition;
    public int pid;
    public OpProduceStatus status;

    public long prev_oid = -1;
    public long next_oid = -1;
  }

  static class Commit {
    public long offset;
    public long id;
    public long started;
    public long ended;
    public long overwritten;
  }

  static class PartitionCommits {
    public HashMap<Long, Integer> known_commits = new HashMap<>();
    public HashMap<Long, Commit> passed_commits = new HashMap<>();
    public HashMap<Long, Commit> inflight_commits = new HashMap<>();
    public HashMap<Long, Commit> overwritten_commits = new HashMap<>();

    void add_inflight(Commit commit) {
      if (!known_commits.containsKey(commit.offset)) {
        known_commits.put(commit.offset, 0);
      }
      known_commits.put(commit.offset, known_commits.get(commit.offset) + 1);
      inflight_commits.put(commit.id, commit);
    }

    void mark_passed(long id) {
      var commit = inflight_commits.get(id);
      inflight_commits.remove(id);
      passed_commits.put(id, commit);

      ArrayList<Long> overwritten = new ArrayList<>();
      for (var cid : passed_commits.keySet()) {
        if (cid == id) continue;
        var old = passed_commits.get(cid);
        if (old.ended < commit.started) {
          overwritten.add(cid);
        }
      }

      for (var cid : overwritten) {
        var old = passed_commits.get(cid);
        passed_commits.remove(cid);
        old.overwritten = commit.ended;
        overwritten_commits.put(cid, old);
        known_commits.put(old.offset, known_commits.get(old.offset) - 1);
        if (known_commits.get(old.offset) == 0) {
          known_commits.remove(old.offset);
        }
      }
    }
  }

  public volatile boolean is_active = false;
  public boolean is_paused = false;
  public int paused_rid = -1;

  private volatile App.InitBody args;
  private BufferedWriter opslog;

  private HashMap<Integer, App.OpsInfo> ops_info;
  private synchronized void succeeded(int thread_id) {
    ops_info.get(thread_id).succeeded_ops += 1;
  }
  private synchronized void failed(int thread_id) {
    ops_info.get(thread_id).failed_ops += 1;
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

  private long last_id = 0;
  private synchronized long get_id() { return ++this.last_id; }

  Random random;
  HashMap<Integer, Semaphore> produce_limiter;
  volatile ArrayList<Thread> producing_threads;
  volatile ArrayList<Thread> consuming_threads;

  private HashMap<Long, OpProduceRecord> producing_records;
  private HashMap<Integer, Long> first_offset;
  private HashMap<Integer, PartitionCommits> partition_commits;

  public Workload(App.InitBody args) { this.args = args; }

  public void start() throws Exception {
    is_active = true;
    past_us = 0;
    opslog = new BufferedWriter(
        new FileWriter(new File(new File(args.results_dir), "workload.log")));

    random = new Random();
    ops_info = new HashMap<>();
    produce_limiter = new HashMap<>();

    producing_records = new HashMap<>();
    first_offset = new HashMap<>();
    producing_threads = new ArrayList<>();
    partition_commits = new HashMap<>();

    for (var i = 0; i < args.partitions; i++) {
      partition_commits.put(i, new PartitionCommits());
    }

    int thread_id = 0;

    for (int i = 0; i < args.settings.writers; i++) {
      produce_limiter.put(i, new Semaphore(10));
      final var pid = thread_id++;
      ops_info.put(pid, new App.OpsInfo());
      producing_threads.add(new Thread(() -> {
        try {
          producingProcess(pid);
        } catch (Exception e) {
          synchronized (this) {
            System.out.println(e);
            e.printStackTrace();
          }
          System.exit(1);
        }
      }));
    }

    consuming_threads = new ArrayList<>();

    for (int i = 0; i < args.settings.readers; i++) {
      final var rid = thread_id++;
      ops_info.put(rid, new App.OpsInfo());
      consuming_threads.add(new Thread(() -> {
        try {
          consumingProcess(rid);
        } catch (Exception e) {
          synchronized (this) {
            System.out.println(e);
            e.printStackTrace();
          }
          System.exit(1);
        }
      }));
    }

    for (var th : producing_threads) {
      th.start();
    }

    for (var th : consuming_threads) {
      th.start();
    }
  }

  public void stop() throws Exception {
    is_active = false;
    synchronized (this) {
      is_paused = false;
      this.notifyAll();
    }

    Thread.sleep(1000);
    if (opslog != null) {
      opslog.flush();
    }

    System.out.println("waiting for consuming_threads");
    for (var th : consuming_threads) {
      th.join();
    }

    for (int i = 0; i < args.settings.writers; i++) {
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

  private void producingProcess(int pid) throws Exception {
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
    props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10000);
    // default value: 0
    props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
    // default value: 60000
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
    // default value: 1000
    props.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000);
    // default value: 50
    props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 50);
    // default value: 30000
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
    // default value: 100
    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
    // default value: 300000
    props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, 10000);
    // default value: 300000
    props.put(ProducerConfig.METADATA_MAX_IDLE_CONFIG, 10000);

    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
    props.put(ProducerConfig.RETRIES_CONFIG, 5);
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

    log(pid, "started\t" + args.hostname + "\tproducing");

    Producer<String, String> producer = null;

    while (is_active) {
      try {
        if (producer == null) {
          log(pid, "constructing");
          producer = new KafkaProducer<>(props);
          log(pid, "constructed");
          continue;
        }
      } catch (Exception e1) {
        var eid = get_error_id();
        log(pid, "err\t" + eid);
        failed(pid);
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
      op.offset = -1;
      op.pid = pid;
      op.oid = get_op_id();
      op.partition = random.nextInt(args.partitions);
      op.status = OpProduceStatus.WRITING;

      synchronized (this) {
        op.started_us = System.nanoTime() / 1000;
        producing_records.put(op.oid, op);
      }

      try {
        log(pid, "send\t" + op.partition + "\t" + op.oid);
        offset = producer
                     .send(new ProducerRecord<String, String>(
                         args.topic, op.partition, args.hostname, "" + op.oid))
                     .get()
                     .offset();
      } catch (Exception e1) {
        var eid = get_error_id();
        log(pid, "err\t" + eid);
        failed(pid);

        synchronized (this) {
          System.out.println("=== " + eid + " error on send pid:" + pid);
          System.out.println(e1);
          e1.printStackTrace();
        }

        synchronized (this) {
          if (op.status == OpProduceStatus.WRITING) {
            op.status = OpProduceStatus.UNKNOWN;
          } else if (op.status == OpProduceStatus.SEEN) {
            // ok
            // TODO: check offset increasing
          } else {
            violation(pid, "unexpected status: " + op.status.name());
          }
        }

        try {
          producer.close();
        } catch (Exception e3) {
        }
        producer = null;

        continue;
      }

      synchronized (this) {
        if (op.status == OpProduceStatus.WRITING) {
          op.status = OpProduceStatus.WRITTEN;
          op.offset = offset;
        } else if (op.status == OpProduceStatus.SEEN) {
          if (op.offset != offset) {
            violation(
                pid, "written " + op.oid + "@" + offset + "/" + op.partition
                         + " was already seen by offset:" + op.offset);
          }
        } else {
          violation(pid, "unexpected status: " + op.status.name());
        }
      }

      log(pid, "ok\t" + offset);

      produce_limiter.get(pid).acquire();
    }

    if (producer != null) {
      try {
        producer.close();
      } catch (Exception e) {
      }
    }
  }

  private void consumingProcess(int rid) throws Exception {
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

    log(rid, "started\t" + args.hostname + "\tconsuming");

    Consumer<String, String> consumer = null;
    TrackingRebalanceListener tracker = null;
    HashMap<Integer, Long> last_oid = new HashMap<>();
    HashMap<Integer, Long> last_offset = new HashMap<>();
    HashMap<Integer, Integer> processed_records = new HashMap<>();

    boolean should_reset = false;

    while (is_active) {
      if (should_reset) {
        should_reset = false;

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
      }

      try {
        if (consumer == null) {
          log(rid, "constructing");
          consumer = new KafkaConsumer<>(cprops);
          tracker = new TrackingRebalanceListener(this, rid);
          consumer.subscribe(Collections.singleton(args.topic), tracker);
          log(rid, "constructed");
          last_offset.clear();
          processed_records.clear();
          last_oid.clear();
        }
      } catch (Exception e1) {
        var eid = get_error_id();
        log(rid, "err\t" + eid);
        failed(rid);

        synchronized (this) {
          System.out.println("=== " + eid + " error on KafkaConsumer ctor");
          System.out.println(e1);
          e1.printStackTrace();
        }

        should_reset = true;

        continue;
      }

      log(rid, "poll");
      ConsumerRecords<String, String> records = null;

      var poll_ts = get_id();

      try {
        records = consumer.poll(Duration.ofMillis(10000));
      } catch (Exception e1) {
        var eid = get_error_id();
        log(rid, "err\t" + eid);
        failed(rid);

        synchronized (this) {
          System.out.println("=== " + eid + " error on poll");
          System.out.println(e1);
          e1.printStackTrace();
        }

        should_reset = true;

        continue;
      }
      log(rid, "ok");

      for (int partition : tracker.lost_partitions) {
        last_oid.remove(partition);
      }
      for (int partition : tracker.added_partitions) {
        last_oid.put(partition, -1L);
      }
      tracker.resetPartitions();

      var it = records.iterator();
      var has_processed = false;
      while (it.hasNext()) {
        synchronized (this) {
          if (is_paused && paused_rid == -1) {
            paused_rid = rid;

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

            paused_rid = -1;
          }
        }

        if (!has_processed) {
          has_processed = true;
        }

        var record = it.next();

        long oid = Long.parseLong(record.value());

        log(rid, "read\t" + record.partition() + "\t" + record.offset() + "\t"
                     + record.key() + "\t" + oid);

        if (!last_oid.containsKey(record.partition())) {
          violation(rid, "read isn't tracked partition: " + record.partition());
        }

        OpProduceRecord op = null;

        synchronized (this) {
          if (last_oid.get(record.partition()) == -1L) {
            // detected a jump
            var commits = partition_commits.get(record.partition());

            if (!commits.known_commits.containsKey(record.offset())) {
              // maybe the offset is from overwritten offsets?

              var match = false;
              for (var id : commits.overwritten_commits.keySet()) {
                var old_commit = commits.passed_commits.get(id);
                if (old_commit.overwritten < poll_ts) {
                  continue;
                }
                if (old_commit.offset == record.offset()) {
                  match = true;
                  break;
                }
              }

              if (!match) {
                // isn't a part of the committed offsets => must be first offset
                // but before we should check if any offset commit was already
                // known before poll started

                for (var id : commits.passed_commits.keySet()) {
                  var passed_commit = commits.passed_commits.get(id);
                  if (passed_commit.ended < poll_ts) {
                    violation(
                        rid, "jump to offset:" + record.offset()
                                 + " while at the time poll started offset:"
                                 + passed_commit.offset + " was already known");
                  }
                }

                if (!first_offset.containsKey(record.partition())) {
                  first_offset.put(record.partition(), record.offset());
                }
                if (record.offset() != first_offset.get(record.partition())) {
                  violation(
                      rid, "started reading from offset:" + record.offset()
                               + (" which wasn't committed and isn't the first "
                                  + "offset in the partition"));
                }
              }
            }
          }

          if (record.offset() < first_offset.get(record.partition())) {
            violation(
                rid, "detected offset:" + record.offset()
                         + " jumping before first offset:"
                         + first_offset.get(record.partition()));
          }

          if (!producing_records.containsKey(oid)) {
            violation(rid, "can't find cached oid:" + oid);
          }
          op = producing_records.get(oid);

          if (last_oid.get(record.partition()) != -1L) {
            var prev_oid = last_oid.get(record.partition());
            var prev_op = producing_records.get(prev_oid);

            if (prev_op.next_oid == -1) {
              prev_op.next_oid = oid;
            }
            if (prev_op.next_oid != oid) {
              violation(
                  rid, "observed " + prev_oid + "->" + oid + " while "
                           + prev_oid + "->" + prev_op.next_oid
                           + " was already known");
            }
            if (op.prev_oid == -1) {
              op.prev_oid = prev_oid;
            }
            if (op.prev_oid != prev_oid) {
              violation(
                  rid, "observed " + prev_oid + "->" + oid + " while "
                           + op.prev_oid + "->" + oid + " was already known");
            }
          }
          last_oid.put(record.partition(), oid);
        }

        if (!last_offset.containsKey(record.partition())) {
          last_offset.put(record.partition(), -1L);
          processed_records.put(record.partition(), 0);
        }

        last_offset.put(record.partition(), record.offset());
        processed_records.put(
            record.partition(), processed_records.get(record.partition()) + 1);

        if (processed_records.get(record.partition()) == 10) {
          processed_records.put(record.partition(), 0);
          var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
          Commit commit = new Commit();
          commit.offset = last_offset.get(record.partition()) + 1;
          commit.id = get_id();
          commit.started = get_id();
          commit.ended = -1;
          offsets.put(
              new TopicPartition(args.topic, record.partition()),
              new OffsetAndMetadata(commit.offset));

          try {
            log(rid, "commit\t" + record.partition() + "\t"
                         + (last_offset.get(record.partition()) + 1));
            synchronized (this) {
              var commits = partition_commits.get(record.partition());
              commits.add_inflight(commit);
            }
            consumer.commitSync(offsets);
            commit.ended = get_id();
            synchronized (this) {
              var commits = partition_commits.get(record.partition());
              commits.mark_passed(commit.id);
            }
            log(rid, "ok");
          } catch (Exception e1) {
            var eid = get_error_id();
            log(rid, "err\t" + eid);
            failed(rid);

            synchronized (this) {
              System.out.println("=== " + eid + " error on poll");
              System.out.println(e1);
              e1.printStackTrace();
            }

            should_reset = true;

            break;
          }
        }

        synchronized (this) {
          if (!produce_limiter.containsKey(op.pid)) {
            violation(rid, "can't find limiter for pid:" + op.pid);
          }
          produce_limiter.get(op.pid).release();

          long seen_us = System.nanoTime() / 1000;

          if (op.status == OpProduceStatus.WRITING) {
            op.status = OpProduceStatus.SEEN;
            op.offset = record.offset();
            succeeded(rid);
            log(rid, "seen\t" + oid + "\t" + (seen_us - op.started_us));
          } else if (op.status == OpProduceStatus.WRITTEN) {
            if (op.offset != record.offset()) {
              violation(
                  rid, "" + oid + "@" + record.offset()
                           + " has cached offset:" + op.offset);
            }
            op.status = OpProduceStatus.SEEN;
            succeeded(rid);
            log(rid, "seen\t" + oid + "\t" + (seen_us - op.started_us));
          } else if (op.status == OpProduceStatus.SEEN) {
            if (op.offset != record.offset()) {
              violation(
                  rid, "" + oid + "@" + record.offset()
                           + " has cached offset:" + op.offset);
            }
          } else if (op.status == OpProduceStatus.UNKNOWN) {
            op.status = OpProduceStatus.SEEN;
            op.offset = record.offset();
            succeeded(rid);
            log(rid, "seen\t" + oid + "\t" + (seen_us - op.started_us));
          } else {
            violation(rid, "unexpected status: " + op.status.name());
          }
        }
      }
    }

    if (consumer != null) {
      try {
        consumer.close();
      } catch (Exception e) {
      }
    }
  }
}