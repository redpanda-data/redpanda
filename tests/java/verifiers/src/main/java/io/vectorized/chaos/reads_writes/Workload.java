package io.vectorized.chaos.reads_writes;
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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NotLeaderOrFollowerException;
import org.apache.kafka.common.errors.TimeoutException;

public class Workload {
  static class WriteInfo {
    public int thread_id;
    public long started_us;
    public long written_us;
    public long seen_us;
    public long op;
    public long curr_offset;
    public long last_offset;
    public boolean has_write_passed;
    public boolean has_write_failed;
    public boolean should_backpressure;
    public boolean is_expired;
    public HashSet<Integer> has_seen = new HashSet<>();
  }

  public volatile boolean is_active = false;

  private volatile App.InitBody args;
  private volatile ArrayList<Thread> write_threads;
  private volatile ArrayList<Thread> read_threads;
  private BufferedWriter opslog;
  private long past_us;
  private long before_us = -1;
  private long last_op = 0;

  private long last_success_us = -1;
  private HashMap<Integer, Boolean> should_reset;
  private HashMap<Integer, App.OpsInfo> ops_info;

  long last_known_offset;
  Queue<Long> read_offsets;
  HashMap<Integer, Long> read_fronts;
  HashMap<Long, Long> next_offset;
  HashMap<Long, WriteInfo> write_by_op;
  HashMap<Long, WriteInfo> write_by_offset;
  HashMap<Integer, Queue<Long>> known_writes;
  HashMap<Integer, Semaphore> semaphores;

  private synchronized void succeeded(int thread_id) {
    ops_info.get(thread_id).succeeded_ops += 1;
  }
  private synchronized void timedout(int thread_id) {
    ops_info.get(thread_id).timedout_ops += 1;
  }
  private synchronized void failed(int thread_id) {
    ops_info.get(thread_id).failed_ops += 1;
  }

  public synchronized HashMap<String, App.OpsInfo> get_ops_info() {
    HashMap<String, App.OpsInfo> result = new HashMap<>();
    for (Integer key : ops_info.keySet()) {
      result.put("" + key, ops_info.get(key).copy());
    }
    return result;
  }

  synchronized long get_op() { return ++this.last_op; }

  public Workload(App.InitBody args) { this.args = args; }

  private synchronized void update_last_success() {
    last_success_us = Math.max(last_success_us, System.nanoTime() / 1000);
  }

  private synchronized void tick() {
    var now_us = Math.max(last_success_us, System.nanoTime() / 1000);
    if (now_us - last_success_us > 10000 * 1000) {
      for (var thread_id : should_reset.keySet()) {
        should_reset.put(thread_id, true);
      }
      last_success_us = now_us;
    }
  }

  public void start() throws Exception {
    is_active = true;
    past_us = 0;
    opslog = new BufferedWriter(
        new FileWriter(new File(new File(args.results_dir), "workload.log")));

    last_known_offset = -1;
    write_by_op = new HashMap<>();
    write_by_offset = new HashMap<>();
    read_fronts = new HashMap<>();
    next_offset = new HashMap<>();
    read_offsets = new LinkedList<>();
    semaphores = new HashMap<>();
    known_writes = new HashMap<>();
    should_reset = new HashMap<>();
    ops_info = new HashMap<>();

    int thread_id = 0;
    write_threads = new ArrayList<>();
    for (int i = 0; i < this.args.settings.writes; i++) {
      final var j = thread_id++;
      should_reset.put(j, false);
      ops_info.put(j, new App.OpsInfo());
      write_threads.add(new Thread(() -> {
        try {
          writeProcess(j);
        } catch (Exception e) {
          System.out.println(e);
          e.printStackTrace();
        }
      }));
    }

    read_threads = new ArrayList<>();
    for (int i = 0; i < this.args.settings.reads; i++) {
      final var j = thread_id++;
      should_reset.put(j, false);
      read_fronts.put(j, -1L);
      ops_info.put(j, new App.OpsInfo());
      read_threads.add(new Thread(() -> {
        try {
          readProcess(j);
        } catch (Exception e) {
          System.out.println(e);
          e.printStackTrace();
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

    Thread.sleep(1000);
    if (opslog != null) {
      opslog.flush();
    }

    synchronized (this) {
      for (var thread_id : semaphores.keySet()) {
        try {
          semaphores.get(thread_id).release();
        } catch (Exception e) {
        }
      }
    }
    for (var th : write_threads) {
      th.join();
    }
    for (var th : read_threads) {
      th.join();
    }
    if (opslog != null) {
      opslog.flush();
      opslog.close();
    }
  }

  public void event(String name) throws Exception { log(-1, "event\t" + name); }

  private synchronized void log(int thread_id, String message)
      throws Exception {
    var now_us = System.nanoTime() / 1000;
    if (now_us < past_us) {
      throw new Exception(
          "Time cant go back, observed: " + now_us + " after: " + before_us);
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
          "Time cant go back, observed: " + now_us + " after: " + before_us);
    }
    opslog.write(
        "" + thread_id + "\t" + (now_us - past_us) + "\tviolation"
        + "\t" + message + "\n");
    opslog.flush();
    opslog.close();
    System.exit(1);
    past_us = now_us;
  }

  private void readProcess(int thread_id) throws Exception {
    var tp = new TopicPartition(args.topic, 0);
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

    log(thread_id, "started\t" + args.hostname + "\tconsumer");

    long prev_offset = -1;
    while (is_active) {
      tick();

      synchronized (this) {
        if (should_reset.get(thread_id)) {
          should_reset.put(thread_id, false);
          consumer = null;
        }
      }

      try {
        if (consumer == null) {
          log(thread_id, "constructing");
          consumer = new KafkaConsumer<>(props);
          consumer.assign(tps);
          if (prev_offset == -1) {
            consumer.seekToBeginning(tps);
          } else {
            consumer.seek(tp, prev_offset + 1);
          }
          log(thread_id, "constructed");
          continue;
        }
      } catch (Exception e) {
        log(thread_id, "err");
        System.out.println(e);
        e.printStackTrace();
        failed(thread_id);
        continue;
      }

      ConsumerRecords<String, String> records
          = consumer.poll(Duration.ofMillis(10000));
      var it = records.iterator();
      while (it.hasNext()) {
        var record = it.next();

        if (!record.key().equals(args.hostname)) {
          continue;
        }

        long offset = record.offset();
        long op = Long.parseLong(record.value());

        synchronized (this) {
          last_known_offset = Math.max(last_known_offset, offset);
          if (offset <= prev_offset) {
            violation(
                thread_id, "reads must be monotonic but observed " + offset
                               + " after " + prev_offset);
          }

          if (!write_by_op.containsKey(op)) {
            violation(
                thread_id,
                "read " + op + "@" + offset + ": duplication or truncation");
          }
          var write = write_by_op.get(op);

          if (next_offset.containsKey(prev_offset)) {
            if (next_offset.get(prev_offset) != offset) {
              violation(
                  thread_id, "read " + offset + " after " + prev_offset
                                 + " while abother thread read "
                                 + next_offset.get(prev_offset));
            }
          } else {
            next_offset.put(prev_offset, offset);
            read_offsets.add(offset);
          }
          prev_offset = offset;

          if (write.curr_offset == -1) {
            write.curr_offset = offset;
            write_by_offset.put(offset, write);
          }
          if (write.curr_offset != offset) {
            violation(
                thread_id, "read " + op + "@" + offset
                               + " conflicting with already observed " + op
                               + "@" + write.curr_offset);
          }
          if (write.curr_offset <= write.last_offset) {
            violation(
                thread_id, "read " + op + "@" + offset + " while "
                               + write.last_offset
                               + " was already known when the write started");
          }
          if (write.has_seen.isEmpty()) {
            write.seen_us = System.nanoTime() / 1000;
            if (!write.has_write_failed) {
              log(write.thread_id, "ok\t" + offset + "\t" + op);
              succeeded(write.thread_id);
            }
            if (write.written_us > 0) {
              log(write.thread_id,
                  "delta\t" + (write.seen_us - write.written_us));
            }
          }
          write.has_seen.add(thread_id);

          if (write.should_backpressure) {
            write.should_backpressure = false;
            semaphores.get(write.thread_id).release();
          }

          read_fronts.put(thread_id, offset);
          var min_front = offset;
          for (var rid : read_fronts.keySet()) {
            min_front = Math.min(min_front, read_fronts.get(rid));
          }

          if (min_front == offset) {
            var thread_known = known_writes.get(write.thread_id);

            while (thread_known.size() > 0 && thread_known.peek() <= offset) {
              var expiring_offset = thread_known.remove();
              if (!write_by_offset.containsKey(expiring_offset)) {
                violation(thread_id, "int. assert violation - 1");
              }
              var expiring_write = write_by_offset.get(expiring_offset);
              for (var read_thread_id : read_fronts.keySet()) {
                if (!expiring_write.has_seen.contains(read_thread_id)) {
                  violation(
                      read_thread_id, "read " + read_fronts.get(read_thread_id)
                                          + " but skipped "
                                          + expiring_offset);
                }
              }
              if (expiring_write.has_write_passed) {
                write_by_offset.remove(expiring_offset);
                write_by_op.remove(expiring_write.op);
              } else {
                expiring_write.is_expired = true;
              }
            }
          }

          while (read_offsets.size() > 0 && read_offsets.peek() < min_front) {
            var expired_offset = read_offsets.remove();
            next_offset.remove(expired_offset);
          }
        }
      }
    }
  }

  private void writeProcess(int thread_id) throws Exception {
    Semaphore semaphore = new Semaphore(0, true);
    synchronized (this) {
      semaphores.put(thread_id, semaphore);
      known_writes.put(thread_id, new LinkedList<>());
    }

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
    props.put(ProducerConfig.RETRIES_CONFIG, args.settings.retries);
    props.put(
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
        args.settings.enable_idempotency);

    Producer<String, String> producer = null;

    log(thread_id, "started\t" + args.hostname + "\tproducer");

    while (is_active) {
      tick();

      synchronized (this) {
        if (should_reset.get(thread_id)) {
          should_reset.put(thread_id, false);
          producer = null;
        }
      }

      long op = get_op();

      try {
        if (producer == null) {
          log(thread_id, "constructing");
          producer = new KafkaProducer<>(props);
          log(thread_id, "constructed");
          continue;
        }
      } catch (Exception e) {
        log(thread_id, "err");
        System.out.println(e);
        e.printStackTrace();
        failed(thread_id);
        continue;
      }

      try {
        log(thread_id, "msg\t" + op);
        synchronized (this) {
          var info = new WriteInfo();
          info.started_us = System.nanoTime() / 1000;
          info.written_us = -1;
          info.seen_us = -1;
          info.thread_id = thread_id;
          info.op = op;
          info.curr_offset = -1;
          info.last_offset = last_known_offset;
          info.has_write_passed = false;
          info.has_write_failed = false;
          info.should_backpressure = false;
          info.is_expired = false;

          write_by_op.put(op, info);
        }
        var f = producer.send(new ProducerRecord<String, String>(
            args.topic, args.hostname, "" + op));
        var m = f.get();
        var offset = m.offset();
        var should_acquire = false;
        synchronized (this) {
          known_writes.get(thread_id).add(offset);
          var write = write_by_op.get(op);

          if (write.curr_offset == -1) {
            write.curr_offset = offset;
            write_by_offset.put(offset, write);
          }
          if (write.curr_offset != offset) {
            violation(
                thread_id, "read " + op + "@" + offset
                               + " conflicting with already observed " + op
                               + "@" + write.curr_offset);
          }
          if (write.curr_offset <= write.last_offset) {
            violation(
                thread_id, "read " + op + "@" + offset + " while "
                               + write.last_offset
                               + " was already known when the write started");
          }
          write.written_us = System.nanoTime() / 1000;
          if (write.seen_us > 0) {
            log(thread_id, "delta\t" + (write.seen_us - write.written_us));
          }
          write.has_write_passed = true;

          if (write.is_expired) {
            write_by_offset.remove(offset);
            write_by_op.remove(op);
          }

          if (write.has_seen.size() != read_fronts.size()) {
            write.should_backpressure = true;
            should_acquire = true;
          }

          last_known_offset = Math.max(last_known_offset, write.curr_offset);
        }
        if (should_acquire) {
          semaphore.acquire();
        }
        update_last_success();
      } catch (ExecutionException e) {
        synchronized (this) {
          var write = write_by_op.get(op);
          write.has_write_failed = true;
          if (!write.has_seen.isEmpty()) {
            continue;
          }
        }
        var cause = e.getCause();
        if (cause != null) {
          if (cause instanceof TimeoutException) {
            log(thread_id, "time");
            timedout(thread_id);
            continue;
          } else if (cause instanceof NotLeaderOrFollowerException) {
            log(thread_id, "err");
            failed(thread_id);
            continue;
          } else if (cause instanceof KafkaException) {
            log(thread_id, "err");
            failed(thread_id);
            System.out.println(e);
            e.printStackTrace();
            if (producer != null) {
              try {
                producer.close();
                producer = null;
              } catch (Exception e2) {
              }
            }
            continue;
          }
        }

        log(thread_id, "err");
        System.out.println(e);
        failed(thread_id);
      } catch (Exception e) {
        synchronized (this) {
          var write = write_by_op.get(op);
          write.has_write_failed = true;
          if (!write.has_seen.isEmpty()) {
            continue;
          }
        }
        log(thread_id, "err");
        System.out.println(e);
        e.printStackTrace();
        failed(thread_id);
      }
    }
    producer.close();
  }
}
