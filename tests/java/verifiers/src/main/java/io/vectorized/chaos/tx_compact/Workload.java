package io.vectorized.chaos.tx_compact;
import java.io.*;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.lang.Thread;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Random;
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

public class Workload {
  public volatile boolean is_active = false;

  private volatile App.InitBody args;
  private BufferedWriter opslog;
  private volatile Random random = new Random();

  private HashMap<Integer, App.OpsInfo> ops_info;
  private synchronized void succeeded(int thread_id) {
    ops_info.get(thread_id).succeeded_ops += 1;
  }
  private synchronized void failed(int thread_id) {
    ops_info.get(thread_id).failed_ops += 1;
  }

  private volatile long last_offset = -1;

  private long last_error_id = 0;
  private synchronized long get_error_id() { return ++this.last_error_id; }

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

  private volatile ArrayList<Thread> producers;
  private volatile ArrayList<Thread> edge_consumers;
  private volatile ArrayList<Thread> hist_consumers;

  private HashMap<Integer, Integer> last_committed_seq;
  private HashMap<Integer, Integer> committing_ltid;
  private static final String randAlpha = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
  private static final int alphaLen = randAlpha.length();

  public Workload(App.InitBody args) { this.args = args; }

  private String randomString(Random random, int length) {
    StringBuffer buf = new StringBuffer();
    buf.append('~');
    for (int i = 0; i < length - 1; i++) {
      buf.append(randAlpha.charAt(random.nextInt(alphaLen)));
    }
    return buf.toString();
  }

  public void start() throws Exception {
    is_active = true;
    past_us = 0;
    opslog = new BufferedWriter(
        new FileWriter(new File(new File(args.results_dir), "workload.log")));

    ops_info = new HashMap<>();

    int thread_id = 0;
    producers = new ArrayList<>();
    edge_consumers = new ArrayList<>();
    hist_consumers = new ArrayList<>();
    last_committed_seq = new HashMap<>();
    committing_ltid = new HashMap<>();
    for (int i = 0; i < this.args.settings.writers; i++) {
      final var j = thread_id++;
      ops_info.put(j, new App.OpsInfo());
      producers.add(new Thread(() -> {
        try {
          writeProcess(j);
        } catch (Exception e) {
          synchronized (this) {
            System.out.println("=== err on writeProcess");
            System.out.println(e);
            e.printStackTrace();
          }

          try {
            opslog.flush();
            opslog.close();
          } catch (Exception e2) {
          }
          System.exit(1);
        }
      }));
    }

    for (int i = 0; i < this.args.settings.edge_readers; i++) {
      final var j = thread_id++;
      edge_consumers.add(new Thread(() -> {
        try {
          readEdgeProcess(j);
        } catch (Exception e) {
          synchronized (this) {
            System.out.println("=== err on readProcess");
            System.out.println(e);
            e.printStackTrace();
          }
          try {
            opslog.flush();
            opslog.close();
          } catch (Exception e2) {
          }
          System.exit(1);
        }
      }));
    }

    for (int i = 0; i < this.args.settings.hist_readers; i++) {
      final var j = thread_id++;
      hist_consumers.add(new Thread(() -> {
        try {
          readHistProcess(j);
        } catch (Exception e) {
          synchronized (this) {
            System.out.println("=== err on readProcess");
            System.out.println(e);
            e.printStackTrace();
          }
          try {
            opslog.flush();
            opslog.close();
          } catch (Exception e2) {
          }
          System.exit(1);
        }
      }));
    }

    for (var th : producers) {
      th.start();
    }

    for (var th : edge_consumers) {
      th.start();
    }

    for (var th : hist_consumers) {
      th.start();
    }
  }

  public void stop() throws Exception {
    is_active = false;

    Thread.sleep(1000);
    if (opslog != null) {
      opslog.flush();
    }

    for (var th : producers) {
      th.join();
    }

    for (var th : edge_consumers) {
      th.join();
    }

    for (var th : hist_consumers) {
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

  private String key(int seq) { return "key" + (seq % 5); }

  private void writeProcess(int wid) throws Exception {
    Random random = new Random();

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
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-" + wid);

    Producer<String, String> producer = null;

    log(wid, "started\t" + args.hostname + "\twriter");

    var seq = 0;
    var ltid = 0;
    var retry = false;

    while (is_active) {
      try {
        if (producer == null) {
          log(wid, "constructing");
          producer = new KafkaProducer<>(props);
          producer.initTransactions();
          log(wid, "constructed");
          continue;
        }
      } catch (Exception e1) {
        var eid = get_error_id();
        synchronized (this) {
          System.out.println(
              "=== " + eid + " err on constructing KafkaProducer");
          System.out.println(e1);
          e1.printStackTrace();
        }

        log(wid, "err\t" + eid);
        try {
          if (producer != null) {
            producer.close();
          }
        } catch (Exception e2) {
        }
        producer = null;
        continue;
      }

      var should_abort = false;
      if (!retry) {
        ltid++;
        should_abort = random.nextInt(2) == 0;
      }

      var is_err = false;

      log(wid, "tx");
      producer.beginTransaction();

      Future<RecordMetadata> of = null;
      long offset = 0;

      try {
        for (int i = 0; i < 10; i++) {
          var n = seq + i;
          var key = this.key(n);
          var marker = retry ? "r" : "c";
          marker = (should_abort ? "a" : marker);
          log(wid, "log\tput\t" + key + "\t" + ltid + "\t" + n + "\t" + marker);
          of = producer.send(new ProducerRecord<String, String>(
              args.topic, key,
              "" + wid + "\t" + ltid + "\t" + n + "\t" + marker + "\t"
                  + randomString(random, 1024)));
        }
        offset = of.get().offset();
      } catch (Exception e1) {
        var eid = get_error_id();
        log(wid, "log\terr\t" + eid);
        synchronized (this) {
          System.out.println("=== " + eid + " error on produce => aborting tx");
          System.out.println(e1);
          e1.printStackTrace();
        }
        is_err = true;
      }

      if (should_abort || is_err) {
        try {
          log(wid, "brt");
          producer.abortTransaction();
          log(wid, "ok");
          if (is_err) {
            failed(wid);
          } else {
            succeeded(wid);
          }
        } catch (Exception e2) {
          var eid = get_error_id();
          synchronized (this) {
            System.out.println(
                "=== " + eid + " error on abort => reset producer");
            System.out.println(e2);
            e2.printStackTrace();
          }
          log(wid, "err\t" + eid);
          failed(wid);
          try {
            producer.close();
          } catch (Exception e3) {
          }
          producer = null;
        }

        if (!should_abort) {
          retry = true;
        }

        continue;
      }

      synchronized (this) { committing_ltid.put(wid, ltid); }

      try {
        log(wid, "cmt");
        producer.commitTransaction();
        log(wid, "ok\t" + offset);
        succeeded(wid);
      } catch (Exception e1) {
        var eid = get_error_id();
        synchronized (this) {
          System.out.println(
              "=== " + eid + " error on commit => reset producer");
          System.out.println(e1);
          e1.printStackTrace();
        }
        log(wid, "err\t" + eid);
        failed(wid);
        try {
          producer.close();
        } catch (Exception e3) {
        }
        producer = null;
        retry = true;
        continue;
      }

      synchronized (this) {
        if (offset > last_offset) {
          last_offset = offset;
        }
      }

      seq += 10;
      retry = false;

      synchronized (this) {
        committing_ltid.remove(wid);
        last_committed_seq.put(wid, seq - 1);
      }
    }

    if (producer != null) {
      try {
        producer.close();
      } catch (Exception e) {
      }
    }
  }

  static class WriterState {
    public int last_seen_seq;
    public int last_seen_ltid;
  }

  private void readHistProcess(int rid) throws Exception {
    log(rid, "started\t" + args.hostname + "\tconsumer\thist");

    while (is_active) {
      readProcess(rid, last_offset);
      Thread.sleep(1000);
    }
  }

  private void readEdgeProcess(int rid) throws Exception {
    log(rid, "started\t" + args.hostname + "\tconsumer\tedge");

    while (is_active) {
      readProcess(rid, Long.MAX_VALUE);
    }
  }

  private void readProcess(int rid, long boundary) throws Exception {
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

    long prev_offset = -1;
    HashMap<Integer, WriterState> writers = new HashMap<>();

    long last_success_us = System.nanoTime() / 1000;

    while (is_active) {
      var now_us = Math.max(last_success_us, System.nanoTime() / 1000);
      if (now_us - last_success_us > 10 * 1000 * 1000) {
        consumer = null;
        last_success_us = now_us;
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
              "=== " + eid + " err on constructing KafkaConsumer");
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
        last_success_us = System.nanoTime() / 1000;
        var record = it.next();

        long offset = record.offset();

        if (offset >= boundary) {
          return;
        }

        if (offset < prev_offset) {
          violation(rid, "TODO-0");
        }

        var key = record.key();
        String[] parts = record.value().split("\t");
        int wid = Integer.parseInt(parts[0]);
        int ltid = Integer.parseInt(parts[1]);
        int seq = Integer.parseInt(parts[2]);
        if (parts[3].equals("a")) {
          log(rid, "log\tread\tkey=" + key + " " + wid + ":" + ltid + ":" + seq
                       + ":" + parts[3] + "@" + record.offset());
          violation(rid, "read aborted record");
        }

        synchronized (this) {
          if (this.last_committed_seq.containsKey(wid)) {
            if (this.last_committed_seq.get(wid) < seq) {
              if (!this.committing_ltid.containsKey(wid)) {
                violation(rid, "TODO-4");
              }
              if (this.committing_ltid.get(wid) != ltid) {
                violation(rid, "TODO-5");
              }
            }
          }
        }

        if (boundary == Long.MAX_VALUE) {
          log(rid, "log\tread\tkey=" + key + " " + wid + ":" + ltid + ":" + seq
                       + ":" + parts[3] + "@" + record.offset());
        }

        WriterState state;
        if (writers.containsKey(wid)) {
          state = writers.get(wid);

          if (state.last_seen_seq + 1 != seq) {
            log(rid, "log\tgap");
          }

          if (state.last_seen_ltid > ltid) {
            violation(
                rid, "observed " + ltid + " while already saw "
                         + state.last_seen_ltid);
          }
          if (state.last_seen_ltid < ltid) {
            if (state.last_seen_seq >= seq) {
              violation(
                  rid, "observed " + ltid + "/" + seq + " while already saw "
                           + state.last_seen_ltid + "/" + state.last_seen_seq);
            }
          }
          if (state.last_seen_ltid == ltid) {
            if (state.last_seen_seq >= seq) {
              if (!parts[3].equals("r")) {
                violation(
                    rid, "observed " + ltid + "/" + seq + " while already saw "
                             + state.last_seen_ltid + "/" + state.last_seen_seq
                             + " without retry");
              }
            }
          }
        } else {
          state = new WriterState();
          writers.put(wid, state);
        }

        state.last_seen_seq = seq;
        state.last_seen_ltid = ltid;

        prev_offset = offset;
      }
    }
  }
}