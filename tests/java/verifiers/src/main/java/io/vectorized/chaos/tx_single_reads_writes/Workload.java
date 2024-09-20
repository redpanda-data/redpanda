package io.vectorized.chaos.tx_single_reads_writes;
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
import java.util.Random;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

public class Workload {
  static enum TxStatus { ONGOING, COMMITTING, COMMITTED, ABORTING, UNKNOWN }

  static class TxRecord {
    public int wid;
    public long tid;
    public TxStatus status;
    public LinkedList<OpRecord> ops;
    public long started_us;
    public long seen_us;
  }

  static class OpRecord {
    public long tid;
    public long oid;
    public long offset;
    public HashSet<Integer> seen;
  }

  public volatile boolean is_active = false;

  public volatile boolean is_paused_before_send = false;
  public volatile boolean is_paused_before_abort = false;

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

  private long last_op_id = 0;
  private synchronized long get_op_id() { return ++this.last_op_id; }

  private long last_tx_id = 0;
  private synchronized long get_tx_id() { return ++this.last_tx_id; }

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
  private volatile ArrayList<Thread> consumers;

  private HashMap<Long, TxRecord> txes;
  private HashMap<Integer, LinkedList<Long>> tids;
  private long last_offset = -1;

  public Workload(App.InitBody args) { this.args = args; }

  public void start() throws Exception {
    is_active = true;
    past_us = 0;
    opslog = new BufferedWriter(
        new FileWriter(new File(new File(args.results_dir), "workload.log")));

    ops_info = new HashMap<>();
    txes = new HashMap<>();
    tids = new HashMap<>();

    int thread_id = 0;
    producers = new ArrayList<>();
    consumers = new ArrayList<>();
    for (int i = 0; i < this.args.settings.writes; i++) {
      final var j = thread_id++;
      tids.put(j, new LinkedList<>());
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

    for (int i = 0; i < this.args.settings.reads; i++) {
      final var j = thread_id++;
      consumers.add(new Thread(() -> {
        try {
          readProcess(j);
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

    for (var th : consumers) {
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

    for (var th : consumers) {
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

  private void writeProcess(int wid) throws Exception {
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

    if (args.settings.transaction_timeout_config != -1) {
      props.put(
          ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
          args.settings.transaction_timeout_config);
    }

    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
    props.put(ProducerConfig.RETRIES_CONFIG, args.settings.retries);
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-" + wid);

    Producer<String, String> producer = null;

    log(wid, "started\t" + args.hostname);

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

      boolean should_abort = random.nextInt(2) == 0;

      var tx = new TxRecord();
      tx.wid = wid;
      tx.tid = get_tx_id();
      tx.status = should_abort ? TxStatus.ABORTING : TxStatus.ONGOING;
      tx.ops = new LinkedList<>();
      tx.started_us = System.nanoTime() / 1000;
      tx.seen_us = -1;

      long known_offset = -1;

      synchronized (this) {
        tids.get(wid).add(tx.tid);
        txes.put(tx.tid, tx);
        known_offset = last_offset;
      }

      log(wid, "tx\t" + tx.tid);
      producer.beginTransaction();

      long last_oid = -1;
      boolean is_err = false;
      try {
        for (int i = 0; i < 10; i++) {
          last_oid = get_op_id();

          var op = new OpRecord();
          op.tid = tx.tid;
          op.oid = last_oid;
          op.offset = -1;
          op.seen = new HashSet<>();

          synchronized (this) {
            tx.ops.add(op);

            while (is_paused_before_send) {
              try {
                this.wait();
              } catch (Exception e) {
              }
            }
          }
          var offset
              = producer
                    .send(new ProducerRecord<String, String>(
                        args.topic, args.hostname, tx.tid + "\t" + last_oid))
                    .get()
                    .offset();
          log(wid, "log\twriten\t" + last_oid + "@" + offset);
          synchronized (this) {
            if (op.offset != -1) {
              violation(
                  wid, "tid:" + tx.tid + " wasn't committed so oid:" + op.oid
                           + " shouldn't be visible but already has offset:"
                           + op.offset);
            }
            op.offset = offset;
          }
        }
      } catch (Exception e1) {
        var eid = get_error_id();
        log(wid, "log\terr\t" + eid);
        synchronized (this) {
          System.out.println("=== " + eid + " error on produce => aborting tx");
          System.out.println(e1);
          e1.printStackTrace();

          if (!should_abort && tx.status != TxStatus.ONGOING) {
            violation(
                wid, "tx failed before commit but its status is: "
                         + tx.status.name());
          }
          tx.status = TxStatus.ABORTING;
        }

        should_abort = true;
        is_err = true;
      }

      if (should_abort) {
        try {
          synchronized (this) {
            while (is_paused_before_abort) {
              try {
                this.wait();
              } catch (Exception e) {
              }
            }
          }

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

        continue;
      }

      try {
        log(wid, "cmt");
        synchronized (this) {
          if (tx.status != TxStatus.ONGOING) {
            violation(
                wid, "an ongoing tx changed its status before committing: "
                         + tx.status.name());
          }
          tx.status = TxStatus.COMMITTING;
        }
        producer.commitTransaction();
        synchronized (this) {
          if (tx.status == TxStatus.COMMITTING) {
            tx.status = TxStatus.COMMITTED;
          }
          if (tx.status != TxStatus.COMMITTED) {
            violation(
                wid, "a committed tx can't have status: " + tx.status.name());
          }
          for (var op : tx.ops) {
            if (op.offset <= known_offset) {
              violation(
                  wid,
                  "written offset " + op.offset
                      + " is less that last known offset: " + known_offset);
            }
            if (last_offset < op.offset) {
              last_offset = op.offset;
            }
          }
        }
        log(wid, "ok");
        succeeded(wid);
      } catch (Exception e1) {
        var eid = get_error_id();
        synchronized (this) {
          System.out.println(
              "=== " + eid + " error on commit => reset producer");
          System.out.println(e1);
          e1.printStackTrace();
          if (tx.status == TxStatus.COMMITTED) {
            // ok it means it was already seen
          } else if (tx.status == TxStatus.COMMITTING) {
            tx.status = TxStatus.UNKNOWN;
          } else {
            violation(
                wid, "a tx with failed commit can't have status: "
                         + tx.status.name());
          }
        }
        log(wid, "err\t" + eid);
        failed(wid);
        try {
          producer.close();
        } catch (Exception e3) {
        }
        producer = null;
      }
    }

    if (producer != null) {
      try {
        producer.close();
      } catch (Exception e) {
      }
    }
  }

  private void readProcess(int rid) throws Exception {
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

    log(rid, "started\t" + args.hostname + "\tconsumer");

    long prev_offset = -1;

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

        if (!record.key().equals(args.hostname)) {
          continue;
        }

        long offset = record.offset();
        String[] parts = record.value().split("\t");
        long tid = Long.parseLong(parts[0]);
        long oid = Long.parseLong(parts[1]);

        log(rid, "log\tread\t" + tid + ":" + oid + "@" + offset);

        synchronized (this) {
          // monotonicity
          if (offset <= prev_offset) {
            violation(
                rid, "reads must be monotonic but observed " + offset
                         + " after " + prev_offset);
          }

          if (!txes.containsKey(tid)) {
            violation(rid, "can't find tid:" + tid);
          }

          var tx = txes.get(tid);

          var prev_tids = tids.get(tx.wid);
          boolean found_tid = false;
          for (var prev_tid : prev_tids) {
            if (prev_tid == tid) {
              found_tid = true;
              break;
            }
            if (prev_tid > tid) {
              violation(rid, "can't find tid in tids");
            }
            var prev_tx = txes.get(prev_tid);
            if (prev_tx.status == TxStatus.ONGOING) {
              violation(
                  rid, "prev tid:" + prev_tid + " can't have ongoing status");
            }
            if (prev_tx.status == TxStatus.COMMITTING) {
              violation(
                  rid, "prev tid:" + prev_tid
                           + (" can't have committing status (because next tid "
                              + "was already written by the same producer => "
                              + "should be unknown)"));
            }
            if (prev_tx.status == TxStatus.COMMITTED) {
              for (var op : prev_tx.ops) {
                if (!op.seen.contains(rid)) {
                  violation(
                      rid, "skipped over " + prev_tid + ":" + op.oid + "@"
                               + op.offset);
                }
              }
              continue;
            }
            if (prev_tx.status == TxStatus.UNKNOWN) {
              prev_tx.status = TxStatus.ABORTING;
            }
          }
          if (!found_tid) {
            violation(rid, "can't find tid:" + tid + " in the tids");
          }

          if (tx.status == TxStatus.ONGOING) {
            violation(
                rid,
                "can't read ongoing tx but read tid:" + tid + " before commit");
          }
          if (tx.status == TxStatus.ABORTING) {
            violation(rid, "can't read aborting tx but read tid:" + tid);
          }
          if (tx.status == TxStatus.COMMITTING) {
            tx.status = TxStatus.COMMITTED;
          }
          if (tx.status == TxStatus.UNKNOWN) {
            tx.status = TxStatus.COMMITTED;
          }

          boolean found_oid = false;
          for (var op : tx.ops) {
            if (op.oid < oid) {
              if (!op.seen.contains(rid)) {
                violation(
                    rid, "read " + tid + ":" + oid + "@" + offset
                             + " before oid: " + op.oid);
              }
            } else if (op.oid == oid) {
              if (op.seen.contains(rid)) {
                violation(rid, "already seen oid:" + oid);
              }
              op.seen.add(rid);
              if (op.offset != offset) {
                violation(
                    rid, "read offset " + tid + ":" + oid + "@" + offset
                             + " doesn't match written offset:" + op.offset);
              }
              if (tx.seen_us < 0) {
                tx.seen_us = System.nanoTime() / 1000;
                log(rid, "seen\t" + (tx.seen_us - tx.started_us));
              }
              found_oid = true;
              break;
            }
          }

          if (!found_oid) {
            violation(rid, "can't find oid:" + oid + " in tid:" + tid);
          }

          // gc
          if (tx.ops.getLast().oid == oid) {
            boolean has_processed = true;
            for (var op : tx.ops) {
              has_processed
                  = has_processed && (op.seen.size() == consumers.size());
            }
            if (has_processed) {
              while (prev_tids.size() > 0 && prev_tids.element() <= tid) {
                var prev_tid = prev_tids.remove();
                var prev_tx = txes.get(prev_tid);
                if (prev_tx.status == TxStatus.ONGOING) {
                  violation(
                      rid,
                      "prev tid:" + prev_tid + " can't have ongoing status");
                }
                if (prev_tx.status == TxStatus.COMMITTING) {
                  violation(
                      rid, "prev tid:" + prev_tid
                               + (" can't have committing status (because next "
                                  + "tid was already written by the same "
                                  + "producer => should be unknown)"));
                }
                if (prev_tx.status == TxStatus.UNKNOWN) {
                  violation(
                      rid,
                      "prev tid:" + prev_tid + " can't have unknown status");
                }
                if (prev_tx.status == TxStatus.COMMITTED) {
                  for (var op : prev_tx.ops) {
                    if (op.seen.size() != consumers.size()) {
                      var msg = "" + prev_tid + ":" + op.oid + "@" + op.offset
                                + " is seen only by:";
                      for (var cid : op.seen) {
                        msg += " " + cid;
                      }
                      violation(rid, msg);
                    }
                  }
                }
                txes.remove(prev_tid);
                log(rid, "log\trm\t" + prev_tid);
              }
            }
          }

          prev_offset = offset;
        }
      }
    }
  }
}