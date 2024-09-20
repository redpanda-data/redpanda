package io.vectorized.chaos.tx_money;
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
  public volatile boolean is_active = false;

  private volatile App.InitBody args;
  private BufferedWriter opslog;

  private HashMap<Integer, App.OpsInfo> ops_info;
  private synchronized void succeeded(int thread_id) {
    ops_info.get(thread_id).succeeded_ops += 1;
  }
  private synchronized void timedout(int thread_id) {
    ops_info.get(thread_id).timedout_ops += 1;
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
  public void event(String name) throws Exception { log(-1, "event\t" + name); }
  long eid_gen = 0;
  private synchronized long get_eid() { return eid_gen++; }

  private volatile ArrayList<Thread> threads;
  private volatile Random random;

  public Workload(App.InitBody args) {
    this.args = args;
    this.random = new Random();
  }

  public void start() throws Exception {
    is_active = true;
    past_us = 0;
    opslog = new BufferedWriter(
        new FileWriter(new File(new File(args.results_dir), "workload.log")));

    should_reset = new HashMap<>();
    last_success_us = new HashMap<>();
    ops_info = new HashMap<>();

    int thread_id = 0;
    threads = new ArrayList<>();
    for (int i = 0; i < this.args.settings.producers; i++) {
      final var j = thread_id++;
      should_reset.put(j, false);
      last_success_us.put(j, -1L);
      ops_info.put(j, new App.OpsInfo());
      threads.add(new Thread(() -> {
        try {
          transferProcess(j);
        } catch (Exception e) {
          var eid = get_eid();
          synchronized (this) {
            System.out.println("### err " + eid);
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

    for (var th : threads) {
      th.start();
    }
  }

  public void stop() throws Exception {
    is_active = false;

    Thread.sleep(1000);
    if (opslog != null) {
      opslog.flush();
    }

    for (var th : threads) {
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

  private void transferProcess(int wid) throws Exception {
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
    props.put(
        ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, args.settings.timeout_ms);
    props.put(
        ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, args.settings.timeout_ms);
    // default value: 0
    props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
    // default value: 60000
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, args.settings.timeout_ms);
    // default value: 1000
    props.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 1000);
    // default value: 50
    props.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 50);
    // default value: 30000
    props.put(
        ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, args.settings.timeout_ms);
    // default value: 100
    props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 100);
    // default value: 300000
    props.put(ProducerConfig.METADATA_MAX_AGE_CONFIG, args.settings.timeout_ms);
    // default value: 300000
    props.put(
        ProducerConfig.METADATA_MAX_IDLE_CONFIG, args.settings.timeout_ms);

    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
    props.put(ProducerConfig.RETRIES_CONFIG, args.settings.retries);
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-" + wid);

    Producer<String, String> producer = null;

    log(wid, "started\t" + args.hostname);

    while (is_active) {
      tick(wid);

      synchronized (this) {
        if (should_reset.get(wid)) {
          should_reset.put(wid, false);
          if (producer != null) {
            try {
              producer.close();
              producer = null;
            } catch (Exception e) {
            }
          }
        }
      }

      try {
        if (producer == null) {
          log(wid, "constructing");
          producer = new KafkaProducer<>(props);
          producer.initTransactions();
          log(wid, "constructed");
          continue;
        }
      } catch (Exception e1) {
        var eid = get_eid();
        log(wid, "err\t" + eid);

        synchronized (this) {
          System.out.println("### err " + eid);
          System.out.println(e1);
          e1.printStackTrace();
        }

        failed(wid);
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

      var acc1 = "acc" + random.nextInt(this.args.accounts);
      var acc2 = acc1;
      while (acc1.equals(acc2)) {
        acc2 = "acc" + random.nextInt(this.args.accounts);
      }

      log(wid, "tx");
      producer.beginTransaction();

      try {
        var f1 = producer.send(
            new ProducerRecord<String, String>(acc1, args.hostname, "1"));
        var f2 = producer.send(
            new ProducerRecord<String, String>(acc2, args.hostname, "-1"));
        f1.get();
        f2.get();
      } catch (Exception e1) {
        var eid = get_eid();
        synchronized (this) {
          System.out.println("### err " + eid);
          System.out.println("error on produce => aborting tx");
          System.out.println(e1);
          e1.printStackTrace();
        }

        try {
          log(wid, "brt\t" + eid);
          producer.abortTransaction();
          log(wid, "ok");
          failed(wid);
        } catch (Exception e2) {
          eid = get_eid();
          synchronized (this) {
            System.out.println("### err " + eid);
            System.out.println("error on abort => reset producer");
            System.out.println(e2);
            e2.printStackTrace();
          }

          log(wid, "err\t" + eid);
          try {
            producer.close();
          } catch (Exception e3) {
          }
          producer = null;
        }

        continue;
      }

      try {
        if (should_abort) {
          log(wid, "brt");
          producer.abortTransaction();
        } else {
          log(wid, "cmt");
          producer.commitTransaction();
        }
        log(wid, "ok");
        progress(wid);
        succeeded(wid);
      } catch (Exception e1) {
        var eid = get_eid();
        synchronized (this) {
          System.out.println("### err " + eid);
          System.out.println("error on commit/abort => reset producer");
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
      }
    }

    if (producer != null) {
      try {
        producer.close();
      } catch (Exception e) {
      }
    }
  }
}