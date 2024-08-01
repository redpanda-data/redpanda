package io.vectorized.idempotency;

import static spark.Spark.*;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.lang.String;
import java.lang.Thread;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Semaphore;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import spark.*;

// A simple pausable idempotent producer for sanity testing a Java based
// producer from ducktape. This is a not a verfier but rather a simple pausable
// load generating utility that can start, pause and resume a single idempotency
// session using a single producer on demand.
//
//  Supported REST APIs
//  - /start-producer - start a new or resume existing idempotent producer
//  session
//  - /pause-producer - pauses the producer
//  - /stop-producer  - stops the producer
public class App {

  static Logger logger = Logger.getLogger(App.class);

  static void setupLogging() {
    org.apache.log4j.BasicConfigurator.configure();
    Logger.getRootLogger().setLevel(Level.WARN);
    Logger.getLogger("io.vectorized").setLevel(Level.DEBUG);
    Logger.getLogger("org.apache.kafka").setLevel(Level.DEBUG);
  }

  public static class Params {
    public String brokers;
    public String topic;
    public long partitions;
  }

  public static class Progress {
    public long num_produced;
  };

  static Producer<String, String> createIdempotentProducer(String brokers) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
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
    return new KafkaProducer<String, String>(props);
  }

  public static class JsonTransformer implements ResponseTransformer {
    private Gson gson = new Gson();

    @Override
    public String render(Object model) {
      return gson.toJson(model);
    }
  }

  volatile Params params = null;
  volatile Thread produceThread = null;
  volatile Semaphore sem = new Semaphore(1, true);
  volatile boolean started = false;
  volatile boolean stopped = false;
  volatile Exception ex = null;
  volatile long counter = 0;

  void produceLoop() {
    var random = new Random();
    Producer<String, String> idempotentProducer
        = createIdempotentProducer(this.params.brokers);
    while (!stopped) {
      try {
        sem.acquire();
        long partition
            = random.longs(0, this.params.partitions).findFirst().getAsLong();
        String kv = Long.toString(counter);
        ProducerRecord<String, String> record
            = new ProducerRecord<>(this.params.topic, kv, kv);
        idempotentProducer.send(record).get();
      } catch (Exception e) {
        ex = e;
        logger.error("Exception in produce loop: ", e);
      } finally {
        sem.release();
      }
      counter++;
    }
    idempotentProducer.close();
  }

  void run() throws Exception {

    port(8080);

    get("/ping", (req, res) -> {
      res.status(200);
      return "";
    });

    get("/progress", (req, res) -> {
      Progress progress = new Progress();
      progress.num_produced = counter;
      return progress;
    }, new JsonTransformer());

    post("/start-producer", (req, res) -> {
      if (this.started && !this.stopped) {
        logger.info("Producer already started. unpausing it.");
        if (this.sem.availablePermits() == 0) {
          this.sem.release();
        }
        res.status(200);
        return "";
      }
      logger.info("Starting producer");
      try {
        this.params = (new Gson()).fromJson(req.body(), Params.class);
        this.produceThread = new Thread(() -> { this.produceLoop(); });
        this.produceThread.start();

      } catch (Exception e) {
        logger.error("Exception starting produce thread ", e);
        throw e;
      }
      this.started = true;
      this.stopped = false;
      res.status(200);
      return "";
    });

    post("/pause-producer", (req, res) -> {
      if (!this.started) {
        logger.info("Pause failed, not started.");
        res.status(500);
        return "";
      }
      logger.info("Pausing producer");
      this.sem.acquire();
      res.status(200);
      return "";
    });

    post("/stop-producer", (req, res) -> {
      logger.info("Stopping producer");
      this.stopped = true;
      if (this.sem.availablePermits() == 0) {
        this.sem.release();
      }
      try {
        if (produceThread != null) {
          produceThread.join();
        }
      } catch (Exception e) {
        logger.error("Exception stopping producer", e);
        throw e;
      }

      if (ex != null) {
        System.exit(1);
      }

      res.status(200);
      return "";
    });
  }

  public static void main(String[] args) throws Exception {
    setupLogging();
    new App().run();
  }
}
