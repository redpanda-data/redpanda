package io.vectorized.compaction;
import static spark.Spark.*;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.vectorized.compaction.idempotency.IdempotentWorkload;
import io.vectorized.compaction.tx.TxUniqueKeysWorkload;
import io.vectorized.compaction.tx.TxWorkload;
import java.util.ArrayList;
import spark.*;

public class App {
  public static class Metrics {
    public long total_writes = 0;
    public long total_reads = 0;
    public long min_writes = 0;
    public long min_reads = 0;

    public ArrayList<PartitionMetrics> partitions = new ArrayList<>();
  }

  public static class PartitionMetrics {
    public int partition;
    public long end_offset = -1;
    public long read_offset = -1;
    public long read_position = -1;
    public long written_offset = -1;
    public boolean consumed = false;
  }

  public static class JsonTransformer implements ResponseTransformer {
    private Gson gson = new Gson();

    @Override
    public String render(Object model) {
      return gson.toJson(model);
    }
  }

  GatedWorkload workload = null;

  void run() throws Exception {
    port(8080);

    // curl -vX GET http://127.0.0.1:8080/ping
    get("/ping", (req, res) -> {
      res.status(200);
      return "";
    });

    // curl -vX GET http://127.0.0.1:8080/info
    get("/info", "application/json", (req, res) -> {
      try {
        return workload.getMetrics();
      } catch (Exception e) {
        System.out.println(e);
        e.printStackTrace();
        throw e;
      }
    }, new JsonTransformer());

    // curl -vX POST http://127.0.0.1:8080/start-producer -H 'Content-Type:
    // application/json' -d '{"topic":"topic1","brokers":"127.0.0.1:9092",
    // "partitions": 1}'
    post("/start-producer", (req, res) -> {
      try {
        var param = new Gson().fromJson(req.body(), JsonObject.class);
        var type = param.get("workload").getAsString();

        if (type.equals("IDEMPOTENCY")) {
          workload = new IdempotentWorkload(req.body());
        } else if (type.equals("TX")) {
          workload = new TxWorkload(req.body());
        } else if (type.equals("TX_UNIQUE_KEYS")) {
          workload = new TxUniqueKeysWorkload(req.body());
        } else {
          throw new Exception("unknown workload: \"" + type + "\"");
        }

        workload.startProducer();
        res.status(200);
        return "";
      } catch (Exception e) {
        System.out.println(e);
        e.printStackTrace();
        throw e;
      }
    });

    // curl -vX POST http://127.0.0.1:8080/start-consumer
    post("/start-consumer", (req, res) -> {
      try {
        workload.startConsumer();
        res.status(200);
        return "";
      } catch (Exception e) {
        System.out.println(e);
        e.printStackTrace();
        throw e;
      }
    });

    // curl -vX POST http://127.0.0.1:8080/stop-producer
    post("/stop-producer", (req, res) -> {
      try {
        workload.stopProducer();
      } catch (Exception e) {
        System.out.println(e);
        e.printStackTrace();
        throw e;
      }
      res.status(200);
      return "";
    });

    // curl -vX GET http://127.0.0.1:8080/wait-producer
    get("/wait-producer", (req, res) -> {
      try {
        workload.waitProducer();
      } catch (Exception e) {
        System.out.println(e);
        e.printStackTrace();
        throw e;
      }
      res.status(200);
      return "";
    });

    // curl -vX GET http://127.0.0.1:8080/wait-consumer
    get("/wait-consumer", (req, res) -> {
      try {
        workload.waitConsumer();
      } catch (Exception e) {
        System.out.println(e);
        e.printStackTrace();
        throw e;
      }
      res.status(200);
      return "";
    });
  }

  public static void main(String[] args) throws Exception { new App().run(); }
}
