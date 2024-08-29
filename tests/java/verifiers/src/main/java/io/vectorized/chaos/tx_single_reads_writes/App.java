package io.vectorized.chaos.tx_single_reads_writes;

import static spark.Spark.*;

import com.google.gson.Gson;
import java.io.*;
import java.util.HashMap;
import spark.*;

// java -cp $(pwd)/target/uber-1.0-SNAPSHOT.jar:$(pwd)/target/dependency/*
// io.vectorized.tx_single_reads_writes.App
public class App {
  public static class WorkflowSettings {
    public int writes;
    public int reads;
    public int retries = 0;
    public int transaction_timeout_config = -1;
  }

  public static class InitBody {
    public String hostname;
    public String results_dir;
    public String brokers;
    public String topic;
    public WorkflowSettings settings;
  }

  public static class OpsInfo {
    public long succeeded_ops = 0;
    public long failed_ops = 0;
    public long timedout_ops = 0;

    public OpsInfo copy() {
      var value = new OpsInfo();
      value.succeeded_ops = succeeded_ops;
      value.failed_ops = failed_ops;
      value.timedout_ops = timedout_ops;
      return value;
    }
  }

  public static class Info extends OpsInfo {
    public boolean is_active;
    public HashMap<String, OpsInfo> threads = new HashMap<>();
  }

  static enum State { FRESH, INITIALIZED, STARTED, STOPPED }

  public class JsonTransformer implements ResponseTransformer {
    private Gson gson = new Gson();

    @Override
    public String render(Object model) {
      return gson.toJson(model);
    }
  }

  State state = State.FRESH;

  InitBody params = null;
  Workload workload = null;

  void run() throws Exception {
    port(8080);

    get("/ping", (req, res) -> {
      res.status(200);
      return "";
    });

    get("/info", "application/json", (req, res) -> {
      var info = new Info();
      info.is_active = false;
      info.failed_ops = 0;
      info.succeeded_ops = 0;
      info.timedout_ops = 0;
      if (workload != null) {
        info.is_active = workload.is_active;
        info.threads = workload.get_ops_info();
        for (String key : info.threads.keySet()) {
          var value = info.threads.get(key);
          info.succeeded_ops += value.succeeded_ops;
          info.failed_ops += value.failed_ops;
          info.timedout_ops += value.timedout_ops;
        }
      }
      return info;
    }, new JsonTransformer());

    post("/init", (req, res) -> {
      if (state != State.FRESH) {
        throw new Exception("Unexpected state: " + state);
      }
      state = State.INITIALIZED;

      Gson gson = new Gson();

      params = gson.fromJson(req.body(), InitBody.class);
      File root = new File(params.results_dir);

      if (!root.exists() && !root.mkdirs()) {
        throw new Exception("Can't create folder: " + params.results_dir);
      }

      res.status(200);
      return "";
    });

    post("/event/:name", (req, res) -> {
      var name = req.params(":name");
      workload.event(name);
      res.status(200);
      return "";
    });

    post("/start", (req, res) -> {
      if (state != State.INITIALIZED) {
        throw new Exception("Unexpected state: " + state);
      }
      state = State.STARTED;

      workload = new Workload(params);
      workload.start();

      // curl -X POST http://127.0.0.1:8080/start -H 'Content-Type:
      // application/json' -d '{"topic":"topic1","brokers":"127.0.0.1:9092"}'
      res.status(200);
      return "";
    });

    post("/stop", (req, res) -> {
      if (state != State.STARTED) {
        throw new Exception("Unexpected state: " + state);
      }
      state = State.STOPPED;
      workload.stop();
      // curl -X POST http://127.0.0.1:8080/start -H 'Content-Type:
      // application/json' -d '{"topic":"topic1","brokers":"127.0.0.1:9092"}'
      res.status(200);
      return "";
    });

    post("/pause_before_send", (req, res) -> {
      workload.is_paused_before_send = true;
      res.status(200);
      return "";
    });

    post("/resume_before_send", (req, res) -> {
      workload.is_paused_before_send = false;
      synchronized (workload) { workload.notifyAll(); }
      res.status(200);
      return "";
    });

    post("/pause_before_abort", (req, res) -> {
      workload.is_paused_before_abort = true;
      res.status(200);
      return "";
    });

    post("/resume_before_abort", (req, res) -> {
      workload.is_paused_before_abort = false;
      synchronized (workload) { workload.notifyAll(); }
      res.status(200);
      return "";
    });
  }

  public static void main(String[] args) throws Exception { new App().run(); }
}
