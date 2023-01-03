package io.vectorized.reads_writes;
import static spark.Spark.*;

import com.google.gson.Gson;
import java.util.HashMap;
import spark.*;

// java -cp $(pwd)/target/uber-1.0-SNAPSHOT.jar:$(pwd)/target/dependency/*
// io.vectorized.reads_writes.App
public class App {
  public static class InitBody {
    public String name;
    public String brokers;
    public String topic;
    public int partitions;
    public int read_write_loop_slack = 1000;
  }

  public static class Metrics {
    public long total_writes = 0;
    public long total_reads = 0;
    public long min_writes = 0;
    public long min_reads = 0;
  }

  public class JsonTransformer implements ResponseTransformer {
    private Gson gson = new Gson();

    @Override
    public String render(Object model) {
      return gson.toJson(model);
    }
  }

  HashMap<String, Workload> workloads = new HashMap<>();

  void run() throws Exception {
    port(8080);

    get("/ping", (req, res) -> {
      res.status(200);
      return "";
    });

    get("/info", "application/json", (req, res) -> {
      try {
        if (!workloads.containsKey(req.queryParams("name"))) {
          throw new Exception("can't find: " + req.queryParams("name"));
        }
        return workloads.get(req.queryParams("name")).getMetrics();
      } catch (Exception e) {
        System.out.println(e);
        e.printStackTrace();
        throw e;
      }
    }, new JsonTransformer());

    post("/start", (req, res) -> {
      try {
        Gson gson = new Gson();
        InitBody params = gson.fromJson(req.body(), InitBody.class);

        workloads.put(params.name, new Workload(params));
        workloads.get(params.name).start();

        res.status(200);
        return "";
      } catch (Exception e) {
        System.out.println(e);
        e.printStackTrace();
        throw e;
      }
    });

    post("/stop", (req, res) -> {
      try {
        if (!workloads.containsKey(req.queryParams("name"))) {
          throw new Exception("can't find: " + req.queryParams("name"));
        }
        workloads.get(req.queryParams("name")).stop();
      } catch (Exception e) {
        System.out.println(e);
        e.printStackTrace();
        throw e;
      }
      res.status(200);
      return "";
    });

    get("/wait", (req, res) -> {
      try {
        if (!workloads.containsKey(req.queryParams("name"))) {
          throw new Exception("can't find: " + req.queryParams("name"));
        }
        workloads.get(req.queryParams("name")).waitStopped();
        workloads.remove(req.queryParams("name"));
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
