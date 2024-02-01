# Metrics Mock

metrics-mock.py is a script that starts an http server and serves
redpanda-like metrics from /metrics and /public-metrics endpoints.

So it serves as a stand-in for Redpanda when testing things in the
metrics pipeline, as using a large clusters simply to generate large
numbers of metrics is wasteful.

## Usage

The script has no dependencies and should be directly executable on
Python 3.10 or later.

The primary functionality of metrics mock is accessble through `metrics-mock.py host`,
which starts the HTTP server on port `9644` (configurable with `--port`):

```
./metrics-mock host
```

On another terminal run:

```
$ curl -sL 'http://127.0.0.1:9644/metrics' | head
```

which should produce output like:

```
# HELP vectorized_alien_receive_batch_queue_length Current receive batch queue length
# TYPE vectorized_alien_receive_batch_queue_length gauge
vectorized_alien_receive_batch_queue_length{shard="0"} 5923
# HELP vectorized_alien_total_received_messages Total number of received messages
# TYPE vectorized_alien_total_received_messages counter
vectorized_alien_total_received_messages{shard="0"} 1033
# HELP vectorized_alien_total_sent_messages Total number of sent messages
# TYPE vectorized_alien_total_sent_messages counter
vectorized_alien_total_sent_messages{shard="0"} 13440
# HELP vectorized_application_build Redpanda build information
```

There are several options to modify the behavior of the metrics endpoint,
specifically to emulate different sizes of Redpanda node and Redpanda cluster.

E.g., to approximate a T7 cluster, use the following flags:


```
./metrics-mock.py print --partitions 50000 --topics 10 --shards 31 --nodes 9
```

Each `./metrics-mock.py` instance acts as a single node in a multi-node cluster:
in the above example the mock will act as a single node in a 9 node cluster. This
is important when deciding which metrics appear for resources which are allocated
to a partitcular subset nodes, such as topic partitions. To emulate a differnet
node, pass `--node-id`. If you are emulating a multi-node cluster you should pass
a different node id to each mock, or else they all return the same metrics for
per-topic, per-partition or per-group metrics, which will significantly underestimate
the total metrics count.




