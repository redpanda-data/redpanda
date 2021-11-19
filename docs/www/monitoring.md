---
title: Monitoring
order: 2
---
# Monitoring

## Prometheus Configuration

Redpanda exports Prometheus metrics on `<node ip>:9644/metrics`. If you have an
existing Prometheus instance, you can generate the relevant configuration using

```
rpk generate prometheus-config
```

The command will output a YAML object you can add to the `scrape_configs` list
in your Prometheus config file:

```
- job_name: redpanda-node
  static_configs:
  - targets:
    - 172.31.18.239:9644
    - 172.31.18.238:9643
    - 172.31.18.237:9642
```

If you run the command on a node where redpanda is running, it will use
redpanda's Kafka API to discover the other nodes. Otherwise, you can pass
`seed-addr` to specify a remote redpanda node from which to discover the other
ones, or `--node-addrs` with a comma-separated list of all known cluster node
addresses.

## Grafana Configuration

You can generate a comprehensive Grafana dashboard with
```
rpk generate grafana-dashboard --datasource <name> --metrics-endpoint <url>
```

`--metrics-endpoint` is the address to a redpanda node's metrics endpoint
(`<node ip>:9644/metrics`, by default).

`<name>` is the name of the Prometheus datasource configured in your
Grafana instance.

Right out of the box, it will generate panels tracking latency for p50, p95 and
p99, throughput, and errors segmentated by type.

Simply pipe the commmand's output to a file and import it in Grafana.

```
rpk generate grafana-dashboard \
  --datasource prometheus \
  --metrics-endpoint 172.32.89.236:9644/metrics > redpanda-dashboard.json
```

## Stats Reporting

Redpanda ships with an additional `systemd` service which executes periodically
and reports resource usage and configuration data to Vectorized's metrics API.
It is enabled by default, and the data is anonymous. If you'd like us to be able
to identify your cluster's data, so that we can monitor it and alert you of
possible issues, please set the `organization` (your company's domain) and
`cluster_id` (usually your team's or project's name) configuration fields. For
example:

```
rpk config set organization 'vectorized.io'
rpk config set cluster_id 'us-west-2'
```

To opt out of all metrics reporting, set `rpk.enable_usage_stats` to false via
`rpk`

```
rpk config set rpk.enable_usage_stats false
```

## Metrics

Through Prometheus, you can access many metrics about the Redpanda process.
Most of the metrics are used for debugging, but these metrics can be useful to measure system health:

| Metric | Definition | Diagnostics |
| --- | --- | --- |
| vectorized_application_uptime | Redpanda uptime in milliseconds |  |
| vectorized_partition_last_stable_offset | Last stable offset | If this is the last record received by the cluster, then the cluster is up-to-date and ready for maintenance |
| vectorized_io_queue_delay | Total delay time in the queue | Can indicate latency caused by disk operations in seconds |
| vectorized_io_queue_queue_length | Number of requests in the queue | Can indicate latency caused by disk operations |
| vectorized_kafka_rpc_active_connections | kafka_rpc: Currently active connections | Shows the number of clients actively connected |
| vectorized_kafka_rpc_connects | kafka_rpc: Number of accepted connections | Compare to the value at a previous time to derive the rate of accepted connections |
| vectorized_kafka_rpc_received_bytes | kafka_rpc: Number of bytes received from the clients in valid requests | Compare to the value at a previous time to derive the throughput in kafka layer in bytes/sec received |
| vectorized_kafka_rpc_requests_completed | kafka_rpc: Number of successfull requests | Compare to the value at a previous time to derive the messages per sec per shard |
| vectorized_kafka_rpc_requests_pending | kafka_rpc: Number of requests being processed by server |  |
| vectorized_kafka_rpc_sent_bytes | kafka_rpc: Number of bytes sent to clients |  |
| vectorized_kafka_rpc_service_errors | kafka_rpc: Number of service errors |  |
| vectorized_raft_leadership_changes | Number of leadership changes | High value can indicate nodes failing and causing leadership changes |
| vectorized_reactor_utilization | CPU utilization | Shows the true utilization of the CPU by Redpanda process |
| vectorized_storage_log_compacted_segment | Number of compacted segments |  |
| vectorized_storage_log_log_segments_created | Number of created log segments |  |
| vectorized_storage_log_partition_size | Current size of partition in bytes |  |
| vectorized_storage_log_read_bytes | Total number of bytes read |  |
| vectorized_storage_log_written_bytes | Total number of bytes written |  |

These categories of metrics are presented specificly by the seastar component of Redpanda: reactor, memory, scheduler, alien, io_queue
