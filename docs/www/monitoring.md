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
rpk generate grafana-dashboard --datasource <name> --prometheus-url <url>
```

`--prometheus-url` is the address to a redpanda node's metrics endpoint
(`<node ip>:9644/metrics`, by default).

`<name>` is the name of the Prometheus datasource configured in your
Grafana instance.

Right out of the box, it will generate panels tracking latency for p50, p95 and
p99, throughput, and errors segmentated by type.

Simply pipe the commmand's output to a file and import it in Grafana.

```
rpk generate grafana-dashboard \
  --datasource prometheus \
  --prometheus-url 172.32.89.236:9644/metrics > redpanda-dashboard.json
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
