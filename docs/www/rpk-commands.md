---
title: RPK commands
order: 7
---
# RPK commands

## redpanda

### tune

Run all (`rpk redpanda tune all`) or some (i.e. `rpk redpanda tune cpu network`) of the tuners
available on **rpk.**

```

Usage:
  rpk redpanda tune <list_of_elements_to_tune> [flags]

Flags:
      --config string          Redpanda config file, if not set the file will be searched for in the default locations (default "/etc/redpanda/redpanda.yaml")
      --cpu-set string         Set of CPUs for tuner to use in cpuset(7) format if not specified tuner will use all available CPUs (default "all")
  -r, --dirs strings           List of *data* directories. or places to store data. i.e.: '/var/vectorized/redpanda/', usually your XFS filesystem on an NVMe SSD device
  -d, --disks strings          Lists of devices to tune f.e. 'sda1'
      --interactive            Ask for confirmation on every step (e.g. tuner execution, configuration generation)
  -m, --mode string            Operation Mode: one of: [sq, sq_split, mq]
  -n, --nic strings            Network Interface Controllers to tune
      --output-script string   If set tuners will generate tuning file that can later be used to tune the system
      --reboot-allowed         If set will allow tuners to tune boot paramters  and request system reboot
      --timeout duration       The maximum time to wait for the tune processes to complete. The value passed is a sequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h' (default 10s)
```

### start

Start redpanda.

```

Usage:
  rpk redpanda start [flags]

Flags:
      --check                  When set to false will disable system checking before starting redpanda (default true)
      --config string          Redpanda config file, if not set the file will be searched for in the default locations (default "/etc/redpanda/redpanda.yaml")
      --install-dir string     Directory where redpanda has been installed
      --timeout duration       The maximum time to wait for the checks and tune processes to complete. The value passed is a sequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h' (default 10s)
      --tune                   When present will enable tuning before starting redpanda
      --well-known-io string   The cloud vendor and VM type, in the format <vendor>:<vm type>:<storage type>
```

### mode

Enable a default configuration mode (development, production). See the [**rpk
Modes**](https://vectorized.io/docs/rpk-modes/) section.

```

Usage:
  rpk redpanda mode {development, production} [flags]

Flags:
      --config string   Redpanda config file, if not set the file will be searched for in the default locations (default "/etc/redpanda/redpanda.yaml")
```

### config

Edit configuration.

#### config set

Set configuration values, such as the node IDs or the list of seed servers

```

Usage:
  rpk redpanda config set <key> <value> [flags]

Flags:
      --config string   Redpanda config file, if not set the file will be searched for in default location (default "/etc/redpanda/redpanda.yaml")
      --format string   The value format. Can be 'single', for single values such as '/etc/redpanda' or 100; and 'json', 'toml', 'yaml','yml', 'properties', 'props', 'prop', or 'hcl' when partially or completely setting config objects (default "single")
```

#### config bootstrap

Initialize the configuration to bootstrap a cluster. --id is mandatory. `bootstrap` will expect the machine it's running on to have only one non-loopback IP address associated to it, and use it in the configuration as the node's address. If it has multiple IPs, --self must be specified. In that case, the given IP will be used without checking whether it's among the machine's addresses or not. The elements in --ips must be separated by a comma, no spaces. If omitted, the node will be configured as a root node, that otherones can join later.

```

Usage:
  rpk redpanda config bootstrap --id <id> [--self <ip>] [--ips <ip1,ip2,...>] [flags]

Flags:
      --config string   Redpanda config file, if not set the file will be searched for in the default location (default "/etc/redpanda/redpanda.yaml")
      --id int          This node's ID (required). (default -1)
      --ips strings     The list of known node addresses or hostnames
      --self string     Hint at this node's IP address from within the list passed in --ips
```

## topic

Interact with the Redpanda API.

```
Global flags: --brokers strings   Comma-separated list of broker ip:port pair
```

### create

Create a topic.

```
Usage:
  rpk topic create <topic name> [flags]

Flags:
      --compact            Enable topic compaction
  -p, --partitions int32   Number of partitions (default 1)
  -r, --replicas int16     Number of replicas (default 1)
```

### delete

Delete a topic.

```
Usage:
  rpk topic delete <topic name> [flags]
```

### describe

Describe a topic. Default values of the configuration are omitted.

```
Usage:
  rpk topic describe <topic> [flags]

Flags:
      --page int        The partitions page to display. If negative, all partitions will be shown
      --page-size int   The number of partitions displayed per page (default 20)
      --watermarks      If enabled, will display the topic's partitions' high watermarks (default true)
```

### list

List topics.

```
Usage:
  rpk topic list [flags]
  
Aliases:
  list, ls
```

### set-config

Set the topic's config key/value pairs

```
Usage:
  rpk topic set-config <topic> <key> [<value>] [flags]
```

## cluster

### info

Get the cluster's info

```
Usage:
  rpk cluster info [flags]

Flags:
  --brokers strings   Comma-separated list of broker ip:port pairs
  --config string     Redpanda config file, if not set the file will be searched for in the default locations
```

## container

Manage a local container cluster

### container start

Start a local container cluster

```
Usage:
  rpk container start [flags]
  
Flags:
  -n, --nodes uint   The number of nodes to start (default 1)
```

### container stop

Stop an existing local container cluster

```
Usage:
  rpk container stop
```

### container purge

Stop and remove an existing local container cluster's data

```
Usage:
  rpk container purge
```

## iotune

Measure filesystem performance and create IO configuration file.

```

Usage:
  rpk iotune [flags]

Flags:
      --config string         Redpanda config file, if not set the file will be searched for in the default locations (default "/etc/redpanda/redpanda.yaml")
      --directories strings   List of directories to evaluate
      --duration duration     Duration of tests.The value passed is a sequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h' (default 10m0s)
      --out string            The file path where the IO config will be written (default "/var/lib/redpanda/data/io-config.yaml")
      --timeout duration      The maximum time after --duration to wait for iotune to complete. The value passed is a sequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h' (default 1h0m0s)
```

## generate

Generate a configuration template for related services.

### generate grafana-dashboard

Generate a Grafana dashboard for redpanda metrics.

```
Usage:
  rpk generate grafana-dashboard [flags]

Flags:
      --datasource string       The name of the Prometheus datasource as configured in your grafana instance.
      --job-name string         The prometheus job name by which to identify the redpanda nodes (default "redpanda")
      --prometheus-url string   The redpanda Prometheus URL from where to get the metrics metadata (default "http://localhost:9644/metrics")
```

### generate prometheus-config

Generate the Prometheus configuration to scrape redpanda nodes. This command's
output should be added to the `scrape_configs` array in your Prometheus
instance's YAML config file.

If `--seed-addr` is passed, it will be used to discover the rest of the cluster
hosts via redpanda's Kafka API. If `--node-addrs` is passed, they will be used
directly. Otherwise, `rpk generate prometheus-conf` will read the redpanda
config file and use the node IP configured there. `--config` may be passed to
especify an arbitrary config file.

```
Usage:
  rpk generate prometheus-config [flags]

Flags:
      --config string        The path to the redpanda config file (default "/etc/redpanda/redpanda.yaml")
      --job-name string      The prometheus job name by which to identify the redpanda nodes (default "redpanda")
      --node-addrs strings   A comma-delimited list of the addresses (<host:port>) of all the redpanda nodes
                             in a cluster. The port must be the one configured for the nodes' admin API
                             (9644 by default)
      --seed-addr string     The URL of a redpanda node with which to discover the rest
```

## debug

### info

Check the resource usage in the system, and optionally send it to Vectorized.

```
Usage:
  rpk debug info [flags]

Flags:
      --config string         Redpanda config file, if not set the file will be searched for in the default locations
      --send   bool           Tells 'rpk debug info' whether to send the gathered resource usage data to Vectorized
      --timeout duration      The maximum amount of time to wait for the metrics to be gathered. The value passed is a sequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h' (default 2s)
```
