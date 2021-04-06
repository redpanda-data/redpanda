---
title: RPK commands
order: 7
---
# RPK commands

The global flags for the `rpk` command are:

```cmd
  -v, --verbose   enable verbose logging (default: false)
  -h, --help      help for info
```

## redpanda

### redpanda tune

Run all (`rpk redpanda tune all`) or some (i.e. `rpk redpanda tune cpu network`) of the tuners
available on `rpk`.

```cmd
Usage:
  rpk redpanda tune <list of elements to tune> [flags]
  rpk redpanda tune [command]

Available Commands:
  help        Display detailed infromation about the tuner

Flags:
      --config string          Redpanda config file, if not set the file will be searched for in the default locations
      --cpu-set string         Set of CPUs for tuner to use in cpuset(7) format if not specified tuner will use all available CPUs (default: "all")
  -r, --dirs strings           List of *data* directories. or places to store data. i.e.: '/var/vectorized/redpanda/', usually your XFS filesystem on an NVMe SSD device
  -d, --disks strings          Lists of devices to tune f.e. 'sda1'
      --interactive            Ask for confirmation on every step (e.g. tuner execution, configuration generation)
  -m, --mode string            Operation Mode: one of: [sq, sq_split, mq]
  -n, --nic strings            Network Interface Controllers to tune
      --output-script string   If set tuners will generate tuning file that can later be used to tune the system
      --reboot-allowed         If set will allow tuners to tune boot paramters  and request system reboot
      --timeout duration       The maximum time to wait for the tune processes to complete. The value passed is a sequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h' (default: 10s)
```

### redpanda start

Start redpanda.

```cmd
Usage:
  rpk redpanda start [flags]

Flags:
      --advertise-kafka-addr strings   The list of Kafka addresses to advertise (<host>:<port>)
      --advertise-rpc-addr string      The advertised RPC address (<host>:<port>)
      --check                  When set to false will disable system checking before starting redpanda (default: true)
      --config string          Redpanda config file, if not set the file will be searched for in the default locations
      --install-dir string     Directory where redpanda has been installed
      --kafka-addr strings             The list of Kafka listener addresses to bind to (<host>:<port>)
      --node-id int                    The node ID. Must be an integer and must be unique within a cluster
      --rpc-addr string                The RPC address to bind to (<host>:<port>)
  -s, --seeds strings                  A comma-separated list of seed node addresses (<host>[:<port>]) to connect to
      --timeout duration       The maximum time to wait for the checks and tune processes to complete. The value passed is a sequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h' (default: 10s)
      --tune                   When present will enable tuning before starting redpanda
      --well-known-io string   The cloud vendor and VM type, in the format <vendor>:<vm type>:<storage type>
```

### redpanda mode

By default, Redpanda runs in development mode. For [production deployments](https://vectorized.io/docs/production-deployment/), set the redpanda mode to `production`.

```cmd
Usage:
  rpk redpanda mode <mode> [flags]

Flags:
      <mode>            'development' (default) or 'production'
      --config string   Redpanda config file, if not set the file will be searched for in the default locations
```

### redpanda config

Edit configuration.

#### redpanda config set

Set configuration values, such as the node IDs or the list of seed servers

```cmd
Usage:
  rpk redpanda config set <key> <value> [flags]

Flags:
      --config string   Redpanda config file, if not set the file will be searched for in the default location
      --format string   The value format. Can be 'single', for single values such as '/etc/redpanda' or 100; and 'json' and 'yaml' when partially or completely setting config objects (default: "single")
```

#### redpanda config bootstrap

Initialize the configuration to bootstrap a cluster. --id is mandatory. `bootstrap` will expect the machine it's running on to have only one non-loopback IP address associated to it, and use it in the configuration as the node's address. If it has multiple IPs, --self must be specified. In that case, the given IP will be used without checking whether it's among the machine's addresses or not. The elements in --ips must be separated by a comma, no spaces. If omitted, the node will be configured as a root node, that otherones can join later.

```cmd
Usage:
  rpk redpanda config bootstrap --id <id> [--self <ip>] [--ips <ip1,ip2,...>] [flags]

Flags:
      --config string   Redpanda config file, if not set the file will be searched for in the default location
      --id int          This node's ID (required). (default: -1)
      --ips strings     The list of known node addresses or hostnames
      --self string     Hint at this node's IP address from within the list passed in --ips
```

## topic

Interact with the Redpanda API to work with topics.

The global flags for the `rpk topic` command are:

```cmd
      --brokers strings   Comma-separated list of broker 'ip:port' pairs
      --config string     Redpanda config file, if not set the file will be searched for in the default locations
```

### topic create

Create a topic.

```cmd
Usage:
  rpk topic create <topic name> [flags]

Flags:
      --compact            Enable topic compaction
  -p, --partitions int32   Number of partitions (default: 1)
  -r, --replicas int16     Replication factor. If it's negative or is left unspecified, it will use the cluster's default topic replication factor. (default: -1)
```

### topic delete

Delete a topic.

```cmd
Usage:
  rpk topic delete <topic name> [flags]
```

### topic describe

Describe a topic. Default values of the configuration are omitted.

```cmd
Usage:
  rpk topic describe <topic> [flags]

Flags:
      --page int        The partitions page to display. If negative, all partitions will be shown (default: -1)
      --page-size int   The number of partitions displayed per page (default: 20)
      --watermarks      If enabled, will display the topic's partitions' high watermarks (default: true)
```

### topic list

List topics.

```cmd
Usage:
  rpk topic list [flags]
  
Aliases:
  list, ls
```

### topic set-config

Set the topic's config key/value pairs

```cmd
Usage:
  rpk topic set-config <topic> <key> <value> [flags]
```

## cluster

### cluster info

Get the cluster's info

```cmd
Usage:
  rpk cluster info [flags]

Aliases:
  info, status
```

## container

Manage a local container cluster

### container start

Start a local container cluster

```cmd
Usage:
  rpk container start [flags]
  
Flags:
  -n, --nodes uint     The number of nodes to start (default: 1)
      --retries uint   The amount of times to check for the cluster before considering it unstable and exiting. (default: 10)
```

### container stop

Stop an existing local container cluster

```cmd
Usage:
  rpk container stop [flags]
```

### container purge

Stop and remove an existing local container cluster's data

```cmd
Usage:
  rpk container purge [flags]
```

## iotune

Measure filesystem performance and create IO configuration file.

```cmd
Usage:
  rpk iotune [flags]

Flags:
      --config string         Redpanda config file, if not set the file will be searched for in the default locations
      --directories strings   List of directories to evaluate
      --duration duration     Duration of tests.The value passed is a sequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h' (default: 10m0s)
      --out string            The file path where the IO config will be written (default: "/etc/redpanda/io-config.yaml")
      --timeout duration      The maximum time after --duration to wait for iotune to complete. The value passed is a sequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h' (default: 1h0m0s)
```

## generate

Generate a configuration template for related services.

### generate grafana-dashboard

Generate a Grafana dashboard for redpanda metrics.

```cmd
Usage:
  rpk generate grafana-dashboard [flags]

Flags:
      --datasource string       The name of the Prometheus datasource as configured in your grafana instance.
      --job-name string         The prometheus job name by which to identify the redpanda nodes (default: "redpanda")
      --prometheus-url string   The redpanda Prometheus URL from where to get the metrics metadata (default: "http://localhost:9644/metrics")
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

```cmd
Usage:
  rpk generate prometheus-config [flags]

Flags:
      --config string        The path to the redpanda config file
      --job-name string      The prometheus job name by which to identify the redpanda nodes (default: "redpanda")
      --node-addrs strings   A comma-delimited list of the addresses (<host:port>) of all the redpanda nodes
                             in a cluster. The port must be the one configured for the nodes' admin API
                             (9644 by default)
      --seed-addr string     The URL of a redpanda node with which to discover the rest
```

## debug

### debug info

Check the resource usage in the system, and optionally send it to Vectorized.

```cmd
Usage:
  rpk debug info [flags]

Aliases:
  info, status

Flags:
      --config string         Redpanda config file, if not set the file will be searched for in the default locations
      --send rpk debug info   Tells `rpk debug info` whether to send the gathered resource usage data to Vectorized
      --timeout duration      The maximum amount of time to wait for the metrics to be gathered. The value passed is a sequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h' (default: 2s)
```
