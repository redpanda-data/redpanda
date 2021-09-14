---
title: RPK commands
order: 7
---
# RPK commands

Commands marked with ![linux icon][linux] are available on Linux.
Commands marked with ![mac icon][mac] are available on Mac.

The global flags for the `rpk` command are:

```cmd
  -v, --verbose   enable verbose logging (default: false)
  -h, --help      help for info
```

## version ![linux icon][linux] ![mac icon][mac]

Check the current version.

```cmd
Usage:
  rpk version
```

## redpanda ![linux icon][linux]

### redpanda tune ![linux icon][linux]

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

### redpanda start ![linux icon][linux]

Start redpanda.

```cmd
Usage:
  rpk redpanda start [flags]

Flags:
      --advertise-kafka-addr strings   A comma-separated list of Kafka addresses to advertise (<name>://<host>:<port>)
      --advertise-pandaproxy-addr      A comma-separated list of Pandaproxy addresses to advertise (<name>://<host>:<port>)
      --advertise-rpc-addr string      The advertised RPC address (<host>:<port>)
      --check                          When set to false will disable system checking before starting redpanda (default: true)
      --config string                  Redpanda config file, if not set the file will be searched for in the default locations
      --install-dir string             Directory where redpanda has been installed
      --kafka-addr strings             A comma-separated list of Kafka listener addresses to bind to (<name>://<host>:<port>)
      --node-id int                    The node ID. Must be an integer and must be unique within a cluster
      --pandaproxy-addr                A comma-separated list of Pandaproxy listener addresses to bind to (<name>://<host>:<port>)
      --rpc-addr string                The RPC address to bind to (<host>:<port>)
      --schema-registry-addr           A comma-separated list of Schema Registry listener addresses to bind to (<name>://<host>:<port>)
  -s, --seeds strings                  A comma-separated list of seed node addresses (<host>[:<port>]) to connect to
      --timeout duration               The maximum time to wait for the checks and tune processes to complete. The value passed is a sequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h' (default: 10s)
      --tune                           When present will enable tuning before starting redpanda
      --well-known-io string           The cloud vendor and VM type, in the format <vendor>:<vm type>:<storage type>
```

### redpanda mode ![linux icon][linux] ![mac icon][mac]

By default, Redpanda runs in development mode. For [production deployments](https://vectorized.io/docs/production-deployment/), set the redpanda mode to `production`.

```cmd
Usage:
  rpk redpanda mode <mode> [flags]

Flags:
      <mode>            'development' (default) or 'production'
      --config string   Redpanda config file, if not set the file will be searched for in the default locations
```

### redpanda config ![linux icon][linux]

Edit configuration.

#### redpanda config set ![linux icon][linux]

Set configuration values, such as the node IDs or the list of seed servers

```cmd
Usage:
  rpk redpanda config set <key> <value> [flags]

Flags:
      --config string   Redpanda config file, if not set the file will be searched for in the default location
      --format string   The value format. Can be 'single', for single values such as '/etc/redpanda' or 100; and 'json' and 'yaml' when partially or completely setting config objects (default: "single")
```

#### redpanda config bootstrap ![linux icon][linux]

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

## topic ![linux icon][linux] ![mac icon][mac]

Interact with the Redpanda API to work with topics.

The global flags for the `rpk topic` command are:

```cmd
      --brokers strings         Comma-separated list of broker ip:port pairs
      --config string           Redpanda config file, if not set the file will be searched for in the default locations
      --user string             SASL user to be used for authentication.
      --password string         SASL password to be used for authentication.
      --sasl-mechanism string   The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.
      --tls-cert string         The certificate to be used for TLS authentication with the broker.
      --tls-key string          The certificate key to be used for TLS authentication with the broker.
      --tls-truststore string   The truststore to be used for TLS communication with the broker.
```

### topic create ![linux icon][linux] ![mac icon][mac]

Create a topic.

```cmd
Usage:
  rpk topic create <topic name> [flags]

Flags:
      --compact            Enable topic compaction
  -p, --partitions int32   Number of partitions (default: 1)
  -r, --replicas int16     Replication factor. If it's negative or is left unspecified, it will use the cluster's default topic replication factor. (default: -1)
  -c, --topic-config stringArray   Config entries in the format <key>:<value>. May be used multiple times to add more entries.
```

### topic delete ![linux icon][linux] ![mac icon][mac]

Delete a topic.

```cmd
Usage:
  rpk topic delete <topic name> [flags]
```

### topic describe ![linux icon][linux] ![mac icon][mac]

Describe a topic. Default values of the configuration are omitted.

```cmd
Usage:
  rpk topic describe <topic> [flags]

Flags:
      --page int        The partitions page to display. If negative, all partitions will be shown (default: -1)
      --page-size int   The number of partitions displayed per page (default: 20)
      --watermarks      If enabled, will display the topic's partitions' high watermarks (default: true)
```

### topic produce ![linux icon][linux] ![mac icon][mac]

Produce a record from data entered in stdin.

```cmd
Usage:
  rpk topic produce <topic> [flags]

Flags:
  -H, --header stringArray   Header in format <key>:<value>. May be used multiple times to add more headers.
  -j, --jvm-partitioner      Use a JVM-compatible partitioner. If --partition is passed with a positive value, this will be overridden and a manual partitioner will be used.
  -k, --key string           Key for the record. Currently only strings are supported.
  -n, --num int              Number of records to send. (default 1)
  -p, --partition int32      Partition to produce to. (default -1)
  -t, --timestamp string     RFC3339-compliant timestamp for the record. If the value passed can't be parsed, the current time will be used.
```

### topic consume ![linux icon][linux] ![mac icon][mac]

Consume (read) records from a topic.

```cmd
Usage:
  rpk topic consume <topic> [flags]

Flags:
      --commit                  Commit group offset after receiving messages (Only when consuming as Consumer Group)
  -g, --group string            Consumer Group to use for consuming
      --offset string           Offset to start consuming. Supported values: oldest, newest (default "oldest")
  -p, --partitions int32Slice   Partitions to consume from (default [])
      --pretty-print            Pretty-print the consumed messages. (default true)
```

### topic list ![linux icon][linux] ![mac icon][mac]

List topics.

```cmd
Usage:
  rpk topic list [flags]
  
Aliases:
  list, ls
```

### topic alter-config ![linux icon][linux] ![mac icon][mac]

Set, delete, append to, and remove from topic config's key/value pairs

```cmd
Usage:
  rpk topic alter-config <topics...> --set <key>=<value> --append <key>=<value> [flags]
```

## cluster ![linux icon][linux] ![mac icon][mac]

### cluster info ![linux icon][linux] ![mac icon][mac]

Get the cluster's info

```cmd
Usage:
  rpk cluster info [flags]

Aliases:
  info, status
```

## container ![linux icon][linux] ![mac icon][mac]

Manage a local container cluster

### container start ![linux icon][linux] ![mac icon][mac]

Start a local container cluster

```cmd
Usage:
  rpk container start [flags]
  
Flags:
  -n, --nodes uint     The number of nodes to start (default: 1)
      --retries uint   The amount of times to check for the cluster before considering it unstable and exiting. (default: 10)
```

### container stop ![linux icon][linux] ![mac icon][mac]

Stop an existing local container cluster

```cmd
Usage:
  rpk container stop [flags]
```

### container purge ![linux icon][linux] ![mac icon][mac]

Stop and remove an existing local container cluster's data

```cmd
Usage:
  rpk container purge [flags]
```

## acl ![linux icon][linux] ![mac icon][mac]

Manage ACLs

The global flags for `rpk acl` are:
```cmd
      --brokers strings         Comma-separated list of broker ip:port pairs
      --config string           Redpanda config file, if not set the file will be searched for in the default locations
      --password string         SASL password to be used for authentication.
      --sasl-mechanism string   The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.
      --tls-cert string         The certificate to be used for TLS authentication with the broker.
      --tls-key string          The certificate key to be used for TLS authentication with the broker.
      --tls-truststore string   The truststore to be used for TLS communication with the broker.
      --user string             SASL user to be used for authentication.
  -v, --verbose                 enable verbose logging (default false)
```

### acl create ![linux icon][linux] ![mac icon][mac]

Create ACLs

```cmd
Usage:
  rpk acl create [flags]

Flags:
      --allow-host strings        Host from which access will be granted. Can be passed many times.
      --allow-principal strings   Principal to which permissions will be granted. Can be passed many times.
      --deny-host strings         Host from which access will be denied. Can be passed many times.
      --deny-principal strings    Principal to which permissions will be denied. Can be passed many times.
      --name-pattern string       The name pattern type to be used when matching the resource names. Supported values: any, match, literal, prefixed. (default "literal")
      --operation strings         Operation that the principal will be allowed or denied. Can be passed many times. Supported values: any, all, read, write, create, delete, alter, describe, clusteraction, describeconfigs, alterconfigs, idempotentwrite.
      --resource string           The target resource for the ACL. Supported values: *, cluster, group, topic, transactionalid.
      --resource-name string      The name of the target resource for the ACL.
```

### acl delete ![linux icon][linux] ![mac icon][mac]

Delete ACLs

```cmd
Usage:
  rpk acl delete [flags]

Flags:
      --allow-host strings        Host from which access will be granted. Can be passed many times.
      --allow-principal strings   Principal to which permissions will be granted. Can be passed many times.
      --deny-host strings         Host from which access will be denied. Can be passed many times.
      --deny-principal strings    Principal to which permissions will be denied. Can be passed many times.
      --name-pattern string       The name pattern type to be used when matching the resource names. Supported values: any, match, literal, prefixed. (default "literal")
      --operation strings         Operation that the principal will be allowed or denied. Can be passed many times. Supported values: any, all, read, write, create, delete, alter, describe, clusteraction, describeconfigs, alterconfigs, idempotentwrite.
      --resource string           The target resource for the ACL. Supported values: *, cluster, group, topic, transactionalid.
      --resource-name string      The name of the target resource for the ACL.
```

### acl list ![linux icon][linux] ![mac icon][mac]

List ACLs

```cmd
Usage:
  rpk acl list [flags]

Aliases:
  list, ls

Flags:
      --host strings           Host to filter by. Can be passed multiple times to filter by many hosts.
      --name-pattern string    The name pattern type to be used when matching affected resources. Supported values: any, match, literal, prefixed.
      --operation strings      Operation to filter by. Can be passed multiple times to filter by many operations. Supported values: any, all, read, write, create, delete, alter, describe, clusteraction, describeconfigs, alterconfigs, idempotentwrite.
      --permission strings     Permission to filter by. Can be passed many times to filter by multiple permission types. Supported values: any, deny, allow.
      --principal strings      Principal to filter by. Can be passed multiple times to filter by many principals.
      --resource string        Resource type to filter by. Supported values: *, cluster, group, topic, transactionalid.
      --resource-name string   The name of the resource of the given type.
```

### acl user ![linux icon][linux] ![mac icon][mac]

Manage users

The global flags for `rpk acl user` are:

```cmd
      --api-url string   The Admin API URL (default "localhost:9644")
```

#### acl user create ![linux icon][linux] ![mac icon][mac]

Create users

```cmd
Usage:
  rpk acl user create [flags]

Flags:
      --new-password string   The new user's password
      --new-username string   The user to be created
```

#### acl user delete ![linux icon][linux] ![mac icon][mac]

Delete users

```cmd
Usage:
  rpk acl user delete [flags]

Flags:
      --delete-username string   The user to be deleted
```

#### acl user list ![linux icon][linux] ![mac icon][mac]

List users

```cmd
List users

Usage:
  rpk acl user list [flags]

Aliases:
  list, ls
```

## wasm ![linux icon][linux] ![mac icon][mac]

Deploy and remove inline WASM engine scripts

The global flags for `rpk wasm` are:
```cmd
      --brokers strings         Comma-separated list of broker ip:port pairs
      --config string           Redpanda config file, if not set the file will be searched for in the default locations
      --password string         SASL password to be used for authentication.
      --sasl-mechanism string   The authentication mechanism to use. Supported values: SCRAM-SHA-256, SCRAM-SHA-512.
      --tls-cert string         The certificate to be used for TLS authentication with the broker.
      --tls-key string          The certificate key to be used for TLS authentication with the broker.
      --tls-truststore string   The truststore to be used for TLS communication with the broker.
      --user string             SASL user to be used for authentication.
  -v, --verbose                 enable verbose logging (default false)
```

### wasm generate ![linux icon][linux] ![mac icon][mac]

Create an npm template project for the inline WASM engine

```cmd
Usage:
  rpk wasm generate <project directory> [flags]
```


### wasm deploy ![linux icon][linux] ![mac icon][mac]

Deploy inline WASM scripts

```cmd
Usage:
  rpk wasm deploy <path> [flags]

Flags:
      --description string   Optional description about what the wasm function does, for reference.
```

### wasm remove ![linux icon][linux] ![mac icon][mac]

Remove an inline WASM script

```cmd
Usage:
  rpk wasm remove <name> [flags]
```


## iotune ![linux icon][linux]

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

## generate ![linux icon][linux] ![mac icon][mac]

Generate a configuration template for related services.

### generate grafana-dashboard ![linux icon][linux] ![mac icon][mac]

Generate a Grafana dashboard for redpanda metrics.

```cmd
Usage:
  rpk generate grafana-dashboard [flags]

Flags:
      --datasource string       The name of the Prometheus datasource as configured in your grafana instance.
      --job-name string         The prometheus job name by which to identify the redpanda nodes (default: "redpanda")
      --prometheus-url string   The redpanda Prometheus URL from where to get the metrics metadata (default: "http://localhost:9644/metrics")
```

### generate prometheus-config ![linux icon][linux] ![mac icon][mac]

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

## debug ![linux icon][linux]

### debug info ![linux icon][linux]

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
[linux]: https://vectorized.io/images/icon-linux.svg "Available on Linux"
[mac]: https://vectorized.io/images/icon-mac.svg "Available on Mac"
