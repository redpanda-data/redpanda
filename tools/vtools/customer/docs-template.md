# Redpanda Documentation

{{customer_name}}, {{date}}

## Infrastructure Setup

Redpanda does most of the configuration and tuning itself, but there are a few steps you'll need to follow to set up the infrastructure:

### IO

To get the best IO performance, we recommend the data directory (`/var/lib/redpanda`, by default) to reside on an XFS partition in a local NVMe SSD - Redpanda can drive your SSD at maximum throughput at all times. Using networked block devices is strongly discouraged, because of their inherent performance limitations.

### Network

By default, redpanda uses ports 33145 (internal RPC), 9092 (Kafka API), and 9644 (Admin API and Prometheus exporter), so the firewall should allow inbound traffic on them.

Additionally, every node in a cluster should be able to reach each other over TCP.

Lastly, there's a periodically-executed service that sends hardware, resource usage and configuration data to Vectorized's metrics API. If you'd like to help us improve redpanda and allow this data to be sent, please allow outbound traffic to `https://m.rp.vectorized.io`. To learn more about metrics reporting, please see the **Monitoring** section.

## Installation

> The URLs in the steps below are for your installation only. Don't share them with anyone.

To get started with redpanda, first set up the package repository:

For RPM-based Linux distributions:

```
# Set up the repository
curl -s https://{{master_token}}:@packagecloud.io/install/repositories/vectorizedio/v/script.rpm.sh | sudo bash

# Install the redpanda package
sudo yum install -y redpanda
```

For Debian-based distributions:

```
# Set up the repository
curl -s https://{{master_token}}:@packagecloud.io/install/repositories/vectorizedio/v/script.deb.sh | sudo bash

# Install the redpanda package
sudo apt install -y redpanda
```

### Redpanda & Rpk

Redpanda has a CLI tool called rpk, the Redpanda Keeper, which is a CLI & toolbox for redpanda. As a user, you will always interact with it to auto-tune the OS and hardware settings, start redpanda (when not run as a systemd service) and edit the configuration.

## Single-Node Quickstart

If you're just trying out redpanda on your personal computer, make sure to turn on _development_ mode, so that no permanent configuration changes are made to your OS:

`sudo rpk mode dev`

To make sure redpanda runs as fast as it can, first run the tuner service, which will autoconfigure your VM to guarantee the best performance.

```
sudo systemctl start redpanda-tuner
```

That's it! Now you're ready to run redpanda:

```
sudo systemctl start redpanda
```

You can inspect the logs by running 

```
journalctl -u redpanda
```

## Cluster Quickstart

> Before following along, make sure you followed the steps described in **Installation** on every node.

> All of the concepts and commands used in this guide are described in later sections of the documentation. Please refer to them to learn more.

> This guide assumes that you have configured your firewall to allow traffic between the nodes on ports 33145, 9092 and 9644. Please see the **Configuration** section for reference on which ports should allow inbound traffic.

For each new redpanda cluster, there will be one **root** node (not to be confused with a leader node). The root node is bootstrapped and then new nodes can join it to form a cluster.

To start a cluster, pick a node to be the root and use `rpk config bootstrap` to bootstrap the configuration. For a root node, you only need to set its ID and the IP it should use for its API (usually its private IP). Each node's ID needs to be a unique positive integer.

`sudo rpk config bootstrap --id <unique id> --self <ip>`

Then for each node you'd like to join, you just have to set its ID and give it the root node's address:

> rpk will try to discover a non-loopback IP it can use. However, if the host has multiple addresses associated with it, you'll need to pass the --self flag so it knows which one to use.

`sudo rpk config bootstrap --id <unique id> --self <ip> --ips <root node ip>`

Once you've run the above command in every non-root node, just run the tuner and redpanda in each of them:

```
sudo systemctl start redpanda-tuner
sudo systemctl start redpanda
```

That's it! Your cluster should be up and running. You can check the logs with

`journalctl -f -u redpanda`

## Monitoring

Redpanda exposes a Prometheus metrics endpoint in `/metrics` in port 9644
(`curl localhost:9644/metrics`).

Our packages also ship with a systemd service, which executes periodically and reports resource usage and configuration data to Vectorized's metrics API. It is enabled by default, and the data is anonymous. If you'd like us to be able to identify your cluster's data, to be able to monitor it and alert you of possible issues, please set the `organization` (your company's domain) and `cluster_id` (usually your team's or project's name) configuration fields. For example:

```
rpk config set organization 'vectorized.io'
rpk config set cluster_id 'metrics'
```

To opt out of all metrics reporting, set `rpk.enable_usage_stats` to `false`:
```
rpk config set rpk.enable_usage_stats false
```

## Configuration

The redpanda configuration is by default loaded from and persisted to
`/etc/redpanda/redpanda.yaml`.

It is broadly divided into 2 sections, `redpanda` and `rpk`.
 
The `redpanda` section contains all the runtime configuration, such as the cluster member IPs, the node ID, data direcory, and so on.

The `rpk` section contains configuration related to tuning the machine that redpanda will run on.

Here's a sample of what the config file looks like.

```yaml
# organization and cluster_id help Vectorized identify your system.
organization: ""
cluster_id: ""
pid_file: "/var/lib/redpanda/pid"

redpanda:
  # The directory where the data will be stored. It must reside on an XFS
  # partition.
  data_directory: "/var/lib/redpanda/data"
  
  # An integer to identify the current node. Must be unique within the cluster.
  node_id: 1
  
  # The inter-node RPC server config.
  rpc_server:
    address: "0.0.0.0"
    port: 33145
  
  # The Kafka API config.
  kafka_api:
    address: "0.0.0.0"
    port: 9092
    
  # A list of `host` objects, holding known nodes' configuration. If empty, the
  # node will be considered a root node and become available upon startup.
  seed_servers:
    - node_id: 1
      host:
        address: "0.0.0.0"
        port: 33145

  # The admin API configuration
  admin:
    address: "0.0.0.0"
    port: 9644

rpk:
  # Available tuners. Set to true to enable, false to disable.
  
  # Setup NIC IRQs affinity, sets up NIC RPS and RFS, sets up NIC XPS, increases socket
  # listen backlog, increases the number of remembered connection requests, bans the
  # IRQ Balance service from moving distributed IRQs
  tune_network: true
  
  # Sets the preferred I/O scheduler for given block devices.
  # It can work using both the device name or a directory, in which the device
  # where directory is stored will be optimized. Sets either ‘none’ or ‘noop’ scheduler
  # if supported
  tune_disk_scheduler: true
  
  # Disables IOPS merging
  tune_disk_nomerges: true
  
  # Distributes IRQs across cores with the method deemed the most appropriate for the
  # current device type (i.e. NVMe)
  tune_disk_irq: true

  # Disables hyper-threading, sets the ACPI-cpufreq governor to ‘performance’. Additionaly
  # if system reboot is allowed: disables Intel P-States, disables Intel C-States,
  # disables Turbo Boost
  tune_cpu: true
  
  # Increases the number of allowed asynchronous IO events
  tune_aio_events: true
  
  # Syncs NTP
  tune_clocksource: true
  
  # Tunes the kernel to prefer keeping processes in-memory instead of swapping them out
  tune_swappiness: true
  
  # Enables memory locking
  enable_memory_locking: true
  
  # Installs a custom script to process coredumps and save them to the given directory.
  tune_coredump: true
  
  # The directory where all coredumps will be saved after they're processed.
  coredump_dir: "/var/lib/redpanda/coredump"
  
  # (Optional) The vendor, VM type and storage device type that redpanda will run on, in
  # the format <vendor>:<vm>:<storage>. This hints to rpk which configuration values it
  # should use for the redpanda IO scheduler.
  well_known_io: "aws:i3.xlarge:default"
```

## Rpk Commands

### `tune`
Run all (`rpk tune all`) or some (i.e. `rpk tune cpu network`) of the tuners available on rpk.

```
Usage:
  rpk tune <list_of_elements_to_tune> [flags]

Flags:
      --cpu-set string         Set of CPUs for tuner to use in cpuset(7) format if not specified tuner will use all available CPUs (default "all")
  -r, --dirs strings           List of *data* directories. or places to store data. i.e.: '/var/vectorized/redpanda/', usually your XFS filesystem on an NVMe SSD device
  -d, --disks strings          Lists of devices to tune f.e. 'sda1'
  -h, --help                   help for tune
  -m, --mode string            Operation Mode: one of: [sq, sq_split, mq]
  -n, --nic strings            Network Interface Controllers to tune
      --output-script string   If set tuners will generate tuning file that can later be used to tune the system
      --reboot-allowed         If set will allow tuners to tune boot paramters  and request system reboot
      --redpanda-cfg string    If set, pointed redpanda config file will be used to populate tuner parameters
      --timeout duration       The maximum time to wait for the tune processes to complete. The value passed is a sequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h' (default 10s)
```

### `start`
Start redpanda.

```
Usage:
  rpk start [flags]

Flags:
      --check                  When set to false will disable system checking before starting redpanda (default true)
  -h, --help                   help for start
      --install-dir string     Directory where redpanda has been installed
      --redpanda-cfg string     Redpanda config file, if not set the file will be searched forin default locations
      --timeout duration       The maximum time to wait for the checks and tune processes to complete. The value passed is a sequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h' (default 10s)
      --tune                   When present will enable tuning before starting redpanda
      --well-known-io string   The cloud vendor and VM type, in the format <vendor>:<vm type>:<storage type>
```

### `mode`
Enable a default configuration mode (development, production). See the **Rpk Modes** section below.

```
Usage:
  rpk mode {development, production} [flags]

Flags:
  -h, --help                  help for mode
      --redpanda-cfg string   Redpanda config file, if not set the file will be searched for in default locations
```

### `config set`
Edit the configuration.
```
Usage:
  rpk config set <key> <value> [flags]

Flags:
      --config string   Redpanda config file, if not set the file will be searched for in default location (default "/etc/redpanda/redpanda.yaml")
      --format string   The value format. Can be 'single', for single values such as '/etc/redpanda' or 100; and 'json', 'toml', 'yaml','yml', 'properties', 'props', 'prop', or 'hcl' when partially or completely setting config objects (default "single")
  -h, --help            help for set
```

### `iotune`
Measure filesystem performance and create IO configuration file.

```
Usage:
  rpk iotune [flags]

Flags:
      --directories strings   List of directories to evaluate
      --duration int          Duration of tests in seconds (default 30)
  -h, --help                  help for iotune
      --redpanda-cfg string   Redpanda config file, if not set the file will be searched for in default locations
      --timeout duration      The maximum time after --duration to wait for iotune to complete. The value passed is a sequence of decimal numbers, each with optional fraction and a unit suffix, such as '300ms', '1.5s' or '2h45m'. Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h' (default 1h0m0s)
```

## Well Known IO

`rpk` comes with a command called `iotune` which finds the optimal configuration for redpanda's IO scheduler, with the current storage device and processor. To get the most precise results, iotune should run for approximately 30 minutes - or 1800 seconds (i.e. `rpk iotune --duration 1800`).

However, rpk ships with a collection of optimal configurations for the recommended VM/ storage device type setups on AWS (support for GCP and Azure is coming soon). Thanks to this, when running redpanda on the recommended setups, you can just fire it up with no additional steps.

> It is still recommended to execute `rpk iotune` on custom setups. There's no need to run it every time, however; you can run it once for each setup that you will use to run a redpanda node, keep the results, and reuse them from there.

Upon startup, rpk will try to detect the current cloud and instance type via the vendor's metadata API, setting the correct iotune properties if the detected setup is a supported one.

If access to the metadata API isn't allowed from the instance, you can also hint the desired setup by passing the `--well-known-io` flag to `rpk start` with the cloud vendor, VM type and storage type surrounded by quotes and separated by colons (`:`):

```sh
rpk start --well-known-io 'aws:i3.xlarge:default'
```

It can also be specified in the redpanda YAML configuration file, under the
`rpk` object:

```yaml
rpk:
  well_known_io: 'aws:i3.large:default'
```

> If `well-known-io` is specified in the config file, and as a flag, the value
> passed with the flag will take precedence.

In the case where a certain cloud vendor, machine type or storage type isn't found, or if the metadata isn't available and no hint is given, `rpk` will print an error pointing out the issue and **continue** using the default values.

## Rpk Modes

To achieve the best performance possible, redpanda relies on interrupt coalescing, interrupt distribution, no-op IO scheduling, CPU frequency scaling (performance governor), NIC multi-queue setup, memory locking and real-time scheduling. However, enabling these features isn't always the best for a personal computer running multiple processes, each of which are always competing for memory, CPU and IO.

Because of this, redpanda enables them by default, but allows them to be disabled according to your needs. To do it easily, you can toggle on redpanda's Developer Mode, which disables these production settings so that you can still use it in your laptop.

Enabling Developer Mode is as simple as running

```sh
# You may need to run this command as root.
rpk mode developer
```

For runtime settings (that is, settings that are only effective while redpanda is running), it will conveniently disable any which may affect other processes' performance, by setting the corresponding fields in the `redpanda.yaml` configuration file to `false`.

For persistent session or system configurations, it will also undo any changes redpanda might have made in your machine (such as enabling real-time scheduling or disabling certain interrupt requests for some CPUs).

Likewise, if you'd like to re-enable the default, production settings, just run

```sh
# You may need to run this command as root.
rpk mode production
```

These commands are idempotent, so they're safe to run multiple times.

For runtime settings, you can also review and edit `redpanda.yaml` and enable or disable any of them. However, if you run `rpk mode` again, it might overwrite any changes you made in the affected fields.

If you switch modes or change any setting, you have to restart redpanda for the changes to take place.
