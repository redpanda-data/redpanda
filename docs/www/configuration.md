---
title: Configuration
order: 5
---
# Configuration

The redpanda configuration is by default loaded from and persisted to
`/etc/redpanda/redpanda.yaml`. It is broadly divided into 2 sections, `redpanda`
and `rpk`.

The `redpanda` section contains all the runtime configuration, such as the
cluster member IPs, the node ID, data directory, and so on. The `rpk` section
contains configuration related to tuning the machine that redpanda will run on.

Here’s a sample of what the config file looks like.

```yaml
# organization and cluster_id help Vectorized identify your system.
organization: ""
cluster_id: ""

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

  # Target quota byte rate (bytes/ sec) - 64MB default
  target_quota_byte_rate: 64000000

  # How often (in ms) the background compaction is triggered
  log_compaction_interval: 600000

  # Max bytes per partition on disk before triggering a compaction
  retention_bytes: null

  # delete segments older than this - default 1 week
  delete_retention_ms: 604800000000,

rpk:
  # Available tuners. Set to true to enable, false to disable.

  # Setup NIC IRQs affinity, sets up NIC RPS and RFS, sets up NIC XPS, increases socket
  # listen backlog, increases the number of remembered connection requests, bans the
  # IRQ Balance service from moving distributed IRQs
  tune_network: true

  # Sets the preferred I/O scheduler for given block devices.
  # It can work using both the device name or a directory, in which the device
  # where directory is stored will be optimized. Sets either 'none' or 'noop' scheduler
  # if supported
  tune_disk_scheduler: true

  # Disables IOPS merging
  tune_disk_nomerges: true

  # Distributes IRQs across cores with the method deemed the most appropriate for the
  # current device type (i.e. NVMe)
  tune_disk_irq: true

  # Disables hyper-threading, sets the ACPI-cpufreq governor to 'performance'. Additionaly
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
