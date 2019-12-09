# Redpanda Documentation

{{client_name}}, {{date}}

## Installation
To get started with redpanda, first set up the package repository:

For RPM-based Linux distributions:

```
curl -s https://{{master_token}}:@packagecloud.io/install/repositories/vectorizedio/v/script.rpm.sh | sudo bash
```

For Debian-based distributions:

```
curl -s https://{{master_token}}:@packagecloud.io/install/repositories/vectorizedio/v/script.deb.sh | sudo bash
```

Then, install the redpanda package

```
sudo yum install -y redpanda
```

Or for Debian-based:

```
sudo apt install -y redpanda
```

## Running Redpanda
To make sure redpanda runs as fast as it can, first run the tuner
service, which will autoconfigure your VM to guarantee the best
performance.

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

## Redpanda & Rpk
Redpanda has a CLI tool called rpk, the Redpanda Keeper. As a user, you will always
interact with it to auto-tune the OS and hardware settings, start redpanda (when not run
as a systemd service) and enable configuration modes (described below).

## Configuration
The redpanda configuration is loaded from and persisted to `/etc/redpanda/redpanda.conf`.

It is divided into 2 sections, `redpanda` and `rpk`.
 
The `redpanda` section contains all the runtime configuration, such as the cluster
member IPs, the node ID, data direcory, and so on.

The `rpk` section contains configuration related to tuning the machine that redpanda will
run on.

Here's a sample of what the config file looks like.

```yaml
redpanda:
  # The directory where the data will be stored. It must reside on an XFS
  # partition.
  data_directory: "/var/lib/redpanda/data"
  
  # An integer to identify the current node. Must be unique within the cluster.
  node_id: 1
  
  # The inter-node RPC server config.
  rpc_server:
    address: "127.0.0.1"
    port: 33145
  
  # The Kafka API config.
  kafka_api:
    address: "127.0.0.1"
    port: 9092
    
  # A list of `host` objects, holding known nodes' configuration.
  seed_servers:
    - node_id: 1
      host:
        address: "127.0.0.1"
        port: 33145

  # The admin API configuration
  admin:
    address: "127.0.0.1"
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

### Well Known IO
`rpk` comes with a command called `iotune` which finds the optimal configuration
for redpanda's IO scheduler, with the current storage device and processor. To
get the most precise results, iotune should usually run for approximately 30
minutes - or 1800 seconds (i.e. `rpk iotune --duration 1800`).

However, rpk ships with a matrix of optimal configurations for the
recommended VM/ storage device type setups on AWS (currently, GCP and Azure VM types
support is coming soon), GCP and Azure. Thanks to this, when running redpanda on the
recommended setups, you can just fire it up with no additional steps.

> It is still recommended to execute `rpk iotune` on custom setups. There's no need
> to run it every time, however: you can run it once for each setup that you
> will use to run a redpanda node, keep the results, and reuse them from there.

Upon statup, rpk will try to detect the current cloud and instance type via the
different vendors' metadata APIs, setting the correct iotune properties if the
detected setup is a supported one.

If access to the metadata API isn't allowed from the instance, you can also hint
the desired setup by passing the `--well-known-io` flag to `rpk start` with the
cloud vendor, VM type and storage type surrounded by quotes and separated by
colons (`:`):

```sh
rpk start --well-known-io 'aws:l3.xlarge:default'
```

It can also be specified in the redpanda YAML configuration file, under the
`rpk` object:

```yaml
rpk:
  well_known_io: 'gcp:c2-standard-16:nvme'
```

> If `well-known-io` is specified in the config file, and as a flag, the value
> passed with the flag will take precedence.

In the case where a certain cloud vendor, machine type or storage type isn't
found, or if the metadata isn't available and no hint is given, `rpk` will
print an error pointing out the issue and **continue** using the default values.

### Rpk Modes
To achieve the best performance possible, redpanda relies on interrupt coalescing, interrupt
distribution, no-op IO scheduling, CPU frequency scaling (performance governor), NIC
multi-queue setup, memory locking and real-time scheduling. However, enabling these features
isn't always the best for a personal computer running multiple processes, each of which are
always competing for memory, CPU and IO.

Because of this, redpanda enables them by default, but allows them to be disabled according to
your needs. To do it easily, you can toggle on redpanda's Developer Mode, which disables these
production settings so that you can still use it in your laptop.

Enabling Developer Mode is as simple as running

```sh
# You may need to run this command as root.
rpk mode developer
```

For runtime settings (that is, settings that are only effective while redpanda is running),
it will conveniently disable any which may affect other processes' performance, by setting the
corresponding fields in the `redpanda.yaml` configuration file to `false`.

For persistent session or system configurations, it will also undo any changes redpanda might
have made in your machine (such as enabling real-time scheduling or disabling certain interrupt
requests for some CPUs).

Likewise, if you'd like to re-enable the default, production settings, just run

```sh
# You may need to run this command as root.
rpk mode production
```

These commands are idempotent, so they're safe to run multiple times.

For runtime settings, you can also review and edit `redpanda.yaml` and enable or disable
any of them. However, if you run `rpk mode` again, it might overwrite any changes you
made in the affected fields.

If you switch modes or change any setting, you have to restart redpanda for the changes
to take place.
