---
title: Cloud IO optimization
order: 3
---
# Cloud IO optimization

Redpanda relies on its own disk IO scheduler, and by default tells the kernel to
use the `noop` scheduler. To give the users near optimal performance by default,
`rpk` comes with an embedded database of IO settings for well-known cloud
computers, which are specific combinations of CPUs, SSD types, and VM sizes. It
is not the same to run software on 4 VCPUs than it is to run on an EC2 i3.metal
with 96 physical cores. Often,when trying to scale rapidly to meet demands,
product teams will not have the time to measure IO throughput and latency before
starting every new instance (via `rpk iotune`) and instead need resources right
now. To meet this demand, redpanda will attempt to predict the best known
settings for VM cloud types.

We still encourage users to run `rpk iotune` for production workloads, but we’ll
do our best to start with near optimal settings for popular machines types we
have measured. It’s important to mention that this doesn’t need to be done each
time redpanda is started. rpk iotune’s output parameters are written to a file
which can be saved and reused in nodes running on the same type of hardware.

Currently, we only have well-known-types for AWS (GCP and Azure VM types support
is coming soon). Upon startup, rpk will try to detect the current cloud and
instance type via the cloud’s metadata API, setting the correct iotune
properties if the detected setup is known apriori.

If access to the metadata API isn’t allowed from the instance, you can also hint
the desired setup by passing the --well-known-io flag to rpk start with the
cloud vendor, VM type and storage type surrounded by quotes and separated by
colons:

```
rpk start --well-known-io 'aws:l3.xlarge:default'
```

It can also be specified in the redpanda YAML configuration file, under the rpk
object:

```
rpk:
  well_known_io: 'gcp:c2-standard-16:nvme'
```

If well-known-io is specified in the config file, and as a flag, the value
passed with the flag will take precedence.

In the case where a certain cloud vendor, machine type or storage type isn’t
found, or if the metadata isn’t available and no hint is given, rpk will print a
warning pointing out the issue and continue using the default values.
