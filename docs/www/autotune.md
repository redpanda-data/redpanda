---
title: Designed for performance
order: 1
---

# Designed for performance

Great news! Redpanda comes with an autotuner that detects the optimal settings for your hardware.
To get the best performance for your hardware, set Redpanda to [production mode](https://vectorized.io/docs/production-deployment).
In production mode, Redpanda identifies your hardware configuration and tunes itself to give you the best performance.

The performance settings listed here are just for general reference.

<!-- It’s worth mentioning - as you might have noticed by now - you’ll always
interact with redpanda through `rpk`. `rpk` is the Redpanda Keeper, a command-line interface to automate all tasks related to managing, running, and upgrading
redpanda. We also leverage `systemd` to make it even simpler to run and
operate redpanda as a service. This means you’ll also find yourself using
`systemctl` to start and stop `redpanda`, as well as checking its status. You
can also manage, filter, and rotate the logs created by redpanda through
journalctl.

```
rpk --help             # interact with the server and service
journalctl -u redpanda # see logs
``` -->

## Disk

Redpanda uses DMA (Direct Memory Access) for all its disk IO. To get the
best IO performance, we recommend the you place the data directory
(/var/lib/redpanda/data) on an XFS partition in a local NVMe SSD. Redpanda can
drive your SSD at maximum throughput at all times. Redpanda relies on XFS due
to its use of sparse file system support to flush concurrent, non-overlapping pages.
Although other file systems might work, they may have limitations that prevent
you from getting the most value out of your hardware.

> **_Note:_** We recommend not using networked block devices because of their inherent performance limitations.

For multi-disk setups we recommend using Raid-0 with XFS on
top. Future releases will manage multi-disk virtualization without user
involvement.

While monitoring, you might notice that the file system file sizes might jump
around. This is expected behavior as we use internal heuristics to expand the
file system metadata eagerly when we determine it would improve performance for a
sequence of operations or to amortize the cost of synchronization events.

## Network

Modern NICs can drive multi-gigabit traffic to hosts. `rpk` probes the hardware
(taking into account the number of CPUs, etc) and automatically chooses the best
setting to drive high throughput traffic to the machine. The modes are all but
cpu0, cpu0 + Hyper Thread sibling, or distributed across all cores, in addition
to other settings like backlog and max sockets, regardless if the NIC is bonded
or not. The user is never aware of any of these low level settings, and in most
production scenarios it is usually distributed across all cores. This is
to distribute the cost of interrupt processing evenly among all cores.

## CGROUPS

To run at peak performance for extended periods, we leverage cgroups
to isolate the Redpanda processes. This shields Redpanda processes from
“noisy neighbors”, processes running alongside redpanda which demand sharing
resources that adversely affect performance.

We also leverage `systemd` slices. We instruct the kernel to strongly prefer
evicting other processes before evicting our process’ memory and to reserve IO
quotas and CPU time. This way, even when other processes are competing for resources,
we still deliver predictable latency and throughput to end users.

## CPU

Frequently, the default CPU configuration is prioritized for typical end-user
use cases, such as non-cpu-intensive desktop applications and optimizing power
usage. Redpanda disables all power-saving modes and ensures that the CPU is
configured for predictable latency at all times. We designed Redpanda to drive
machines around ~90% utilization and still give the user predictable low latency
results.

## Memory

Swapping is prevented so that Redpanda is never swapped out of memory. By
design, `redpanda` allocates nearly all of the available memory upfront,
partitioning the allocated memory between all cores and pinning such memory
to the specified NUMA domain (specific CPU socket). This makes sure that we have predictable memory allocations and provides predictable latency.
