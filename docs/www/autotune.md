---
title: Performance tuning
order: 1
---
# Performance tuning

Thank you for making it this far! Until this point we have not explained how
anything works or why. Before you go into production we’d like to highlight the
system components so you understand what is running on your hardware.

It’s worth mentioning - as you might have noticed by now - you’ll always
interact with redpanda through `rpk`. `rpk` is the Redpanda Keeper; a command
line interface to automate all tasks related to managing, running and upgrading
your system. We also leverage `systemd` to make it even simpler to run and
operate redpanda as a service. This means you’ll also find yourself using
`systemctl` to start and stop `redpanda`, as well as checking its status. You
can also manage, filter and rotate the logs emitted by redpanda through
journalctl.

```
rpk --help             # interact with the server & machine
journalctl -u redpanda # see logs
```

## Disk

To get the best IO performance, we recommend the data directory
(/var/lib/redpanda/data) to reside on an XFS partition in a local NVMe SSD -
redpanda can drive your SSD at maximum throughput at all times. Using networked
block devices is strongly discouraged, because of their inherent performance
limitations.

Furthermore, redpanda relies on XFS due to its use of sparse filesystem support
to flush concurrent, non-overlapping pages. Although other filesystems might
work, they may have limitations that prevent you from getting the most value out
of your hardware. For multi-disk setups we recommend using Raid-0 with XFS on
top. Future releases will manage multi-disk virtualization without user
involvement.

While monitoring, you might notice that the filesystem file sizes might jump
around. This is expected behavior as we use internal heuristics to expand the
filesystem metadata eagerly when we determine it would improve performance for a
sequence of operations, to amortize the cost of synchronization events. We note
that `redpanda` uses DMA (Direct Memory Access) for all its disk IO.

## Network

Modern NICs can drive multi gigabit traffic to hosts. rpk probes the hardware
(taking into account the number of CPUs, etc) and automatically chooses the best
setting to drive high throughput traffic to the machine. The modes are all but
cpu0, cpu0 + Hyper Thread sibling, or distributed across all cores, in addition
to other settings like backlog and max sockets, regardless if the NIC is bonded
or not. The user is never aware of any of these low level settings, and in most
production scenarios it is usually always distributed across all cores. This is
to distribute the cost of interrupt processing evenly among all cores.

## CGROUPS

To be able to run at peak performance for extended periods, we leverage cgroups
to isolate the redpanda processes, shielding them from “noisy neighbors”:
processes running alongside redpanda which demand sharing resources that
adversely affect performance.

We leverage `systemd` slices, instructing the kernel to strongly prefer evicting
other processes before evicting our process’ memory, as well as to reserve IO
quotas and CPU time so that even in some adversarial situations we still deliver
predictable latency and throughput to end users.

## CPU

Oftentimes, the default CPU configuration is prioritized for end-user-typical
use cases such as non-cpu-intensive desktop applications and optimizing power
usage. Redpanda disables all power-saving modes and ensures that the CPU is
configured for predictable latency at all times. We designed redpanda to drive
machines around ~90% utilization and still give the user predictable low latency
results.

## Memory

Swapping is prevented so that Redpanda is never swapped out of memory. By
design, `redpanda` allocates all the available memory (aside for some small OS
reservation) upfront, partitioning the allocated memory between all cores and
pinning such memory to the specified NUMA domain (specific CPU socket). We do
this to have predictable memory allocations without having to go to the system
memory, ensuring predictable latency.
