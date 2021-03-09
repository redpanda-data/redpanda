---
title: rpk Modes
order: 4
---
# rpk Modes

Usability is paramount to us. We know the delta from downloading and running
something from the internet and running in production is large. `redpanda`
relies on interrupt coalescing, interrupt distribution, noop IO scheduling, CPU
frequency scaling (performance governor), NIC multi-queue setup, memory locking
and real-time scheduling. Not to mention measuring the IO throughput to disk,
etc and keeping them updated with every release.

While these settings exist to give the users predictable low latency, high
throughput, high resource utilization, etc, they are not good settings for
developer’s laptops. We keep a set of auto-tunables in big groups:
`rpk mode developer` and `rpk mode production.`

These commands are idempotent, they’re safe to run multiple times. Some settings
might require restart of redpanda, others are injected into the runtime settings
of the kernel and can take effect immediately **if followed** by a
`rpk tune all.` Users can also review and edit redpanda.yaml and enable or
disable any individual setting manually, or to enable experimental flags.
