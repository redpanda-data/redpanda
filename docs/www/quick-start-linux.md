---
title: Linux Quick Start Guide
order: 0
---
# Linux Quick Start Guide

Redpanda is a modern streaming platform for mission critical workloads. Redpanda
is also fully API compatible Kafka allowing you to make full use of the Kafka ecosystem.

This quick start guide to intended to help you get started with Redpanda for
development and testing purposes. For production deployments or performance
testing please see our [Production Deployment](production-deployment)
for more information.

## Installation

The first step is to install either the RPM or DEB package of Redpanda.

### On Fedora/RedHat Systems

```
curl -1sLf 'https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.rpm.sh' | \
sudo -E bash && sudo yum install redpanda -y && sudo systemctl start redpanda
```

### On Debian/Ubuntu Systems

```
curl -1sLf 'https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' | \
sudo -E bash && sudo apt install redpanda -y && sudo systemctl start redpanda
```

## Getting Started

Now that Redpanda is installed and running we can either setup a single
node cluster of Redpanda or setup a local multi-node cluster using docker.

### Single Node Deployment

First we should check the status of the redpanda service as the install step
above should have started Redpanda.

```
sudo systemctl status redpanda
```

The output should look like the following:

```
● redpanda.service - Redpanda, the fastest queue in the West.
     Loaded: loaded (/lib/systemd/system/redpanda.service; enabled; vendor preset: enabled)
     Active: active (running) since Wed 2020-12-02 16:01:03 PST; 18s ago
```

You now have a running Redpanda instance!

### Local Multi Node Deployment

The simplest way to get a multi node cluster up and running is by using
`rpk container`. You can follow the
[rpk Container Guide](guide-rpk-container). If you want a more manual
approach you can check out the
[Quick Start Docker Guide](quick-start-docker).

## What's Next?

- Check out our [FAQ](faq)
- Want to setup a production cluster? Check out our [Production Deployment](production-deployment) Guide.
  