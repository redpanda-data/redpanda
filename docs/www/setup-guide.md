---
title: Redpanda Setup Guide
order: 0
---
# Redpanda Setup Guide

Redpanda is a Kafka replacement for mission critical systems. This guide will
lead you through initial setup and testing.


# Quick Start - 60 Seconds 

The following oneliner should get you up and running on your linux laptop in seconds.
It is designed to let you experiment with `redpanda` locally to get a feel for the system.
If you are planning on benchmarking locally, make sure to check out our
**Single Node Production Setup** section below

#### On Fedora/RedHat Systems {#qs-fedora}

```
curl -s https://{{client_token}}:@packagecloud.io/install/repositories/vectorizedio/v/script.rpm.sh \
    | sudo bash && sudo yum install redpanda -y && sudo systemctl start redpanda
```

#### On Debian Systems {#qs-debian}

```
curl -s https://{{client_token}}:@packagecloud.io/install/repositories/vectorizedio/v/script.deb.sh \
    | sudo bash && sudo apt install redpanda -y && sudo systemctl start redpanda
```

# Single Node Production Setup

Requirements:

- XFS Must be the filesystem for the data directory (/var/lib/redpanda/data)
- Ensure port 9092 - Kafka API - is open and can be reached

## Step 1: Install the binary

#### On Fedora/RedHat Systems {#single-node-fedora}

```
curl -s https://{{client_token}}:@packagecloud.io/install/repositories/vectorizedio/v/script.rpm.sh \ 
    | sudo bash && sudo yum install redpanda -y
```

#### On Debian Systems {#single-node-debian}

```
curl -s https://{{client_token}}:@packagecloud.io/install/repositories/vectorizedio/v/script.deb.sh \
    | sudo bash && sudo apt install redpanda -y
```


## Step 2: Tune the hardware and Linux Kernel

#### Automatically optimize the hardware

```
sudo rpk tune all
```

#### Optionally benchmark your SSD

`rpk` comes with a program to allow you to test the actual hardware you are about to use - note you
only need to run it once. For reference, a decent local NVMe SSD should yield around
1GB/s sustained writes. `iotune` will capture SSD wear and tear and give accurate measurements
of what your hardware is actually capable of delivering. It is recommended you run this before benchmarking. 
If you are on AWS, GCP or Azure, creating a new instance and upgrading to an image with a recent
Linux Kernel version is often the easiest way to work around bad devices.

```
sudo rpk iotune # takes 10mins
```

## Step 3: Profit!

```
sudo systemctl start redpanda
```

# Multi Node Production Setup

Running redpanda in a multi-node setup requires only one extra step per node. No
awk, grep or `command-line-fu` needed.

Requirements:

- XFS Must be the filesystem for the data directory (`/var/lib/redpanda/data`)
- The following ports must be open:
  - `33145` - Internal RPC Port - ensure your firewall allows node-to-node
    communication over TCP on this port
  - `9092` - Kafka API Port
  - `9644` - Prometheus & HTTP Admin port
- Allow outbound traffic to
  <code>[https://m.rp.vectorized.io](https://m.rp.vectorized.io)</code> - allows
  us to optimize code paths based on production use - see
  <strong>Autotuning</strong> section.

## Step 1: Install the binary

#### On Fedora/RedHat Systems {#multi-node-fedora}

```
curl -s https://{{client_token}}:@packagecloud.io/install/repositories/vectorizedio/v/script.rpm.sh \
    | sudo bash && sudo yum install redpanda -y
```

#### On Debian Systems {#multi-node-debian}

```
curl -s https://{{client_token}}:@packagecloud.io/install/repositories/vectorizedio/v/script.deb.sh \
    | sudo bash && sudo apt install redpanda -y
```

## Step 2: Start the root node

To get started, choose one node in your cluster to be the root node. The root
node will start as a standalone node, and every other one will join it, forming
a cluster along the way.

For the root node we’ll choose 0 as its ID. --self tells the node which
interface address to bind to. Usually you want that to be its private IP.

```
sudo rpk config bootstrap --id 0 --self <ip> && \
sudo systemctl start redpanda-tuner redpanda
```

## Step 3: Start the other nodes

For every other node, we just have to choose a unique integer id for it and let
it know where to reach the root node.
```
sudo rpk config bootstrap --id <unique id> \
--self <private ip>                        \
--ips <root node ip> &&                    \
sudo systemctl start redpanda-tuner redpanda
```

You can verify that the cluster is up and running by checking the logs:

```
journalctl -u redpanda
```
