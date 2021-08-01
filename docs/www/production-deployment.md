---
title: Deploying Redpanda for production
order: 0
---

# Deploying for production

This guide will take you through what is needed to setup a production cluster
of Redpanda.

If you just want to try out Redpanda, check out our Getting Started Guides for
[Linux](/docs/quick-start-linux), [MacOS](/docs/quick-start-macos),
[Docker](/docs/quick-start-docker), or [Kubernetes](/docs/quick-start-kubernetes).

## Prepare infrastructure

For the best performance, we need to provision the hardware according to these hardware requirements:

- XFS for the data directory of Redpanda (/var/lib/redpanda/data)
- A kernel that is at least 3.10.0-514, but a 4.18 or newer kernel is preferred
- Local NVMe, RAID-0 when using multiple disks
- 2GB of memory per core
- TCP ports:
  - `33145` - Internal RPC Port
  - `9092` - Kafka API Port
  - `8082` - Pandaproxy Port
  - `9644` - Prometheus and HTTP admin port

If you want, you can [use Terraform to deploy Redpanda](/docs/production-deployment-automation).

## Install Redpanda

After the hardware is provisioned, install Redpanda and configure it for production use.

You can also install Redpanda using an [Ansible playbook](/docs/production-deployment-automation).

### Step 1: Install the binary

On Fedora/RedHat Systems:

```
curl -1sLf 'https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.rpm.sh' | \
sudo -E bash && sudo yum install redpanda -y
```

On Debian Systems:

```
curl -1sLf 'https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' | \
sudo -E bash && sudo apt install redpanda -y
```

### Step 2: Set Redpanda production mode

By default Redpanda is installed in **development** mode, which turns off hardware optimization.
To enable hardware optimization, set Redpanda to run in **production** mode:

```
sudo rpk redpanda mode production
```

We then need to tune the hardware, which can be done by running the following
on each node:

```
sudo rpk redpanda tune all
```

> **_Optional: Benchmark your SSD_**
>
> On taller machines we recommend benchmarking your SSD. This can be done
> with `rpk iotune`. You only need to run this once. For reference, a decent
> local NVMe SSD should yield around 1GB/s sustained writes.
> `rpk iotune` will capture SSD wear and tear and give accurate measurements
> of what your hardware is actually capable of delivering. It is recommended
> you run this before benchmarking.
>
> If you are on AWS, GCP or Azure, creating a new instance and upgrading to
> an image with a recent Linux Kernel version is often the easiest way to
> work around bad devices.
>
> ```
> sudo rpk iotune # takes 10mins
> ```

### Step 3: Configure and start the root node

Now that the software is installed we need to configure it. The first step is
to setup the root node. The root node will start as a standalone node, and
every other one will join it, forming a cluster along the way.

For the root node we’ll choose 0 as its ID. `--self` tells the node which interface address to bind to. Usually you want that to be its private IP.

```
sudo rpk config bootstrap --id 0 --self <ip> && \
sudo systemctl start redpanda-tuner redpanda
```

### Step 4: Configure and start the other nodes

For every other node, we just have to choose a unique integer id for it and let
it know where to reach the root node.

```
sudo rpk config bootstrap --id <unique id> \
--self <private ip>                        \
--ips <root node ip> &&                    \
sudo systemctl start redpanda-tuner redpanda
```

### Step 5: Verify the installation

You can verify that the cluster is up and running by checking the logs:

```
journalctl -u redpanda
```

You should also be able to create a topic with the following command:

```
rpk topic create panda
```
