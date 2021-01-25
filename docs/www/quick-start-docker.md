---
title: Docker Quick Start Guide
order: 0
---
# Docker Quick Start Guide

Redpanda is a modern streaming platform for mission critical workloads.
Redpanda is also fully API compatible Kafka allowing you to make full use
of the Kafka ecosystem.

This quick start guide to intended to help you get started with Redpanda
for development and testing purposes. For production deployments or
performance testing please see our
[Production Deployment](production-deployment) for more information.

## Step 1: Create a bridge network

The bridge network will allow for the Redpanda instances to communicate
with one another but still allow for the Kafka API to be availbale on
the localhost.

```
docker network create -d bridge redpandanet
```

## Step 2: Start Redpanda

First we need to create the persistent volumes to be used by the Redpanda
instances. This will allow for us to be able to keep state across restarts.

```
docker volume create redpanda1;
docker volume create redpanda2;
docker volume create redpanda3;
```

We then need to start the first node of the Redpanda cluster.

```
docker run -d \
--name=redpanda-1 \
--hostname=redpanda-1 \
--net=redpandanet \
-p 9092:9092 \
-v "redpanda1:/var/lib/redpanda/data" \
vectorized/redpanda:v20.12.9 start \
--smp 1  \
--memory 1G  \
--reserve-memory 0M \
--overprovisioned \
--node-id 0 \
--check=false \
--kafka-addr 0.0.0.0:9092 \
--advertise-kafka-addr 127.0.0.1:9092 \
--rpc-addr 0.0.0.0:33145 \
--advertise-rpc-addr redpanda-1:33145
```

We then need to bring up the second and third node of the Redpanda cluster.

```
docker run -d \
--name=redpanda-2 \
--hostname=redpanda-2 \
--net=redpandanet \
-p 9093:9093 \
-v "redpanda2:/var/lib/redpanda/data" \
vectorized/redpanda:v20.12.9 start \
--smp 1  \
--memory 1G  \
--reserve-memory 0M \
--overprovisioned \
--node-id 1 \
--seeds "redpanda-1:33145+0" \
--check=false \
--kafka-addr 0.0.0.0:9093 \
--advertise-kafka-addr 127.0.0.1:9093 \
--rpc-addr 0.0.0.0:33146 \
--advertise-rpc-addr redpanda-2:33146
```

```
docker run -d \
--name=redpanda-3 \
--hostname=redpanda-3 \
--net=redpandanet \
-p 9094:9094 \
-v "redpanda3:/var/lib/redpanda/data" \
vectorized/redpanda:v20.12.9 start \
--smp 1  \
--memory 1G  \
--reserve-memory 0M \
--overprovisioned \
--node-id 2 \
--seeds "redpanda-1:33145+0" \
--check=false \
--kafka-addr 0.0.0.0:9094 \
--advertise-kafka-addr 127.0.0.1:9094 \
--rpc-addr 0.0.0.0:33147 \
--advertise-rpc-addr redpanda-3:33147
```

## Using RPK

Now you can run `rpk` on one of the containers to interact with the cluster:

```
docker exec -it redpanda-1 rpk api status
```

You should see output similar to the following:

```
  Redpanda Cluster Status

  0 (127.0.0.1:9092)       (No partitions)
  1 (127.0.0.1:9093)       (No partitions)
  2 (127.0.0.1:9094)       (No partitions)
```

From here you can create topics:

```
docker exec -it redpanda-1 rpk api topic create panda
```

Then send some data to the topic:

```
docker exec -it redpanda-1 rpk api produce panda
```

You will be prompted to enter in some text and then hit `CTRL + D` to send.
Once you've sent a message you can then consume the message: 

```
docker exec -it redpanda-1 rpk api consume panda
```

## Clean Up

When you are finished with the cluster you can shutdown with the following
commands:

```
docker stop redpanda-1 redpanda-2 redpanda-3
```

```
docker rm redpanda-1 redpanda-2 redpanda-3
```

If you wish to remove all the data that was stored in the cluster, you
can also delete the volumes:

```
docker volume rm redpanda1 redpanda2 redpanda3
```

## What's Next?

- Check out our [FAQ](faq)
- Want to setup a production cluster? Check out our [Production Deployment](production-deployment) Guide.
  