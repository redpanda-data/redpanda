---
title: Docker Quick Start Guide
order: 0
---
# Docker Quick Start Guide

Redpanda is a modern [streaming platform](/blog/intelligent-data-api/) for mission critical workloads.
With Redpanda you can get up and running with streaming quickly
and be fully compatible with the [Kafka ecosystem](https://cwiki.apache.org/confluence/display/KAFKA/Ecosystem).

This quick start guide can help you get started with Redpanda for development and testing purposes.
For production or benchmarking, setup a [production deployment](/docs/production-deployment).

## Set up network and persistent volumes

First we need to set up a bridge network so that the Redpanda instances can communicate with each other
but still allow for the Kafka API to be available on the localhost.
We'll also create the persistent volumes that let the Redpanda instances keep state during instance restarts.

```
docker network create -d bridge redpandanet && \
docker volume create redpanda1 && \
docker volume create redpanda2 && \
docker volume create redpanda3
```

## Start Redpanda nodes

We then need to start the nodes for the Redpanda cluster.

```
docker run -d \
--name=redpanda-1 \
--hostname=redpanda-1 \
--net=redpandanet \
-p 9092:9092 \
-v "redpanda1:/var/lib/redpanda/data" \
vectorized/redpanda start \
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

docker run -d \
--name=redpanda-2 \
--hostname=redpanda-2 \
--net=redpandanet \
-p 9093:9093 \
-v "redpanda2:/var/lib/redpanda/data" \
vectorized/redpanda start \
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

docker run -d \
--name=redpanda-3 \
--hostname=redpanda-3 \
--net=redpandanet \
-p 9094:9094 \
-v "redpanda3:/var/lib/redpanda/data" \
vectorized/redpanda start \
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

Now you can run `rpk` on one of the containers to interact with the cluster:

```
docker exec -it redpanda-1 rpk api status
```

The output of the status command looks like:

```
  Redpanda Cluster Status

  0 (127.0.0.1:9092)       (No partitions)
  1 (127.0.0.1:9093)       (No partitions)
  2 (127.0.0.1:9094)       (No partitions)
```

## Do some streaming

Here are the basic commands to produce and consume streams:

1. Create a topic. We'll call it "twitch_chat":

    ```
    rpk topic create twitch_chat
    ```

1. Produce messages to the topic:

    ```
    rpk topic produce twitch_chat
    ```

    Type text into the topic and press Ctrl + D to seperate between messages.

    Press Ctrl + C to exit the produce command.

1. Consume (or read) the messages in the topic:

    ```
    rpk topic consume twitch_chat
    ```
    
    Each message is shown with its metadata, like this:
    
    ```
    {
    "message": "How do you stream with Redpanda?\n",
    "partition": 0,
    "offset": 1,
    "timestamp": "2021-02-10T15:52:35.251+02:00"
    }
    ```

You've just installed Redpanda and done streaming in a few easy steps. 

## Clean Up

When you are finished with the cluster, you can shutdown and delete the containers with:

```
docker stop redpanda-1 redpanda-2 redpanda-3
docker rm redpanda-1 redpanda-2 redpanda-3
```

You can delete the volumes that hold the data that was stored in the cluster with:

```
docker volume rm redpanda1 redpanda2 redpanda3
```

You can delete the network that you created with:

```
docker network rm redpandanet
```

## What's Next?

- Our [FAQ](/docs/faq) page shows all of the clients that you can use to do streaming with Redpanda.
    (Spoiler: Any Kafka-compatible client!)
- Get a multi-node cluster up and running using [`rpk container`](/docs/guide-rpk-container).
- Use the [Quick Start Docker Guide](/docs/quick-start-docker) to try out Redpanda using Docker.
- Want to setup a production cluster? Check out our [Production Deployment Guide](/docs/production-deployment).
