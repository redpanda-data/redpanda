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

## Get your cluster ready

To get a cluster ready for streaming, either run a single docker container with Redpanda running or a cluster of 3 containers.

> **_Note_** - You can also use [`rpk container`](/docs/guide-rpk-container) to run Redpanda in containers
    without having to interact with Docker at all.

### Single command for a 1-node cluster

With a 1-node cluster you can test out a simple implementation of Redpanda.

**_Notes_**:

- `--overprovisioned` is used to accomodate docker resource limitations.
- `--pull=always` makes sure that you are always working with the latest version.

```bash
docker run -d --pull=always --name=redpanda-1 --rm \
-p 9092:9092 \
docker.vectorized.io/vectorized/redpanda:latest \
start \
--overprovisioned \
--smp 1  \
--memory 1G \
--reserve-memory 0M \
--node-id 0 \
--check=false
```

You can do some [simple topic actions](#Do-some-streaming) to do some streaming.
Otherwise, just point your [Kafka-compatible client](/docs/faq/#What-clients-do-you-recommend-to-use-with-Redpanda) to 127.0.0.1:9092.

### Set up a 3-node cluster

To test out the interaction between nodes in a cluster, set up a Docker network with 3 containers in a cluster.

#### Create network and persistent volumes

First we need to set up a bridge network so that the Redpanda instances can communicate with each other
but still allow for the Kafka API to be available on the localhost.
We'll also create the persistent volumes that let the Redpanda instances keep state during instance restarts.

```bash
docker network create -d bridge redpandanet && \
docker volume create redpanda1 && \
docker volume create redpanda2 && \
docker volume create redpanda3
```

#### Start Redpanda nodes

We then need to start the nodes for the Redpanda cluster.

```bash
docker run -d \
--pull=always \
--name=redpanda-1 \
--hostname=redpanda-1 \
--net=redpandanet \
-p 8082:8082 \
-p 9092:9092 \
-v "redpanda1:/var/lib/redpanda/data" \
docker.vectorized.io/vectorized/redpanda start \
--smp 1  \
--memory 1G  \
--reserve-memory 0M \
--overprovisioned \
--node-id 0 \
--check=false \
--pandaproxy-addr 0.0.0.0:8082 \
--advertise-pandaproxy-addr 127.0.0.1:8082 \
--kafka-addr 0.0.0.0:9092 \
--advertise-kafka-addr 127.0.0.1:9092 \
--rpc-addr 0.0.0.0:33145 \
--advertise-rpc-addr redpanda-1:33145 &&

docker run -d \
--pull=always \
--name=redpanda-2 \
--hostname=redpanda-2 \
--net=redpandanet \
-p 9093:9093 \
-v "redpanda2:/var/lib/redpanda/data" \
docker.vectorized.io/vectorized/redpanda start \
--smp 1  \
--memory 1G  \
--reserve-memory 0M \
--overprovisioned \
--node-id 1 \
--seeds "redpanda-1:33145" \
--check=false \
--pandaproxy-addr 0.0.0.0:8083 \
--advertise-pandaproxy-addr 127.0.0.1:8083 \
--kafka-addr 0.0.0.0:9093 \
--advertise-kafka-addr 127.0.0.1:9093 \
--rpc-addr 0.0.0.0:33146 \
--advertise-rpc-addr redpanda-2:33146 &&

docker run -d \
--pull=always \
--name=redpanda-3 \
--hostname=redpanda-3 \
--net=redpandanet \
-p 9094:9094 \
-v "redpanda3:/var/lib/redpanda/data" \
docker.vectorized.io/vectorized/redpanda start \
--smp 1  \
--memory 1G  \
--reserve-memory 0M \
--overprovisioned \
--node-id 2 \
--seeds "redpanda-1:33145" \
--check=false \
--pandaproxy-addr 0.0.0.0:8084 \
--advertise-pandaproxy-addr 127.0.0.1:8084 \
--kafka-addr 0.0.0.0:9094 \
--advertise-kafka-addr 127.0.0.1:9094 \
--rpc-addr 0.0.0.0:33147 \
--advertise-rpc-addr redpanda-3:33147
```

Now you can run `rpk` on one of the containers to interact with the cluster:

```bash
docker exec -it redpanda-1 rpk cluster info
```

The output of the status command looks like:

```bash
  Redpanda Cluster Status

  0 (127.0.0.1:9092)       (No partitions)
  1 (127.0.0.1:9093)       (No partitions)
  2 (127.0.0.1:9094)       (No partitions)
```

### Bring up a docker-compose file

You can easily try out different docker configuration parameters with a docker-compose file.

1. Save this content as `docker-compose.yml`:

    ```yaml
    version: '3.7'
    services:
    redpanda:
        entrypoint:
        - /usr/bin/rpk
        - redpanda
        - start
        - --smp
        - '1'
        - --reserve-memory
        - 0M
        - --overprovisioned
        - --node-id
        - '0'
        - --kafka-addr
        - PLAINTEXT://0.0.0.0:29092,OUTSIDE://0.0.0.0:9092
        - --advertise-kafka-addr
        - PLAINTEXT://redpanda:29092,OUTSIDE://localhost:9092
        # NOTE: Please use the latest version here!
        image: docker.vectorized.io/vectorized/redpanda:v21.4.13
        container_name: redpanda-1
        ports:
        - 9092:9092
        - 29092:29092
    ```

2. In the directory where the file is saved, run:

    ```bash
    docker-compose up -d
    ```

If you want to change the parameters, edit the docker-compose file and run the command again.

## Do some streaming

Here are some sample commands to produce and consume streams:

1. Create a topic. We'll call it "twitch_chat":

    ```bash
    docker exec -it redpanda-1 \
    rpk topic create twitch_chat --brokers=localhost:9092
    ```

1. Produce messages to the topic:

    ```bash
    docker exec -it redpanda-1 \
    rpk topic produce twitch_chat --brokers=localhost:9092
    ```

    Type text into the topic and press Ctrl + D to seperate between messages.

    Press Ctrl + C to exit the produce command.

1. Consume (or read) the messages in the topic:

    ```bash
    docker exec -it redpanda-1 \
    rpk topic consume twitch_chat --brokers=localhost:9092
    ```
    
    Each message is shown with its metadata, like this:
    
    ```bash
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

```bash
docker stop redpanda-1 redpanda-2 redpanda-3 && \
docker rm redpanda-1 redpanda-2 redpanda-3
```

If you set up volumes and a network, delete them with:

```bash
docker volume rm redpanda1 redpanda2 redpanda3 && \
docker network rm redpandanet
```

## What's Next?

- Our [FAQ](/docs/faq) page shows all of the clients that you can use to do streaming with Redpanda.
    (Spoiler: Any Kafka-compatible client!)
- Get a multi-node cluster up and running using [`rpk container`](/docs/guide-rpk-container).
- Use the [Quick Start Docker Guide](/docs/quick-start-docker) to try out Redpanda using Docker.
- Want to setup a production cluster? Check out our [Production Deployment Guide](/docs/production-deployment).

<img src="https://static.scarf.sh/a.png?x-pxid=3c187215-e862-4b67-8057-45aa9a779055" />
