---
title: Windows Quick Start Guide
order: 0
---
# Windows Quick Start Guide

Redpanda is a modern [streaming platform](/blog/intelligent-data-api/) for mission critical workloads.
With Redpanda you can get up and running with streaming quickly
and be fully compatible with the [Kafka ecosystem](https://cwiki.apache.org/confluence/display/KAFKA/Ecosystem).

This quick start guide can help you get started with Redpanda for development and testing purposes.
For production or benchmarking, set up a [production deployment](/docs/production-deployment).

## Getting Redpanda running

You can run Redpanda on a Windows machine in a Docker container.

Before you start you'll need to install [WSL2](https://docs.microsoft.com/en-us/windows/wsl/install) and [Docker for Windows](https://docs.docker.com/desktop/windows/install/).

Don’t forget that in order for Docker for Windows to work, you have to [enable your Hypervisor service](https://docs.microsoft.com/en-us/virtualization/hyper-v-on-windows/quick-start/enable-hyper-v) in the Windows Control Panel.


## Executing directly from Docker repository

The [Docker image for Redpanda](https://hub.docker.com/r/vectorized/redpanda) is hosted in Docker Hub.


### Setting a 1-node cluster
You can copy and paste this command to create a 1-node cluster:

```console
docker run -d --pull=always --name=redpanda-1 --rm ^
-p 9092:9092 ^
-p 9644:9644 ^
docker.vectorized.io/vectorized/redpanda:latest ^
redpanda start ^
--overprovisioned ^
--smp 1  ^
--memory 1G ^
--reserve-memory 0M ^
--node-id 0 ^
--check=false
```
### Setting a 3-node cluster

To test out the interaction between nodes in a cluster, set up a Docker network with 3 containers in a cluster.

#### Create network and persistent volumes

First we need to set up a bridge network so that the Redpanda instances can communicate with each other
but still allow for the Kafka API to be available on the localhost.
We'll also create the persistent volumes that let the Redpanda instances keep state during instance restarts.

```bash
docker network create -d bridge redpandanet && ^
docker volume create redpanda1 && ^
docker volume create redpanda2 && ^
docker volume create redpanda3
```

#### Start Redpanda nodes

We then need to start the nodes for the Redpanda cluster.

```bash
docker run -d ^
--pull=always ^
--name=redpanda-1 ^
--hostname=redpanda-1 ^
--net=redpandanet ^
-p 8082:8082 ^
-p 9092:9092 ^
-p 9644:9644 ^
-v "redpanda1:/var/lib/redpanda/data" ^
docker.vectorized.io/vectorized/redpanda redpanda start ^
--smp 1  ^
--memory 1G  ^
--reserve-memory 0M ^
--overprovisioned ^
--node-id 0 ^
--check=false ^
--pandaproxy-addr 0.0.0.0:8082 ^
--advertise-pandaproxy-addr 127.0.0.1:8082 ^
--kafka-addr 0.0.0.0:9092 ^
--advertise-kafka-addr 127.0.0.1:9092 ^
--rpc-addr 0.0.0.0:33145 ^
--advertise-rpc-addr redpanda-1:33145 &&

docker run -d ^
--pull=always ^
--name=redpanda-2 ^
--hostname=redpanda-2 ^
--net=redpandanet ^
-p 9093:9093 ^
-v "redpanda2:/var/lib/redpanda/data" ^
docker.vectorized.io/vectorized/redpanda redpanda start ^
--smp 1  ^
--memory 1G  ^
--reserve-memory 0M ^
--overprovisioned ^
--node-id 1 ^
--seeds "redpanda-1:33145" ^
--check=false ^
--pandaproxy-addr 0.0.0.0:8083 ^
--advertise-pandaproxy-addr 127.0.0.1:8083 ^
--kafka-addr 0.0.0.0:9093 ^
--advertise-kafka-addr 127.0.0.1:9093 ^
--rpc-addr 0.0.0.0:33146 ^
--advertise-rpc-addr redpanda-2:33146 &&

docker run -d ^
--pull=always ^
--name=redpanda-3 ^
--hostname=redpanda-3 ^
--net=redpandanet ^
-p 9094:9094 ^
-v "redpanda3:/var/lib/redpanda/data" ^
docker.vectorized.io/vectorized/redpanda redpanda start ^
--smp 1  ^
--memory 1G  ^
--reserve-memory 0M ^
--overprovisioned ^
--node-id 2 ^
--seeds "redpanda-1:33145" ^
--check=false ^
--pandaproxy-addr 0.0.0.0:8084 ^
--advertise-pandaproxy-addr 127.0.0.1:8084 ^
--kafka-addr 0.0.0.0:9094 ^
--advertise-kafka-addr 127.0.0.1:9094 ^
--rpc-addr 0.0.0.0:33147 ^
--advertise-rpc-addr redpanda-3:33147
```


## Executing with a docker-compose file

Another way to spin up a Redpanda cluster is with a docker-compose file.
Copy the code here and save it as `docker-compose.yaml`:

```console 
version: '3.7'
services:
  redpanda:
    command:
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
    image: docker.vectorized.io/vectorized/redpanda:latest
    container_name: redpanda-1
    ports:
    - 9092:9092
    - 29092:29092
```   
  
In the directory that you saved the file, open your CMD and execute this command:     
 
```console
docker-compose up –d
```
If everything is correct, you’ll see this:
```console
Creating redpanda-1 ... done
```

You can also check Docker for Desktop for any container errors.


## Start streaming

We're going to use the `rpk` to run our commands.
`rpk` is essentially a CLI tool that you can use to run [management and data commands](https://vectorized.io/docs/rpk-commands/) on the cluster.

Check the information about the cluster. 

```
docker exec -it redpanda-1 rpk cluster info
```

1. Create a topic. We’ll call it “twitch_chat”:

```
docker exec -it redpanda-1 ^
rpk topic create twitch_chat --brokers=localhost:9092
```

2. Produce messages to the topic:

```
docker exec -it redpanda-1 ^
rpk topic produce twitch_chat --brokers=localhost:9092
``` 

Type text into the topic and press Ctrl + D to separate between messages.

Press Ctrl + C to exit the produce command.

3. Consume (or read) the messages in the topic:

```
docker exec -it redpanda-1 ^
rpk topic consume twitch_chat --brokers=localhost:9092
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


## Clean up

When you are finished with the cluster, you can shutdown and delete the containers.
Change the commands below accordingly if you used the 1-cluster option or the 3-cluster option.

```
docker stop redpanda-1 redpanda-2 redpanda-3 && ^
docker rm redpanda-1 redpanda-2 redpanda-3
```

If you set up volumes and a network, delete them with:

```
docker volume rm redpanda1 redpanda2 redpanda3 && ^
docker network rm redpandanet
```

## What's next?

- Our [FAQ](/docs/faq) page shows all of the clients that you can use to do streaming with Redpanda.
    (Spoiler: Any Kafka-compatible client!)
- Get a multi-node cluster up and running using [`rpk container`](/docs/guide-rpk-container).
- Use the [Quick Start Docker Guide](/docs/quick-start-docker) to try out Redpanda using Docker.
- Want to setup a production cluster? Check out our [Production Deployment Guide](/docs/production-deployment).
