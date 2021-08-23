---
title: Using MirrorMaker 2 with Redpanda
order: 2
---

# Migrating data to Redpanda

One of the more elegant aspects of Redpanda is that it is compatible with the [Kafka API and ecosystem](/docs/www/faq.md).
So, when you want to migrate data from Kafka or replicate data between Redpanda clusters,
MirrorMaker 2 tool, bundled in the [Kafka download package](https://kafka.apache.org/downloads), is a natural solution.

In this article, we'll go through the process of configuring MirrorMaker to replicate from one Redpanda cluster to another.
For all of the details on MirrorMaker and its options, check out the [Kafka documentation](https://kafka.apache.org/documentation/#georeplication).

## Prerequisites

To set up the replication, we'll need:

- 2 Redpanda Clusters - You can set up these clusters in any deployment you like, including [Kubernetes](/docs/www/quick-start-kubernetes.md), [Docker](/docs/www/quick-start-docker.md), [Linux](/docs/www/quick-start-linux.md), or [MacOS](/docs/www/quick-start-macos.md).
    The key points here are:
    - Make sure that the IP addresses and ports on each cluster are accessible by MirrorMaker.

- An instance of MirrorMaker - You can set up a separate system with MirrorMaker or run MirrorMaker on one of the Redpanda clusters.

## Installing MirrorMaker

MirrorMaker 2 is run by a shell script that is part of the Kafka download package.
To install MirrorMaker 2 on the machine that you want to run the replication between the clusters:

1. Download the latest [Kafka download package](https://kafka.apache.org/downloads).

    For example:

    ```
    curl -O https://www.apache.org/dyn/closer.cgi?path=/kafka/2.8.0/kafka_2.13-2.8.0.tgz
    ```

2. Extract the files from the archive:

    ```
    tar -xvf kafka_2.13-2.8.0.tgz
    ```

## Create the MirrorMaker 2 config files

MirrorMaker uses configuration files to get the connection details for the clusters.
In this example, we'll assume the file structure:

```
.
└── kafka_2.13-2.8.0
    └── bin
        └── connect-mirror-maker.sh
```

You can find a sample configuration file within the Kafka directory:
```
.
└── kafka_2.13-2.8.0
    └── config
        └── connect-mirror-maker.properties
```

The sample configuration is a great place to understand a number of the
settings for MirroMaker. For this example we will create a new configuration file. To create this file do the following within the config directory of the Kafka directory:

    ```
    cat << EOF > mm2.properties
    clusters = redpanda1, redpanda2
    redpanda1.bootstrap.server = redpanda1:9092
    redpanda2.bootstrap.server = redpanda2:9092

    redpanda1->redpanda2.enabled = true
    redpanda1->redpanda2.topics = .*

    replication.factor = 3
    EOF
    ```

## Run MirrorMaker to start replication

To start MirrorMaker from the `kafka_2.12-2.8.0/bin/` directory, run:

```
cd kafka_2.12-2.8.0/ && \
./bin/connect-mirror-maker.sh  ./config/mm2.properties
```

With this command, MirrorMaker will consume all topics from `redpanda1` and will replicated them into redpanda2. MirrorMaker will add a prefix to the topic names of `redpanda1` for the topics replicated in `redpanda2`.

## Caveats

MirrorMaker relies upon `__consumer_offsets` to replicate consumer offsets between clusters. This is currently not supported in Redpanda, and you can follow along the progress of this issue here: https://github.com/vectorizedio/redpanda/issues/1752
