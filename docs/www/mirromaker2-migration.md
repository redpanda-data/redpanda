---
title: Migrating data to Redpanda
order: 2
---

# Migrating data to Redpanda

One of the more elegant aspects of Redpanda is that it is compatible with the [Kafka API and ecosystem](/docs/www/faq.md).
So, when you want to migrate data from Kafka or replicate data between Redpanda clusters,
MirrorMaker 2 tool, which is bundled in the [Kafka download package](https://kafka.apache.org/downloads), is a natural solution.

In this article, we'll go through the process of configuring MirrorMaker to replicate data from one Redpanda cluster to another.
In short, MirrorMaker reads a configuration file that specifies the connection details for the Redpanda clusters, as well as other options.
After you start MirrorMaker, the data migration continues according to the configuration at launch time until you kill the MirrorMaker process.

For all of the details on MirrorMaker and its options, check out the [Kafka documentation](https://kafka.apache.org/documentation/#georeplication).

## Prerequisites

To set up the replication, we'll need:

- 2 Redpanda clusters - You can set up these clusters in any deployment you like, including [Kubernetes](/docs/www/quick-start-kubernetes.md), [Docker](/docs/www/quick-start-docker.md), [Linux](/docs/www/quick-start-linux.md), or [MacOS](/docs/www/quick-start-macos.md).
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

The sample configuration is a great place to understand a number of the settings for MirroMaker.

To create a basic configuration file, go to the `config` and run this command:

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

To start MirrorMaker in the `kafka_2.12-2.8.0/bin/` directory, run:

```
cd kafka_2.12-2.8.0/ && \
./bin/connect-mirror-maker.sh  ./config/mm2.properties
```

With this command, MirrorMaker consumes all topics from the redpanda1 cluster and replicates them into the redpanda2 cluster.
MirrorMaker adds the prefix `redpanda1` to the names of replicated topics.

> **_Note:_** MirrorMaker uses the `__consumer_offsets` to replicate consumer offsets between clusters. This is currently not supported in Redpanda, but you can follow the progress of this issue at: https://github.com/vectorizedio/redpanda/issues/1752

## Stop MirrorMaker

To stop the MirrorMaker process, use `top` to find its process ID and then run: `kill <MirrorMaker pid>`
