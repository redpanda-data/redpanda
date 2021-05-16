---
title: Migrating data to Redpanda
order: 2
---

# Migrating data to Redpanda

One of the more elegant aspects of Redpanda is that it is compatible with the [Kafka API and ecosystem](/docs/www/faq.md).
So, when you want to migrate data from Kafka or replicate data between Redpanda clusters,
MirrorMaker 2 tool, bundled in the [Kafka download package](https://kafka.apache.org/downloads), is a natural solution.

In this article, we'll go through the process of configuring MirrorMaker to replicate from one Redpanda cluster to another.
For all of the details on MirrorMaker and its options, check out the [Kafka documentation](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27846330).

## Prerequisites

To set up the replication, we'll need:

- 2 Redpanda clusters - You can set up these clusters in any deployment you like, including [Kubernetes](/docs/www/quick-start-kubernetes.md), [Docker](/docs/www/quick-start-docker.md), [Linux](/docs/www/quick-start-linux.md), or [MacOS](/docs/www/quick-start-macos.md).
    The key points here are:
    - Make sure that the IP addresses and ports on each cluster are accessible by the  MirrorMaker.
    - Create topics in the destination cluster that match the names of the topics that you want to replicate from the source cluster.

- An instance of MirrorMaker - You can set up a separate system with MirrorMaker or run MirrorMaker on one of the Redpanda clusters.

## Installing MirrorMaker

MirrorMaker 2 is run by a shell script that is part of the Kafka download package.
To install MirrorMaker on the machine that you want to run the replication between the clusters:

1. Download the latest [Kafka download package](https://kafka.apache.org/downloads).

    For example:

    ```
    curl -O https://www.apache.org/dyn/closer.cgi?path=/kafka/2.8.0/kafka_2.13-2.8.0.tgz
    ```

2. Extract the files from the archive:

    ```
    tar -xvf kafka_2.13-2.8.0.tgz
    ```

## Create the MirrorMaker config files

MirrorMaker uses configuration files to get the connection details for the clusters.
You need to have one config file for the source cluster and another for the destination or mirror cluster.
In this example, we'll assume the file structure:

```
.
└── kafka_2.13-2.8.0
    └── bin
        └── kafka-mirror-maker.sh
```

To create the MirrorMaker configuration files:

1. In the base directory, create the config for consuming from the source cluster with:

    ```
    cat << EOF > consumer.config
    bootstrap.servers=34.107.64.237:31098
    exclude.internal.topics=true
    client.id=mirror_maker_consumer
    group.id=mirror_maker_consumer
    EOF
    ```

    The `bootstrap.servers` address is either:

    - For Kafka - The zookeeper address
    - For Redpanda - The address of one or more of the cluster nodes

2. Create the config for producing to the destination cluster with:

    ```
    cat << EOF > producer.config
    bootstrap.servers=34.107.5.41:31432
    acks=1
    batch.size=50
    client.id=mirror_maker_test_producer
    EOF
    ```

This is of course a simplified example of the config files.
You can experiment with the other options for the [producer config](https://kafka.apache.org/documentation.html#producerconfigs) and [consumer config](https://kafka.apache.org/documentation.html#consumerconfigs) to get the results that you need.

## Run MirrorMaker to start replication

To start the replication from the source cluster to the destination cluster, from the `kafka_2.12-2.8.0/bin/` directory, run:

```
cd kafka_2.12-2.8.0/bin/ && \
./kafka-mirror-maker.sh --consumer.config ../../redpanda.config --num.streams 1 --producer.config ../../redpanda-mirror.config --whitelist=".*"
```

With this command, MirrorMaker consumes events from all topics in the source cluster and produces them to the same topics in the destination cluster, if those topics exist.

## Next steps

After you have this basic replication running, you can:

- Use [consumer-offset-checker](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27846330#Kafkamirroring(MirrorMaker)-Howtocheckwhetheramirroriskeepingup) to verify that the data is synchronized.
- Use the [MirrorMaker parameters](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=27846330#Kafkamirroring(MirrorMaker)-Importconfigurationparametersforthemirrormaker) to specify timeouts, buffer sizes, and filter the topics by regex.
