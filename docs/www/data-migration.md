---
title: Migrating data to Redpanda
order: 2
---

# Migrating data to Redpanda

One of the more elegant aspects of Redpanda is that it is compatible with the [Kafka API and ecosystem](/docs/faq).
So when you want to migrate data from Kafka or replicate data between Redpanda clusters,
the MirrorMaker 2 tool bundled in the [Kafka download package](https://kafka.apache.org/downloads) is a natural solution.

In this article, we'll go through the process of configuring MirrorMaker to replicate data from one Redpanda cluster to another.
In short, MirrorMaker reads a configuration file that specifies the connection details for the Redpanda clusters, as well as other options.
After you start MirrorMaker, the data migration continues according to the configuration at launch time until you shutdown the MirrorMaker process.

For all of the details on MirrorMaker and its options, check out the [Kafka documentation](https://kafka.apache.org/documentation/#georeplication).

## Prerequisites

To set up the replication, we'll need:

- 2 Redpanda clusters - You can set up these clusters in any deployment you like, including [Kubernetes](/docs/quick-start-kubernetes), [Docker](/docs/quick-start-docker), [Linux](/docs/quick-start-linux), or [MacOS](/docs/quick-start-macos).

- MirrorMaker host - You install MirrorMaker on a separate system or on one of the Redpanda clusters, as long as the IP addresses and ports on each cluster are accessible from the MirrorMaker host.
You must install the [Java Runtime Engine (JRE)](https://docs.oracle.com/javase/10/install/toc.htm) on the MirrorMaker host.

## Installing MirrorMaker

MirrorMaker 2 is run by a shell script that is part of the Kafka download package.
To install MirrorMaker 2 on the machine that you want to run the replication between the clusters:

1. Download the latest [Kafka download package](https://kafka.apache.org/downloads).

    For example:

    ```
    curl -O https://dlcdn.apache.org/kafka/3.0.0/kafka_2.13-3.0.0.tgz
    ```

2. Extract the files from the archive:

    ```
    tar -xvf kafka_2.13-3.0.0.tgz
    ```

## Create the MirrorMaker 2 config files

MirrorMaker uses configuration files to get the connection details for the clusters.
You can find the MirrorMaker script and configuration files in the expanded Kafka directory.

```
.
└── kafka_2.13-3.0.0
    └── bin
        └── connect-mirror-maker.sh
    └── config
        └── connect-mirror-maker.properties
```

The sample configuration is a great place to understand a number of the settings for MirrorMaker.

To create a basic configuration file, go to the `config` and run this command:

```
cat << EOF > mm2.properties
// Name the clusters
clusters = redpanda1, redpanda2

// Assign IP addresses to the cluster names
redpanda1.bootstrap.servers = <redpanda1_cluster_ip>:9092
redpanda2.bootstrap.servers = <redpanda2_cluster_ip>:9092

// Set replication for all topics from Redpanda 1 to Redpanda 2
redpanda1->redpanda2.enabled = true
redpanda1->redpanda2.topics = .*

// Setting replication factor of newly created remote topics
replication.factor = 1
EOF
```

## Run MirrorMaker to start replication

To start MirrorMaker in the `kafka_2.13-3.0.0/bin/` directory, run:

```
./kafka_2.13-3.0.0/bin/connect-mirror-maker.sh mm2.properties
```

With this command, MirrorMaker consumes all topics from the redpanda1 cluster and replicates them into the redpanda2 cluster.
MirrorMaker adds the prefix `redpanda1` to the names of replicated topics.

> **_Note:_** MirrorMaker uses the `__consumer_offsets` topic to replicate consumer offsets between clusters. This is currently not supported in Redpanda, but you can follow the progress of this issue at: https://github.com/vectorizedio/redpanda/issues/1752

## See migration in action

Here are the basic commands to produce and consume streams:

1. Create a topic in the source cluster. We'll call it "twitch_chat":

    ```bash
    rpk topic create twitch_chat --brokers <node IP>:<kafka API port>
    ```

1. Produce messages to the topic:

    ```bash
    rpk topic produce twitch_chat --brokers <node IP>:<kafka API port>
    ```

    Type text into the topic and press Ctrl + D to seperate between messages.

    Press Ctrl + C to exit the produce command.

1. Consume (or read) the messages from the destination cluster:

    ```bash
    rpk topic consume twitch_chat --brokers <node IP>:<kafka API port>
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

Now you know the replication is working!

## Stop MirrorMaker

To stop the MirrorMaker process, use `top` to find its process ID and then run: `kill <MirrorMaker pid>`

## Troubleshooting

If you run into any difficulty with data migration, contact us in our [Slack](https://vectorized.io/slack) community so we can help you succeed.
