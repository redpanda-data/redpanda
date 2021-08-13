---
title: MacOS Quick Start Guide
order: 0
---
# MacOS Quick Start Guide

Redpanda is a modern [streaming platform](/blog/intelligent-data-api/) for mission critical workloads.
With Redpanda you can get up and running with streaming quickly
and be fully compatible with the [Kafka ecosystem](https://cwiki.apache.org/confluence/display/KAFKA/Ecosystem).

This quick start guide can help you get started with Redpanda for development and testing purposes.
For production or benchmarking, set up a [production deployment](/docs/production-deployment).

## Getting Redpanda running

To run Redpanda on MacOS, we'll use `rpk` to bring up Redpanda nodes in Docker containers.
Make sure that you install [Docker](https://docs.docker.com/docker-for-mac/install/) first.

If you want to customize the containers, you can also set up your own [Redpanda containers in Docker](/docs/quick-start-docker).

### Installing rpk

You can install `rpk` on MacOS from either with [Homebrew](https://brew.sh/) or you can just download the binary.

- To install `rpk` with Homebrew, run: `brew install vectorizedio/tap/redpanda`
- To download the `rpk` binary:

    1. Download the [rpk archive](https://github.com/vectorizedio/redpanda/releases/latest/download/rpk-darwin-amd64.zip) with: `curl -LO https://github.com/vectorizedio/redpanda/releases/latest/download/rpk-darwin-amd64.zip`
    1. Unpack the archive to `/usr/local/bin` or any location in your `$PATH` with: `unzip rpk-darwin-amd64.zip`

### Running a redpanda cluster

To run Redpanda in a 3-node cluster, run: `rpk container start -n 3`

The first time you run `rpk container start`, it downloads an image matching the rpk version (you can check it by running `rpk version`).
You now have a 3-node cluster running Redpanda!

You can run `rpk` commands to interact with the cluster, for example:

```bash
rpk cluster info
```

## Do some streaming

Here are the basic commands to produce and consume streams:

1. Create a topic. We'll call it "twitch_chat":

    ```bash
    rpk topic create twitch_chat
    ```

1. Produce messages to the topic:

    ```bash
    rpk topic produce twitch_chat
    ```

    Type text into the topic and press Ctrl + D to seperate between messages.

    Press Ctrl + C to exit the produce command.

1. Consume (or read) the messages in the topic:

    ```bash
    rpk topic consume twitch_chat
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

When you are finished with the cluster, you can shutdown and delete the nodes with:

```bash
rpk container purge
```

## What's Next?

- Our [FAQ](/docs/faq) page shows all of the clients that you can use to do streaming with Redpanda.
    (Spoiler: Any Kafka-compatible client!)
- Use the [Quick Start Docker Guide](/docs/quick-start-docker) to try out Redpanda using Docker.
- Want to setup a production cluster? Check out our [Production Deployment Guide](/docs/production-deployment).
