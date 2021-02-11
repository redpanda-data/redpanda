---
title: Linux Quick Start Guide
order: 0
---
# Linux Quick Start Guide

Redpanda is a modern [streaming platform](/blog/intelligent-data-api/) for mission critical workloads.
With Redpanda you can get up and running with streaming quickly
and be fully compatible with the [Kafka ecosystem](https://cwiki.apache.org/confluence/display/KAFKA/Ecosystem).

This quick start guide can help you get started with Redpanda for development and testing purposes.
For production or benchmarking, setup a [production deployment](production-deployment).

## Install Redpanda:

We've simplified the installation process down to a few commands:

- On Fedora/RedHat systems:

     ```
     ## Run the setup script to download and install the repo
     curl -1sLf 'https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.rpm.sh' | sudo -E bash && \
     ## Use yum to install redpanda
     sudo yum install redpanda -y && \
     ## Start redpanda as a service 
     sudo systemctl start redpanda
     ```

- On Debian/Ubuntu systems:

     ```
     ## Run the setup script to download and install the repo
     curl -1sLf 'https://packages.vectorized.io/nzc4ZYQK3WRGd9sy/redpanda/cfg/setup/bash.deb.sh' | sudo -E bash && \
     ## Use apt to install redpanda
     sudo apt install redpanda -y && \
     ## Start redpanda as a service 
     sudo systemctl start redpanda
     ```

To see that Redpanda is up and running, run: `sudo systemctl status redpanda`

The output should look like:

```sh
● redpanda.service - Redpanda, the fastest queue in the West.
     Loaded: loaded (/lib/systemd/system/redpanda.service; enabled; vendor preset: enabled)
     Active: active (running)
```

You now have a single node cluster running Redpanda!

## Do some streaming

Here are the basic commands to produce and consume streams:

1. Create a topic, we'll call it "twitch_chat":

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

     Each message is shown with its metdata, like this:

     ```
     {
     "message": "How do you stream with Redpanda?\n",
     "partition": 0,
     "offset": 1,
     "timestamp": "2021-02-10T15:52:35.251+02:00"
     }
     ```

You've just installed Redpanda and done streaming in a few easy steps. 

## What's Next?

- Our [FAQ](faq) page shows all of the clients that you can use to do streaming with Redpanda.
     (Spoiler: Any Kafka-compatible client!)
- Get a multi-node cluster up and running is by using [`rpk container`](guide-rpk-container).
- Use the [Quick Start Docker Guide](quick-start-docker) to try out Redpanda on Docker.
- Want to setup a production cluster? Check out our [Production Deployment Guide](production-deployment).
