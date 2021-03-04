---
title: MacOS Quick Start Guide
order: 0
---
# MacOS Quick Start Guide

Redpanda is a modern streaming platform for mission critical workloads. Redpanda
is also fully API compatible Kafka allowing you to make full use of the
Kafka ecosystem.

This quick start guide to intended to help you get started with Redpanda
for development and testing purposes. For production deployments or
performance testing please see our
[Production Deployment](/docs/production-deployment) for more information.

## Installation

Redpanda itself cannot run directly on MacOS, so we must make use of
docker. If you wish to run Redpanda manually using Docker please follow our
[Docker Quick Start Guide](/docs/quick-start-docker). Otherwise you can
download our binary, `rpk`, which stands for Redpanda Keeper, to orchestrate
the running of Redpanda via Docker for you.

To install RPK, you can choose to either use [Homebrew](https://brew.sh/)
or download the binary directly.

### Homebrew

The fastest way to install on MacOS is to use Homebrew. If you have
Homebrew installed you can simply run the following command:

```
brew install vectorizedio/tap/redpanda
```

### Binary

The latest RPK binary can be found here: [rpk-darwin-amd64.zip](https://github.com/vectorizedio/redpanda/releases/latest/download/rpk-darwin-amd64.zip)

## What's Next?

- Setup a local multi node cluster using [RPK Container Guide](/docs/guide-rpk-container)
- Check out our [FAQ](/docs/faq)
- Want to setup a production cluster? Check out our [Production Deployment](/docs/production-deployment) Guide.
