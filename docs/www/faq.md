---
title: Frequently Asked Questions
order: 0
---
# Frequently Asked Questions

## What clients do you recommend to use with Redpanda?

Redpanda is Kafka API compatible so any client that works with Kafka
should work out of the box with Redpanda. However here is a list of clients
which we have tested with:

| Language | Client | Link |
| -------- | ------ | ---- |
| Java | Apache Kafka Java Client | https://github.com/apache/kafka |
| C/C++ | librdkafka | https://github.com/edenhill/librdkafka |
| Go | Sarama | https://github.com/Shopify/sarama |
| Python | kafka-python | https://pypi.org/project/kafka-python |
| JS | KafkaJS | https://kafka.js.org |

## Is Redpanda Fully Kafka API Compatible?

We support all parts of the Kafka API except for the transactions API. We are
working on adding this shortly and you can find the issue in our public
github here, [Support the Kafka Transactions API](https://github.com/vectorizedio/redpanda/issues/445). 

If you run into any issues while working with a Kafka tool, please let us know! [File an issue](https://github.com/vectorizedio/redpanda/issues/new)

## Does Redpanda use Zookeeper?

No, Redpanda is a modern streaming platform that has been built using C++ and
Raft for consensus. Since we use Raft we have no need for an external consensus
system like Zookeeper.

## Can I run Redpanda directly on Windows or MacOS?

Unfortunately, you can only run Redpanda directly on Linux. However, you can
use Docker to run Redpanda on any system supported by Docker. Please see our
[Quick Start Docker Guide](/docs/quick-start-docker) for more information.
