# Java Kafka Serde Client

This repository contains a client that can be used to validate
the operation of Redpanda's Schema Registry via the Kafka Java library
provided by Confluent.

The application will create either a simple AVRO or PROTOBUF schema,
serialize a number of messages, produce them, and then consume and validate
that the message can be deserialized.

This test will exercise the schema registry by attempting to register and then
read the generated schema.

## Building

### Pre-requisites

* Java compiler
* Maven

### Build Instructions

```shell
mvn clean package --batch-mode --file <src>/tests/java/kafka-serde/ --define buildDir=<builddir>
```

#### Usage

```shell
usage: Java Kafka Serde Client [-h] -b BROKERS [-t TOPIC] -s SCHEMA_REGISTRY [-g CONSUMER_GROUP] [-p {AVRO,PROTOBUF}] [-c COUNT] [--security SECURITY]

Java Kafka Serde Client

named arguments:
  -h, --help             show this help message and exit
  -b BROKERS, --brokers BROKERS
                         Comma separated list of brokers
  -t TOPIC, --topic TOPIC
                         Name of topic (default: 16cd3248-b384-4042-bf1f-1b8b818a2fb5)
  -s SCHEMA_REGISTRY, --schema-registry SCHEMA_REGISTRY
                         URL of Schema Registry
  -g CONSUMER_GROUP, --consumer-group CONSUMER_GROUP
                         Consumer group (default: 605719f4-752f-4fc0-b3d5-8e7cc850ee41)
  -p {AVRO,PROTOBUF}, --protocol {AVRO,PROTOBUF}
                         Protocol to use (default: AVRO)
  -c COUNT, --count COUNT
                         Number of messages to send and consume (default: 1)
  --security SECURITY    JSON formatted security string
```
