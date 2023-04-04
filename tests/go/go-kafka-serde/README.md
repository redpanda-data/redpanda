# Golang Kafka Serde Client

This directory contains a client that can be used to validate the operation of Redpanda's Schema Registry via
Confluent's Golang client.

This application will create either a simple AVRO or PROTOBUF schema, serialize a number of messages

## Building

### Pre-requisites

* Protobuf Compiler
* Golang compiler

### Build Instructions

It is important to set `$GOPATH` either within your environment or on the command line when you run `make`:

```shell
GOPATH=$HOME/go make clean all
```

## Usage

```shell
❯ ./go-kafka-serde -help
Usage of ./go-kafka-serde:
  -brokers string
        comma delimited list of brokers (default "localhost:9092")
  -consumer-group string
        Consumer group to use (default "db5b01db-7f48-4d7d-8ba6-eb512788341e")
  -count int
        Number of messages to produce and consume (default 1)
  -debug
        Enable verbose logging
  -protocol string
        Protocol to use.  Must be AVRO or PROTOBUF (default "AVRO")
  -schema-registry string
        URL of schema registry (default "http://127.0.0.1:8081")
  -security string
        Security settings
  -topic string
        topic to produce/consume from (default "3aa75595-1222-4daa-b407-afe43c8496d5")
```
