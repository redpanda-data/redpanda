#!/bin/env bash
set -x

topic="sanfrancisco"
replication=1
partitions=6
servers="localhost:9092"
record_count=$((1 << 31))
record_size=1024
throughput=-1
acks=1

bin/kafka-topics.sh --bootstrap-server ${servers} \
  --create --topic sanfrancisco --partitions ${partitions} \
  --replication-factor ${replication}

bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance \
  --record-size ${record_size} \
  --topic ${topic} \
  --num-records ${record_count} \
  --throughput ${throughput} \
  --producer-props "acks=${acks}" \
  bootstrap.servers=${servers} \
  buffer.memory=$((1024 * 1024))
