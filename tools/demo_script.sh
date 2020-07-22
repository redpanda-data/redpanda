#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

# topic name
TOPIC=${TOPIC:-demo-topic}
# topic replication factor
REPLICATION_FACTOR=${REPLICATION_FACTOR:-3}
# number of topic partitions
PARTITIONS=${PARTITIONS:-6}
# topic segment size
SEGMENT_SIZE=${SEGMENT_SIZE:-$((10 * 1024 * 1024))}

# brokers to connect to
SERVERS=${SERVERS:-localhost:9092}
# number of published records
RECORD_COUNT=${RECORD_COUNT:-$((1 << 31))}
# size of the record
RECORD_SIZE=${RECORD_SIZE:-1024}
# produce acks settings
ACKS=${ACKS:-1}

# path to folder containing kafka
KAFKA_PATH=${KAFKA_PATH:-/opt/kafka-dev}

kafka_topics=${KAFKA_PATH}/bin/kafka-topics.sh

producer_performance="${KAFKA_PATH}/bin/kafka-producer-perf-test.sh"

function create_topic() {
  echo "creating topic '${TOPIC}' with ${PARTITIONS} partitions and replication factor ${REPLICATION_FACTOR}"
  ${kafka_topics} \
    --bootstrap-server "${SERVERS}" \
    --create \
    --topic ${TOPIC} \
    --partitions ${PARTITIONS} \
    --replication-factor ${REPLICATION_FACTOR}
}

function delete_topic() {
  echo "deleting topic '${TOPIC}'"
  ${kafka_topics} \
    --bootstrap-server "${SERVERS}" \
    --delete \
    --topic ${TOPIC}
}

# first prepare topic
create_topic
delete_topic
create_topic

# produce to topic immediately
for i in {1..10}; do
  export demo_id="harpoon.demo.${i}"
  (${producer_performance} \
    --record-size ${RECORD_SIZE} \
    --topic ${TOPIC} \
    --num-records ${RECORD_COUNT} \
    --throughput -1 \
    --producer-props acks=${ACKS} \
    bootstrap.servers="${SERVERS}" \
    client.id="${demo_id}" \
    batch.size=81960 \
    buffer.memory=$((1024 * 1024)) &)
done
