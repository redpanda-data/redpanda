#!/bin/bash
set -e
set -x

export KAFKA_PATH=/opt/kafka
export SERVERS=${BAMBOO_BROKERS}
export TOPIC="${BAMBOO_NAMESPACE}-demo-topic"
export REPLICATION_FACTOR=${BAMBOO_BROKER_COUNT}
export RECORD_COUNT=100
export BACKGROUND_PRODUCE=0
export PRODUCER_COUNT=1
/opt/v/tools/demo_script.sh
