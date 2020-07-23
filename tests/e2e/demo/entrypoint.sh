#!/bin/bash
set -e
set -x

export KAFKA_PATH=/opt/kafka
export SERVERS=${BAMBOO_BROKERS}
export TOPIC="${BAMBOO_NAMESPACE}-demo-topic"
export REPLICATION_FACTOR=${BAMBOO_BROKER_COUNT}
export RECORD_COUNT=1000
export BACKGROUND_PRODUCE=0
export PRODUCER_COUNT=2
/opt/v/tools/demo_script.sh
