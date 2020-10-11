#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

# This script runs the kafka verifier application
# ------------------------------------------------
#
# Prerequisites
#   - kafka compacted log verifier built
#     (vtools build java --targets compacted-log-verifier)
#
# Verification consist of two phases
#
# 1. Topic creation and generation of data
#
#   In this phase topic is created and data are produced to topic.
#   After successful run verifier creates a file containing reference state
#   i.e. expected latest value of each key
#
#   running:
#
#     ./compacted_topic_verifier.sh produce
#
# 2. Consume
#
#  In this phase the verifier consumes all the records from given topic and
#  recreates the state. After successful run the state is compared with
#  reference created in produce phase all inconsistencies are logged to the
#  stdout.
#
#  running:
#
#    ./compacted_topic_verifier.sh consume
#
# Default parameters can be overridden using following syntax
#
# TOPIC=other-one PARTITIONS=6 ./compacted_topic_verifier.sh produce
#

tld=$(git rev-parse --show-toplevel)

# topic name
TOPIC=${TOPIC:-sf-compacted-2}
# topic replication factor
REPLICATION=${REPLICATION:-1}
# number of topic partitions
PARTITIONS=${PARTITIONS:-1}
# compression used by producer
COMPRESSION=${COMPRESSION:-none}
# topic segment size
SEGMENT_SIZE=${SEGMENT_SIZE:-$((10 * 1024 * 1024))}
# properties file containing security settings
SECURITY_PROPERTIES=${SECURITY_PROPERTIES:-}

# brokers to connect to
SERVERS=${SERVERS:-localhost:9092}
# number of published records
RECORD_COUNT=${RECORD_COUNT:-1000000}
# size of the record payload
PAYLOAD_SIZE=${PAYLOAD_SIZE:-1024}
# size of the record keys
KEY_SIZE=${KEY_SIZE:-64}
# number of distinct keys
KEY_SET_CARIDINALITY=${KEY_SET_CARIDINALITY:-100000}
# produce acks settings
ACKS=${ACKS:-1}
# state file path
STATE_FILE=${STATE_FILE:-"${tld}/build/verifier.state"}

java_path=${tld}/build/java/bin/java
jar_path=${tld}/build/java-build/compacted-log-verifier/kafka-compacted-topics-verifier.jar

cmd="${java_path} -jar ${jar_path} \
    --topic ${TOPIC} \
    --broker ${SERVERS} \
    --state-file ${STATE_FILE}"

if [ -n "${SECURITY_PROPERTIES}" ]; then
  cmd="${cmd} \
         --security-properties=${SECURITY_PROPERTIES}"
fi

case $1 in
  produce)
    ${cmd} \
      produce \
      --num-records ${RECORD_COUNT} \
      --replication-factor ${REPLICATION} \
      --partitions ${PARTITIONS} \
      --compression ${COMPRESSION} \
      --segment-size ${SEGMENT_SIZE} \
      --payload-size ${PAYLOAD_SIZE} \
      --key-size ${KEY_SIZE} \
      --producer-props acks=${ACKS} \
      --key-cardinality ${KEY_SET_CARIDINALITY}
    ;;
  consume)
    ${cmd} \
      consume
    ;;
  *) echo "Usage: compacted_topic_verifier.sh <mode>" ;;

esac
