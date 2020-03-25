#!/bin/env bash
set -x

topic="sanfrancisco"
replication=1
partitions=6
servers="localhost:9092"
record_count=$((1 << 31))
record_size=1024
throughput=$((1024 * 64))
acks=1
client_names=(
  SKOBIE
  AVENUE
  JOREL
  GIZMO
  GILB
  MONTAGUE
  FRANSWA
  BUTCHIE
  SERENITY
  WATTS
  TILLER
  WELFIE
  RAURI
  ICHIBOD
  CHUMI
  EMORY
  SAPHRIS
  MCGREGOR
  WALKEN
  UBALDO
  MAYKAYLA
  DIOGGIE
  LARES
  WEASEL
  ARAGON
  ALPHA
  CHORIZO
  MARLIE
  SCRIBBLES
  ROXSY
  SAMIR
  BABY-BELLO
  NORWAY
  CAELAN
  ESSENCE
  LUCKLY
  TARKA
  DHALYA
  NAOKI
  MERQUJUQ
  DEFIANCE
  GUSTO
  SPANKY
  PORGY
  KILLIAN
  HIROHITO
  SALLYWAGS
  MOWGLEY
  PETROVICH
  SEPHY
  BENTO
)
client_id=${client_names[$(date +%s) % ${#client_names[@]}]}

if [[ $1 == "create" ]]; then
  bin/kafka-topics.sh --bootstrap-server ${servers} \
    --create --topic sanfrancisco --partitions ${partitions} \
    --replication-factor ${replication} "${@:2:99}"
fi

if [[ $1 == "produce" ]]; then
  bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance \
    --record-size ${record_size} \
    --topic ${topic} \
    --num-records ${record_count} \
    --throughput ${throughput} \
    --producer-props "acks=${acks}" \
    --producer-props "client.id=${client_id}" \
    bootstrap.servers=${servers} \
    buffer.memory=$((1024 * 1024)) "${@:2:99}"
fi

if [[ $1 == "consume" ]]; then
  bin/kafka-consumer-perf-test.sh --broker-list=${servers} \
    --fetch-size=$((1024 * 1024)) \
    --timeout=1000 \
    --messages=${record_count} \
    --group="$client_id" \
    --topic=${topic} \
    --threads 1 "${@:2:99}"
fi
