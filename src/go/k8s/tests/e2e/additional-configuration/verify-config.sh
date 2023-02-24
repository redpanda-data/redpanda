#!/bin/bash
retries=20
until [ "$retries" -lt 0 ]; do
  PANDAPROXY_RETRIES=$1
  CRASH_LOOP_LIMIT=$2
  if [ -z CRASH_LOOP_LIMIT ] && [ -z $PANDAPROXY_RETRIES ]; then
    echo "requires two argument, pandaproxy retries count and crash loop limit"
    exit 1
  fi
  actual=$(kubectl exec additional-configuration-0 -- cat /etc/redpanda/redpanda.yaml)
  expected=$(
    cat <<EOF
config_file: /etc/redpanda/redpanda.yaml
pandaproxy:
  advertised_pandaproxy_api:
  - address: additional-configuration-0.additional-configuration.default.svc.cluster.local.
    name: proxy
    port: 8082
  pandaproxy_api:
  - address: 0.0.0.0
    name: proxy
    port: 8082
pandaproxy_client:
  brokers:
  - address: additional-configuration-0.additional-configuration.default.svc.cluster.local.
    port: 9092
  retries: ${PANDAPROXY_RETRIES}
redpanda:
  admin:
  - address: 0.0.0.0
    name: admin
    port: 9644
  advertised_kafka_api:
  - address: additional-configuration-0.additional-configuration.default.svc.cluster.local.
    name: kafka
    port: 9092
  advertised_rpc_api:
    address: additional-configuration-0.additional-configuration.default.svc.cluster.local.
    port: 33145
  auto_create_topics_enabled: false
  cloud_storage_segment_max_upload_interval_sec: 1800
  data_directory: /var/lib/redpanda/data
  default_topic_partitions: 3
  developer_mode: true
  crash_loop_limit: ${CRASH_LOOP_LIMIT}
  kafka_api:
  - address: 0.0.0.0
    name: kafka
    port: 9092
  log_segment_size: 536870912
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers: []
rpk:
  coredump_dir: /var/lib/redpanda/coredump
  enable_memory_locking: false
  enable_usage_stats: false
  overprovisioned: true
  tune_aio_events: false
  tune_ballast_file: false
  tune_clocksource: false
  tune_coredump: false
  tune_cpu: false
  tune_disk_irq: false
  tune_disk_nomerges: false
  tune_disk_scheduler: false
  tune_disk_write_cache: false
  tune_fstrim: false
  tune_network: false
  tune_swappiness: false
  tune_transparent_hugepages: false
schema_registry:
  schema_registry_api:
  - address: 0.0.0.0
    name: external
    port: 8081
EOF
  )
  echo "$actual"
  diff <(echo "$actual") <(echo "$expected") && break
  echo "Retrying... ({$retries} left)"
  sleep 5
  ((retries = retries - 1))
done
