#!/bin/bash
echo "verify-config-v22.2.sh $*"
retries=20
until [ "$retries" -lt 0 ]; do
  PANDAPROXY_RETRIES=$1
  CRASH_LOOP_LIMIT=$2
  if [ -z "$CRASH_LOOP_LIMIT" ] && [ -z "$PANDAPROXY_RETRIES" ]; then
    echo "requires two argument, pandaproxy retries count and crash loop limit"
    exit 1
  fi
  echo "Fetching configuration from $NAMESPACE/additional-configuration-0"
  actual=$(kubectl -n "$NAMESPACE" exec additional-configuration-0 -- cat /etc/redpanda/redpanda.yaml)
  expected=$(
    cat <<EOF
redpanda:
    data_directory: /var/lib/redpanda/data
    empty_seed_starts_cluster: false
    seed_servers:
        - host:
            address: additional-configuration-0.additional-configuration.${NAMESPACE}.svc.cluster.local.
            port: 33145
    rpc_server:
        address: 0.0.0.0
        port: 33145
    kafka_api:
        - address: 0.0.0.0
          port: 9092
          name: kafka
          authentication_method: none
    admin:
        - address: 0.0.0.0
          port: 9644
          name: admin
    advertised_rpc_api:
        address: additional-configuration-0.additional-configuration.${NAMESPACE}.svc.cluster.local.
        port: 33145
    advertised_kafka_api:
        - address: additional-configuration-0.additional-configuration.${NAMESPACE}.svc.cluster.local.
          port: 9092
          name: kafka
    developer_mode: true
    auto_create_topics_enabled: true
    cloud_storage_segment_max_upload_interval_sec: 1800
    default_topic_partitions: 3
    enable_idempotence: true
    enable_rack_awareness: true
    fetch_reads_debounce_timeout: 10
    group_initial_rebalance_delay: 0
    group_topic_partitions: 3
    internal_topic_replication_factor: 3
    log_segment_size: 536870912
    log_segment_size_min: 1
    storage_min_free_bytes: 10485760
    topic_partitions_per_shard: 1000
rpk:
    tune_network: true
    tune_disk_scheduler: true
    tune_disk_nomerges: true
    tune_disk_write_cache: true
    tune_disk_irq: true
    tune_cpu: true
    tune_aio_events: true
    tune_clocksource: true
    tune_swappiness: true
    coredump_dir: /var/lib/redpanda/coredump
    tune_ballast_file: true
    overprovisioned: true
pandaproxy:
    pandaproxy_api:
        - address: 0.0.0.0
          port: 8082
          name: proxy
    advertised_pandaproxy_api:
        - address: additional-configuration-0.additional-configuration.${NAMESPACE}.svc.cluster.local.
          port: 8082
          name: proxy
pandaproxy_client:
    brokers:
        - address: additional-configuration-0.additional-configuration.${NAMESPACE}.svc.cluster.local.
          port: 9092
    retries: ${PANDAPROXY_RETRIES}
schema_registry:
    schema_registry_api:
        - address: 0.0.0.0
          port: 8081
          name: external
EOF
  )
  echo Actual config:
  echo "$actual"
  echo
  echo Difference:
  diff -b <(echo "$actual") <(echo "$expected") && exit 0
  echo "Retrying... ({$retries} left)"
  sleep 5
  ((retries = retries - 1))
done
echo "ERROR: out of retries"
exit 1