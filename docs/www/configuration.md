---
title: Custom configuration
order: 5
---
# Custom configuration

The redpanda configuration is by default loaded from and persisted to
`/etc/redpanda/redpanda.yaml`. It is broadly divided into a few sections:

- `redpanda` - The runtime configuration parameters, such as the cluster member IPs, the node ID, data directory
- `pandaproxy` - Parameters for the Redpanda REST API
- `pandaproxy_client` - Parameters for the REST API client that Redpanda uses to make calls to other nodes
- `rpk` - Configuration related to tuning the container that redpanda

To create a simple config file that you can customize, run:

```
rpk config init
```

## Sample configuration

Here’s a sample of the config. The [configuration reference](#Config-parameter-reference) shows a more complete list of the configuration options.

This is not a valid Redpanda configuration file, but it shows the parameters that you can configure in the config file.
Only include the sections that you want to customize.

```yaml
# organization and cluster_id help Vectorized identify your system.
organization: ""
cluster_id: ""

redpanda:
  # Path where redpanda will keep the data.
  # Required.
  data_directory: "var/lib/redpanda/data"
    
  # Unique ID identifying the node in the cluster.
  # Required.
  node_id: 1
   
  # Skips most of the checks performed at startup, not recomended for production use.
  # Default: false
  developer_mode: false

  # The log segment size in bytes.
  # Default: 1GiB
  log_segment_size: 1073741824

  # The compacted log segment size in bytes.
  # Default: 256MiB
  compacted_log_segment_size: 268435456

  # The maximum compacted log segment size in bytes. The compaction process will
  # attempt to combine segments to achieve a higher compaction rate. This maximum
  # size will control how large a segment may become during this process.
  # Default: 5 GiB
  max_compacted_log_segment_size: 5368709120
    
  # Enable the admin API.
  # Default: true
  enable_admin_api: true
  
  # Admin API doc directory.
  # Default: /usr/share/redpanda/admin-api-doc
  admin_api_doc_dir: "/usr/share/redpanda/admin-api-doc"
    
  # Address and port of admin server.
  # Default: 127.0.0.1:9644
  admin:
    address: "0.0.0.0"
    port: 9644
  
  # TLS configuration for the admin server.
  # Default: null
  admin_api_tls:
    # Whether to enable TLS for the admin server.
    enabled: false
    # Require client authentication
    require_client_auth: false
    # The path to the server certificate PEM file.
    cert_file: ""
    # The path to the server key PEM file
    key_file: ""
    # The path to the truststore PEM file. Only required if client authentication
    # is enabled.
    truststore_file: ""
  
  # The IP address and port for the internal RPC server.
  # Default: 127.0.0.0:33145
  rpc_server:
    address: "0.0.0.0"
    port: 33145
   
  # TLS configuration for the RPC server.
  # Default: null
  rpc_server_tls:
    # Whether to enable TLS for the RPC server.
    enabled: false
    # Require client authentication
    require_client_auth: false
    # The path to the server certificate PEM file.
    cert_file: ""
    # The path to the server key PEM file
    key_file: ""
    # The path to the truststore PEM file. Only required if client authentication
    # is enabled.
    truststore_file: ""

  # Address of RPC endpoint published to other cluster members.
  # Default: 0.0.0.0:33145
  advertised_rpc_api:
    address: "0.0.0.0"
    port: 33145
  
  # Multiple listeners are also supported as per KIP-103.
  # The names must match those in advertised_kafka_api
  kafka_api:
  - address: "0.0.0.0"
    name: internal
    port: 9092
  - address: "0.0.0.0"
    name: external
    port: 9093

  # A list of TLS configurations for the Kafka API listeners.
  # Default: null
  kafka_api_tls:
    # The name of the specific listener this TLS to which this config
    # will be applied. The names must match those in kafka_api.
  - name: "external"
    # Whether to enable TLS for the Kafka API.
    enabled: true
    # Require client authentication
    require_client_auth: false
    # The path to the server certificate PEM file.
    cert_file: "certs/tls-cert.pem"
    # The path to the server key PEM file
    key_file: "certs/tls-key.pem"
    # The path to the truststore PEM file. Only required if client authentication
    # is enabled.
    truststore_file: "certs/tls-ca.pem"

  - name: "internal"
    enabled: false

  # Multiple listeners are also supported as per KIP-103.
  # The names must match those in kafka_api
  advertised_kafka_api:
  - address: 0.0.0.0
    name: internal
    port: 9092
  - address: redpanda-0.my.domain.com.
    name: external
    port: 9093
  
  # List of the seed server IP addresses and ports used to join current cluster.
  # If the seed_server list is empty the node will be a cluster root and it will form a new cluster.
  # Default: []
  seed_servers:
    - host:
      address: 192.168.0.1
      port: 33145

  # The raft leader heartbeat interval in milliseconds.
  # Default: 150
  raft_heartbeat_interval_ms: 150
  
  # Default number of quota tracking windows.
  # Default: 10
  default_num_windows: 10
  
  # Default quota tracking window size in milliseconds.
  # Default: 1s
  default_window_sec: 1000
  
  # Quota manager garbage collection frequency in milliseconds.
  # Default: 30s
  quota_manager_gc_sec: 30000
  
  # Target quota byte rate (bytes per second).
  # Default: 2GiB
  target_quota_byte_rate: 2147483648
  
  # Cluster identifier.
  # Default: null
  cluster_id: "cluster-id"

  # Rack identifier.
  # Default: null
  rack: "rack-id"
  
  # Disable registering metrics.
  # Default: false
  disable_metrics: false
  
  # The minimum allowed session timeout for registered consumers. Shorter timeouts result
  # in quicker failure detection at the cost of more frequent consumer heartbeating, which
  # can overwhelm broker resources.
  # Default: 6s
  group_min_session_timeout_ms: 6000
  
  # The maximum allowed session timeout for registered consumers. Longer timeouts give
  # consumers more time to process messages in between heartbeats at the cost of a longer
  # time to detect failures.
  # Default: 300s
  group_max_session_timeout_ms: 300000
  
  # Extra delay (in milliseconds) added to the rebalance phase to wait for new members.
  # Default: 300ms
  group_initial_rebalance_delay: 300
  
  # Timeout (in milliseconds) for new member joins.
  # Default: 30s
  group_new_member_join_timeout: 30000
  
  # Interval (in milliseconds) for metadata dissemination batching.
  # Default: 3s
  metadata_dissemination_interval_ms: 3000

  # Time to wait (in millisconds) for next read in fetch request when requested min bytes wasn't reached
  # Default: 1ms
  fetch_reads_debounce_timeout: 1

  # Delete segments older than this.
  # Default; 1 week
  delete_retention_ms: 604800000
  
  # How often do we trigger background compaction.
  # Default: 5min
  log_compaction_interval_ms: 300000

  # Max bytes per partition on disk before triggering a compaction.
  # Default: null
  retention_bytes: 1024
  
  # Number of partitions in the internal group membership topic.
  # Default: 1
  group_topic_partitions: 1
  
  # Default replication factor for new topics.
  # Default: 1 (In v21.8.1 and higher, the default is 3 for Kubernetes clusters with at least 3 nodes)
  default_topic_replications: 1

  # Replication factor for a transaction coordinator topic.
  # Not required
  # Default: 1 (In v21.8.1 and higher, the default is 3 for Kubernetes clusters with at least 3 nodes)
  transaction_coordinator_replication: 1

  # Replication factor for an ID allocator topic.
  # Not required
  # Default: 1 (In v21.8.1 and higher, the default is 3 for Kubernetes clusters with at least 3 nodes)
  id_allocator_replication: 1
  
  # Timeout (in milliseconds) to wait when creating a new topic.
  # Default: 2s
  create_topic_timeout_ms: 2000
  
  # Timeout (in milliseconds) to wait for leadership in metadata cache.
  # Default: 5s
  wait_for_leader_timeout_ms: 5000
  
  # Default number of partitions per topic.
  # Default: 1
  default_topic_partitions: 1
      
  # Disable batch cache in log manager.
  # Default: false
  disable_batch_cache: false
      
  # Election timeout expressed in milliseconds.
  # Default: 1.5s
  election_timeout_ms: 1500
      
  # Kafka group recovery timeout expressed in milliseconds.
  # Default: 30s
  kafka_group_recovery_timeout_ms: 30000
      
  # Timeout for append entries requests issued while replicating entries.
  # Default: 3s
  replicate_append_timeout_ms: 3000
      
  # Timeout for append entries requests issued while updating a stale follower.
  # Default: 5s
  recovery_append_timeout_ms: 5000

  # Max size of requests cached for replication in bytes
  # Default 1 MiB
  raft_replicate_batch_window_size: 1048576

  # Minimum batch cache reclaim size.
  # Default: 128 KiB
  reclaim_min_size: 131072
      
  # Maximum batch cache reclaim size.
  # Default: 4MiB
  reclaim_max_size: 4194304
  
  # Length of time (in milliseconds) in which reclaim sizes grow.
  # Default: 3s
  reclaim_growth_window: 3000
      
  # Length of time (in milliseconds) above which growth is reset.
  # 10s
  reclaim_stable_window: 10000
      
  # Allow topic auto creation.
  # Default: false
  auto_create_topics_enabled: false
  
  # Enable pid file. You probably don't want to change this.
  # Default: true
  enable_pid_file: true
      
  # Key-value store flush interval (in milliseconds).
  # Default: 10ms
  kvstore_flush_interval: 10
      
  # Key-value maximum segment size (bytes).
  # Default: 16 MiB
  kvstore_max_segment_size: 16777216
  
  # Fail-safe maximum throttle delay on kafka requests.
  # Default: 60s
  max_kafka_throttle_delay_ms: 60000
  
  # Raft I/O timeout.
  # Default: 10s
  raft_io_timeout_ms: 10000

  # Time between cluster join retries in milliseconds
  # Default: 5s
  join_retry_timeout_ms: 5000

  # Timeout for a timeout now request in milliseconds
  # Default: 1s
  raft_timeout_now_timeout_ms: 1000

  # Timeout waiting for follower recovery when transferring leadership
  # Default: 10s
  raft_transfer_leader_recovery_timeout_ms: 10000

  # Free cache when segments roll
  # Default: false
  release_cache_on_segment_roll: 10000

  # Maximum delay in milliseconds until buffered data is written
  # Default: 1s
  segment_appender_flush_timeout_ms: 1000

  # Minimum time before which unused session will get evicted from sessions. Maximum time after which inactive session will be deleted is twice the given configuration value
  # Default: 60s
  fetch_session_eviction_timeout_ms: 60000

# The redpanda REST API provides a RESTful interface for producing and consuming messages with redpanda.
# To disable the REST API, remove this top-level config node
pandaproxy:
  # A list of address and port to listen for Kafka REST API requests.
  # Default: 0.0.0.0:8082
  pandaproxy_api: 
  - address: "0.0.0.0"
    name: internal
    port: 8082
  - address: "0.0.0.0"
    name: external
    port: 8083

  # A list of TLS configurations for the REST API.
  # Default: null
  pandaproxy_api_tls:
  - name: external
    # Whether to enable TLS.
    enabled: false
    # Require client authentication
    require_client_auth: false
    # The path to the server certificate PEM file.
    cert_file: ""
    # The path to the server key PEM file
    key_file: ""
    # The path to the truststore PEM file. Only required if client
    # authentication is enabled.
    truststore_file: ""
  - name: internal
    enabled: false

  # A list of address and port for the REST API to publish to client
  # Default: from pandaproxy_api
  advertised_pandaproxy_api:
    - address: 0.0.0.0
      name: internal
      port: 8082
    - address: "redpanda-rest-0.my.domain.com."
      name: external
      port: 8083

  # How long to wait for an idle consumer before removing it.
  # Default: 60000
  consumer_instance_timeout_ms: 60000

# The REST API client
pandaproxy_client:
  # List of address and port of the brokers
  # Default: "127.0.0.1:9092
  brokers:
   - address: "127.0.0.1"
     port: 9092

  # TLS configuration for the brokers
  broker_tls:
    # Whether to enable TLS.
    enabled: false
    # Require client authentication
    require_client_auth: false
    # The path to the server certificate PEM file.
    cert_file: ""
    # The path to the server key PEM file
    key_file: ""
    # The path to the truststore PEM file. Only required if client authentication
    # is enabled.
    truststore_file: ""

  # Number of times to retry a request to a broker
  # Default: 5
  retries: 5

  # Delay (in milliseconds) for initial retry backoff
  # Default: 100ms
  retry_base_backoff_ms: 100

  # Number of records to batch before sending to broker
  # Default: 1000
  produce_batch_record_count: 1000

  # Number of bytes to batch before sending to broker
  # Defautl 1MiB
  produce_batch_size_bytes: 1048576

  # Delay (in milliseconds) to wait before sending batch
  # Default: 100ms
  produce_batch_delay_ms: 100

  # Interval (in milliseconds) for consumer request timeout
  # Default: 100ms
  consumer_request_timeout_ms: 100

  # Max bytes to fetch per request
  # Default: 1MiB
  consumer_request_max_bytes: 1048576
      
  # Timeout (in milliseconds) for consumer session
  # Default: 10s
  consumer_session_timeout_ms: 10000

  # Timeout (in milliseconds) for consumer rebalance
  # Default: 2s
  consumer_rebalance_timeout_ms: 2000

  # Interval (in milliseconds) for consumer heartbeats
  # Default: 500ms
  consumer_heartbeat_interval_ms: 500

  # SASL mechanism to use for authentication
  # Supported: SCRAM-SHA-{256,512}
  # Default: ""
  # Support for SASL is disabled when no mechanism is specified.
  sasl_mechanism: ""

  # Username for SCRAM authentication mechanisms
  # Default: ""
  scram_username: ""

  # Password for SCRAM authentication mechanisms
  # Default: ""
  scram_password: ""

# The Schema Registry provides a RESTful interface for Schema storage, retrieval, and compatibility.
# To disable the Schema Registry, remove this top-level config node
schema_registry:
  # A list of address and port to listen for Schema Registry API requests.
  # Default: 0.0.0.0:8082
  schema_registry_api: 
  - address: "0.0.0.0"
    name: internal
    port: 8081
  - address: "0.0.0.0"
    name: external
    port: 18081

  # The replication factor of Schema Registry's internal storage topic
  schema_registry_replication_factor: 3

  # A list of TLS configurations for the Schema Registry API.
  # Default: null
  schema_registry_api_tls:
  - name: external
    # Whether to enable TLS.
    enabled: false
    # Require client authentication
    require_client_auth: false
    # The path to the server certificate PEM file.
    cert_file: ""
    # The path to the server key PEM file
    key_file: ""
    # The path to the truststore PEM file. Only required if client
    # authentication is enabled.
    truststore_file: ""
  - name: internal
    enabled: false

# The Schema Registry client config
# See pandaproxy_client for a list of options
schema_registry_client:

rpk:
  # Add optional flags to have rpk start redpanda with specific parameters.
  # The available start flags are found in: /src/v/config/configuration.cc
  additional_start_flags:
    - "--overprovisioned"
    - "--smp=2"
    - "--memory=4G"
    - "--default-log-level=info"

  # The Kafka API configuration
  kafka_api:
    # A list of broker addresses that rpk will use
    brokers:
    - 192.168.72.34:9092
    - 192.168.72.35:9092

    # The TLS configuration to be used when interacting with the Kafka API.
    # If present, TLS will be enabled. If missing or null, TLS will be disabled.
    tls:
      # The path to the client certificate (PEM). Only required if client authentication is
      # enabled in the broker.
      cert_file: ~/certs/cert.pem
      # The path to the client certificate key (PEM). Only required if client authentication is
      # enabled in the broker.
      key_file: ~/certs/key.pem
      # The path to the root CA certificate (PEM).
      truststore_file: ~/certs/ca.pem

    # The SASL config, if enabled in the brokers.
    sasl:
      user: user
      password: pass
      type: SCRAM-SHA-256

  # The Admin API configuration
  admin_api:
    # A list of the nodes' admin API addresses that rpk will use.
    addresses:
    - 192.168.72.34:9644
    - 192.168.72.35:9644
    # The TLS configuration to be used when with the Admin API.
    # If present, TLS will be enabled. If missing or null, TLS will be disabled.
    tls:
      # The path to the client certificate (PEM). Only required if client authentication is
      # enabled in the broker.
      cert_file: ~/certs/admin-cert.pem
      # The path to the client certificate key (PEM). Only required if client authentication is
      # enabled in the broker.
      key_file: ~/certs/admin-key.pem
      # The path to the root CA certificate (PEM).
      truststore_file: ~/certs/admin-ca.pem

  # Available tuners. Set to true to enable, false to disable.

  # Setup NIC IRQs affinity, sets up NIC RPS and RFS, sets up NIC XPS, increases socket
  # listen backlog, increases the number of remembered connection requests, bans the
  # IRQ Balance service from moving distributed IRQs.
  # Default: false
  tune_network: false

  # Sets the preferred I/O scheduler for given block devices.
  # It can work using both the device name or a directory, in which the device where
  # directory is stored will be optimized. Sets either 'none' or 'noop' scheduler
  # if supported.
  # Default: false
  tune_disk_scheduler: false

  # Disables IOPS merging.
  # Default: false
  tune_disk_nomerges: false

  # Distributes IRQs across cores with the method deemed the most appropriate for the
  # current device type (i.e. NVMe).
  # Default: false
  tune_disk_irq: false
  
  # Installs a systemd service to run fstrim weekly, or starts the default fstrim service
  # which comes with most Linux distributions.
  # Default: false
  tune_fstrim: false

  # Disables hyper-threading, sets the ACPI-cpufreq governor to 'performance'. Additionaly
  # if system reboot is allowed: disables Intel P-States, disables Intel C-States,
  # disables Turbo Boost.
  # Default: false
  tune_cpu: true

  # Increases the number of allowed asynchronous IO events.
  # Default: false
  tune_aio_events: false

  # Syncs NTP.
  # Default: false
  tune_clocksource: true

  # Tunes the kernel to prefer keeping processes in-memory instead of swapping them out.
  # Default: false
  tune_swappiness: false
  
  # Enables transparent hugepages (THP) to reduce TLB misses.
  # Default: false
  tune_transparent_hugepages: false

  # Enables memory locking.
  # Default: false
  enable_memory_locking: false

  # Installs a custom script to process coredumps and save them to the given directory.
  # Default: false
  tune_coredump: false

  # The directory where all coredumps will be saved after they're processed.
  # Default: ''
  coredump_dir: "/var/lib/redpanda/coredump"

  # Creates a "ballast" file so that, if a Redpanda node runs out of space,
  # you can delete the ballast file to allow the node to resume operations and then
  # delete a topic or records to reduce the space used by Redpanda.
  # Default: false
  tune_ballast_file: false

  # The path where the ballast file will be created.
  # Default: "/var/lib/redpanda/data/ballast"
  ballast_file_path: "/var/lib/redpanda/data/ballast"

  # The ballast file size.
  # Default: "1GiB"
  ballast_file_size: "1GiB"

  # (Optional) The vendor, VM type and storage device type that redpanda will run on, in
  # the format <vendor>:<vm>:<storage>. This hints to rpk which configuration values it
  # should use for the redpanda IO scheduler.
  # Default: ''
  well_known_io: "aws:i3.xlarge:default"
```

## Config parameter reference

Here is a more comprehensive view of the configration so that you can see all of the available configuration options.

### Required parameters

| Parameter | Description |
| --- | --- |
| `node_id` | Unique ID identifying a node in the cluster |
| `data_directory` | Place where redpanda will keep the data |

### Optional parameters

| Parameter | Description | Default |
| --- | --- | --- |
| `admin` | Address and port of admin server | 127.0.0.1:9644 |
| `admin_api_doc_dir` | Admin API doc directory | /usr/share/redpanda/admin-api-doc |
| `admin_api_tls` | TLS configuration for admin HTTP server | validate_many |
| `advertised_kafka_api` | Address of Kafka API published to the clients | None |
| `advertised_pandaproxy_api` | Rest API address and port to publish to client | None |
| `advertised_rpc_api` | Address of RPC endpoint published to other cluster members | None |
| `alter_topic_cfg_timeout_ms` | Time to wait for entries replication in controller log when executing alter configuration requst | 5s |
| `api_doc_dir` | API doc directory | /usr/share/redpanda/proxy-api-doc |
| `auto_create_topics_enabled` | Allow topic auto creation | false |
| `cloud_storage_access_key` | AWS access key | None |
| `cloud_storage_api_endpoint` | Optional API endpoint | None |
| `cloud_storage_api_endpoint_port` | TLS port override | 443 |
| `cloud_storage_bucket` | AWS bucket that should be used to store data | None |
| `cloud_storage_disable_tls` | Disable TLS for all S3 connections | false |
| `cloud_storage_enabled` | Enable archival storage | false |
| `cloud_storage_max_connections` | Max number of simultaneous uploads to S3 | 20 |
| `cloud_storage_reconciliation_ms` | Interval at which the archival service runs reconciliation (ms) | 10s |
| `cloud_storage_region` | AWS region that houses the bucket used for storage | None |
| `cloud_storage_secret_key` | AWS secret key | None |
| `cloud_storage_trust_file` | Path to certificate that should be used to validate server certificate during TLS handshake | None |
| `compacted_log_segment_size` | How large in bytes should each compacted log segment be (default 256MiB) | 256MB |
| `controller_backend_housekeeping_interval_ms` | Interval between iterations of controller backend housekeeping loop | 1s |
| `coproc_max_batch_size` | Maximum amount of bytes to read from one topic read | 32kb |
| `coproc_max_inflight_bytes` | Maximum amountt of inflight bytes when sending data to wasm engine | 10MB |
| `coproc_max_ingest_bytes` | Maximum amount of data to hold from input logs in memory | 640kb |
| `coproc_offset_flush_interval_ms` | Interval for which all coprocessor offsets are flushed to disk | 300000ms (5 min) |
| `coproc_supervisor_server` | IpAddress and port for supervisor service | 127.0.0.1:43189 |
| `create_topic_timeout_ms` | Timeout (ms) to wait for new topic creation | 2000ms |
| `dashboard_dir` | serve http dashboard on / url | None |
| `default_num_windows` | Default number of quota tracking windows | 10 |
| `default_topic_partitions` | Default number of partitions per topic | 1 |
| `default_topic_replications` | Default replication factor for new topics | 1 (In v21.8.1 and higher, the default is 3 for Kubernetes clusters with at least 3 nodes) |
| `transaction_coordinator_replication` | Replication factor for a transaction coordinator topic | 1 (In v21.8.1 and higher, the default is 3 for Kubernetes clusters with at least 3 nodes) |
| `id_allocator_replication` | Replication factor for an ID allocator topic | 1 (In v21.8.1 and higher, the default is 3 for Kubernetes clusters with at least 3 nodes) |
| `default_window_sec` | Default quota tracking window size in milliseconds | 1000ms |
| `delete_retention_ms` | delete segments older than this (default 1 week) | 10080min |
| `developer_mode` | Skips most of the checks performed at startup | Optional |
| `disable_batch_cache` | Disable batch cache in log manager | false |
| `disable_metrics` | Disable registering metrics | false |
| `enable_admin_api` | Enable the admin API | true |
| `enable_coproc` | Enable coprocessing mode | false |
| `enable_idempotence` | Enable idempotent producer | false |
| `enable_pid_file` | Enable pid file; You probably don't want to change this | true |
| `enable_sasl` | Enable SASL authentication for Kafka connections | false |
| `enable_transactions` | Enable transactions | false |
| `fetch_reads_debounce_timeout` | Time to wait for next read in fetch request when requested min bytes wasn't reached | 1ms |
| `fetch_session_eviction_timeout_ms` | Minimum time before which unused session will get evicted from sessions; Maximum time after which inactive session will be deleted is two time given configuration valuecache | 60s |
| `group_initial_rebalance_delay` | Extra delay (ms) added to rebalance phase to wait for new members | 300ms |
| `group_max_session_timeout_ms` | The maximum allowed session timeout for registered consumers; Longer timeouts give consumers more time to process messages in between heartbeats at the cost of a longer time to detect failures; Default quota tracking window size in milliseconds | 300s |
| `group_min_session_timeout_ms` | The minimum allowed session timeout for registered consumers; Shorter timeouts result in quicker failure detection at the cost of more frequent consumer heartbeating | Optional |
| `group_new_member_join_timeout` | Timeout for new member joins | 30000ms |
| `group_topic_partitions` | Number of partitions in the internal group membership topic | 1 |
| `id_allocator_batch_size` | ID allocator allocates messages in batches (each batch is a one log record) and then serves requests from memory without touching the log until the batch is exhausted | 1000 |
| `id_allocator_log_capacity` | Capacity of the id_allocator log in number of messages; Once it reached id_allocator_stm should compact the log | 100 |
| `join_retry_timeout_ms` | Time between cluster join retries in milliseconds | 5s |
| `kafka_api` | Address and port of an interface to listen for Kafka API requests | 127.0.0.1:9092 |
| `kafka_api_tls` | TLS configuration for Kafka API endpoint | None |
| `kafka_group_recovery_timeout_ms` | Kafka group recovery timeout expressed in milliseconds | 30000ms |
| `kafka_qdc_depth_alpha` | Smoothing factor for kafka queue depth control depth tracking | 0.8 |
| `kafka_qdc_depth_update_ms` | Update frequency for kafka queue depth control | 7s |
| `kafka_qdc_enable` | Enable kafka queue depth control | false |
| `kafka_qdc_idle_depth` | Queue depth when idleness is detected in kafka queue depth control | 10 |
| `kafka_qdc_latency_alpha` | Smoothing parameter for kafka queue depth control latency tracking | 0.002 |
| `kafka_qdc_max_depth` | Maximum queue depth used in kafka queue depth control | 100 |
| `kafka_qdc_max_latency_ms` | Max latency threshold for kafka queue depth control depth tracking | 80ms |
| `kafka_qdc_min_depth` | Minimum queue depth used in kafka queue depth control | 1 |
| `kafka_qdc_window_count` | Number of windows used in kafka queue depth control latency tracking | 12 |
| `kafka_qdc_window_size_ms` | Window size for kafka queue depth control latency tracking | 1500ms |
| `kvstore_flush_interval` | Key-value store flush interval (ms) | 10ms |
| `kvstore_max_segment_size` | Key-value maximum segment size (bytes) | 16MB |
| `log_cleanup_policy` | Default topic cleanup policy | deletion |
| `log_compaction_interval_ms` | How often do we trigger background compaction | 5min |
| `log_compression_type` | Default topic compression type | producer |
| `log_message_timestamp_type` | Default topic messages timestamp type | create_time |
| `log_segment_size` | How large in bytes should each log segment be (default 1G) | 1GB |
| `max_compacted_log_segment_size` | Max compacted segment size after consolidation | 5GB |
| `max_kafka_throttle_delay_ms` | Fail-safe maximum throttle delay on kafka requests | 60000ms |
| `metadata_dissemination_interval_ms` | Interaval for metadata dissemination batching | 3000ms |
| `metadata_dissemination_retries` | Number of attempts of looking up a topic's meta data like shard before failing a request | 10 |
| `metadata_dissemination_retry_delay_ms` | Delay before retry a topic lookup in a shard or other meta tables | 500ms |
| `pandaproxy_api` | Rest API listen address and port | 0.0.0.0:8082 |
| `pandaproxy_api_tls` | TLS configuration for Pandaproxy api | validate_many |
| `quota_manager_gc_sec` | Quota manager GC frequency in milliseconds | 30000ms |
| `rack` | Rack identifier | None |
| `raft_election_timeout_ms` | Election timeout expressed in milliseconds | 1500ms |
| `raft_heartbeat_interval_ms` | Milliseconds for raft leader heartbeats | 150ms |
| `raft_heartbeat_timeout_ms` | raft heartbeat RPC timeout | 3s |
| `raft_io_timeout_ms` | Raft I/O timeout | 10000ms |
| `raft_replicate_batch_window_size` | Max size of requests cached for replication | 1MB |
| `raft_timeout_now_timeout_ms` | Timeout for a timeout now request | 1s |
| `raft_transfer_leader_recovery_timeout_ms` | Timeout waiting for follower recovery when transferring leadership | 10s |
| `readers_cache_eviction_timeout_ms` | Duration after which inactive readers will be evicted from cache | 30s |
| `reclaim_growth_window` | Length of time in which reclaim sizes grow | 3000ms |
| `reclaim_max_size` | Maximum batch cache reclaim size | 4MB |
| `reclaim_min_size` | Minimum batch cache reclaim size | 128KB |
| `reclaim_stable_window` | Length of time above which growth is reset | 10000ms |
| `recovery_append_timeout_ms` | Timeout for append entries requests issued while updating stale follower | 5s |
| `release_cache_on_segment_roll` | Free cache when segments roll | false |
| `replicate_append_timeout_ms` | Timeout for append entries requests issued while replicating entries | 3s |
| `retention_bytes` | max bytes per partition on disk before triggering a compaction | None |
| `rm_sync_timeout_ms` | Time to wait state catch up before rejecting a request | 2000ms |
| `rm_violation_recovery_policy` | Describes how to recover from an invariant violation happened on the partition level | crash |
| `rpc_server` | IP address and port for RPC server | 127.0.0.1:33145 |
| `rpc_server_tls` | TLS configuration for RPC server | validate |
| `seed_servers` | List of the seed servers used to join current cluster; If the seed_server list is empty the node will be a cluster root and it will form a new cluster | None |
| `segment_appender_flush_timeout_ms` | Maximum delay until buffered data is written | 1sms |
| `superusers` | List of superuser usernames | None |
| `target_quota_byte_rate` | Target quota byte rate in bytes per second | 2GB |
| `tm_sync_timeout_ms` | Time to wait state catch up before rejecting a request | 2000ms |
| `tm_violation_recovery_policy` | Describes how to recover from an invariant violation happened on the transaction coordinator level | crash |
| `transactional_id_expiration_ms` | Producer ids are expired once this time has elapsed after the last write with the given producer ID | 10080min |
| `wait_for_leader_timeout_ms` | Timeout (ms) to wait for leadership in metadata cache | 5000ms |
