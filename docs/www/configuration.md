---
title: Custom configuration
order: 5
---
# Custom configuration

The redpanda configuration is by default loaded from and persisted to
`/etc/redpanda/redpanda.yaml`. It is broadly divided into 2 sections, `redpanda`
and `rpk`.

The `redpanda` section contains all the runtime configuration, such as the
cluster member IPs, the node ID, data directory, and so on. The `rpk` section
contains configuration related to tuning the machine that redpanda will run on.

Here’s a sample of the config. Only include the sections that you want to customize.

```yaml
# organization and cluster_id help Vectorized identify your system.
organization: ""
cluster_id: ""

redpanda:
  # Path where redpanda will keep the data.
  # Required.
  data_directory: "var/lib/redpanda/data"
    
  # Unique id identifying the node in the cluster.
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
  
  # List of the seed servers used to join current cluster. If the seed_server list is
  # empty the node will be a cluster root and it will form a new cluster
  # Default: []
  seed_servers:
    - address: "192.167.32.78"
      port: 33145
  
  # Number of partitions for the internal raft metadata topic.
  # Default: 7
  seed_server_meta_topic_partitions: 7

  # The raft leader heartbeat interval in milliseconds.
  # Default: 150
  raft_heartbeat_interval_ms: 150
  
  # Minimum redpanda version
  min_version: 0
  
  # Maximum redpanda version
  max_version: 1
  
  # Manage CPU scheduling.
  # Default: false
  use_scheduling_groups: false 
  
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
  # Default: 1
  default_topic_replications: 1
  
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

# The REST API (alpha)
# This top-level config node enables the REST Proxy
pandaproxy:
  # IP and port to listen for Kafka REST API requests.
  # Default: 0.0.0.0:8082
  pandaproxy_api: 
    address: "0.0.0.0"
    port: 8082

  # TLS configuration for the Kafka REST API.
  # Default: null
  pandaproxy_api_tls:
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

  # REST API address and port to publish to client
  # Default: from pandaproxy_api
  advertised_pandaproxy_api:
    address: "redpanda-rest-0.my.domain.com."
    port: 8082

# The REST API client
pandaproxy_client:
  # List of address and port of the brokers
  # Default: "127.0.0.1:9092
  broker:
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

rpk:
  # TLS configuration to allow rpk to make requests to the redpanda API.
  tls:
    # The path to the root CA certificate (PEM).
    truststore_file: ""
    # The path to the client certificate (PEM). Only required if client authentication is
    # enabled in the broker.
    cert_file: ""
    # The path to the client certificate key (PEM). Only required if client authentication is
    # enabled in the broker.
    key_file: ""

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

  # (Optional) The vendor, VM type and storage device type that redpanda will run on, in
  # the format <vendor>:<vm>:<storage>. This hints to rpk which configuration values it
  # should use for the redpanda IO scheduler.
  # Default: ''
  well_known_io: "aws:i3.xlarge:default"
```
