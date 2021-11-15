// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"

#include "config/base_property.h"
#include "config/node_config.h"
#include "model/metadata.h"
#include "storage/chunk_cache.h"
#include "units.h"

#include <cstdint>

namespace config {
using namespace std::chrono_literals;

uint32_t default_raft_non_local_requests() {
    /**
     * raft max non local requests
     * - up to 7000 groups per core
     * - up to 256 concurrent append entries per group
     * - additional requests like (vote, snapshot, timeout now)
     *
     * All the values have to be multiplied by core count minus one since
     * part of the requests will be core local
     *
     * 7000*256 * (number of cores-1) + 10 * 7000 * (number of cores-1)
     *         ^                                 ^
     * append entries requests          additional requests
     */
    static constexpr uint32_t max_partitions_per_core = 7000;
    static constexpr uint32_t max_append_requests_per_follower = 256;
    static constexpr uint32_t additional_requests_per_follower = 10;

    return max_partitions_per_core
           * (max_append_requests_per_follower + additional_requests_per_follower)
           * (ss::smp::count - 1);
}

configuration::configuration()
  : developer_mode(
    *this,
    "developer_mode",
    "Skips most of the checks performed at startup, not recomended for "
    "production use",
    required::no,
    false)
  , log_segment_size(
      *this,
      "log_segment_size",
      "How large in bytes should each log segment be (default 1G)",
      required::no,
      1_GiB)
  , compacted_log_segment_size(
      *this,
      "compacted_log_segment_size",
      "How large in bytes should each compacted log segment be (default "
      "256MiB)",
      required::no,
      256_MiB)
  , readers_cache_eviction_timeout_ms(
      *this,
      "readers_cache_eviction_timeout_ms",
      "Duration after which inactive readers will be evicted from cache",
      required::no,
      30s)
  , rpc_server_listen_backlog(
      *this,
      "rpc_server_listen_backlog",
      "TCP connection queue length for Kafka server and internal RPC server",
      required::no,
      std::nullopt)
  , rpc_server_tcp_recv_buf(
      *this,
      "rpc_server_tcp_recv_buf",
      "TCP receive buffer size in bytes.",
      required::no,
      std::nullopt,
      32_KiB)
  , rpc_server_tcp_send_buf(
      *this,
      "rpc_server_tcp_send_buf",
      "TCP transmit buffer size in bytes.",
      required::no,
      std::nullopt,
      32_KiB)
  , enable_coproc(
      *this, "enable_coproc", "Enable coprocessing mode", required::no, false)
  , coproc_max_inflight_bytes(
      *this,
      "coproc_max_inflight_bytes",
      "Maximum amountt of inflight bytes when sending data to wasm engine",
      required::no,
      10_MiB)
  , coproc_max_ingest_bytes(
      *this,
      "coproc_max_ingest_bytes",
      "Maximum amount of data to hold from input logs in memory",
      required::no,
      640_KiB)
  , coproc_max_batch_size(
      *this,
      "coproc_max_batch_size",
      "Maximum amount of bytes to read from one topic read",
      required::no,
      32_KiB)
  , coproc_offset_flush_interval_ms(
      *this,
      "coproc_offset_flush_interval_ms",
      "Interval for which all coprocessor offsets are flushed to disk",
      required::no,
      300000ms) // five minutes
  , seed_server_meta_topic_partitions(
      *this,
      "seed_server_meta_topic_partitions",
      "Number of partitions in internal raft metadata topic",
      required::no,
      7)
  , raft_heartbeat_interval_ms(
      *this,
      "raft_heartbeat_interval_ms",
      "Milliseconds for raft leader heartbeats",
      required::no,
      std::chrono::milliseconds(150))
  , raft_heartbeat_timeout_ms(
      *this,
      "raft_heartbeat_timeout_ms",
      "raft heartbeat RPC timeout",
      required::no,
      3s)
  , raft_heartbeat_disconnect_failures(
      *this,
      "raft_heartbeat_disconnect_failures",
      "After how many failed heartbeats to forcibly close an unresponsive TCP "
      "connection.  Set to 0 to disable force disconnection.",
      required::no,
      3)
  , min_version(
      *this, "min_version", "minimum redpanda compat version", required::no, 0)
  , max_version(
      *this, "max_version", "max redpanda compat version", required::no, 1)
  , use_scheduling_groups(
      *this,
      "use_scheduling_groups",
      "Manage CPU scheduling",
      required::no,
      false)
  , enable_admin_api(
      *this, "enable_admin_api", "Enable the admin API", required::no, true)
  , default_num_windows(
      *this,
      "default_num_windows",
      "Default number of quota tracking windows",
      required::no,
      10)
  , default_window_sec(
      *this,
      "default_window_sec",
      "Default quota tracking window size in milliseconds",
      required::no,
      std::chrono::milliseconds(1000))
  , quota_manager_gc_sec(
      *this,
      "quota_manager_gc_sec",
      "Quota manager GC frequency in milliseconds",
      required::no,
      std::chrono::milliseconds(30000))
  , target_quota_byte_rate(
      *this,
      "target_quota_byte_rate",
      "Target quota byte rate (bytes per second) - 2GB default",
      required::no,
      2_GiB)
  , cluster_id(
      *this, "cluster_id", "Cluster identifier", required::no, std::nullopt)
  , disable_metrics(
      *this,
      "disable_metrics",
      "Disable registering metrics",
      required::no,
      false)
  , group_min_session_timeout_ms(
      *this,
      "group_min_session_timeout_ms",
      "The minimum allowed session timeout for registered consumers. Shorter "
      "timeouts result in quicker failure detection at the cost of more "
      "frequent "
      "consumer heartbeating, which can overwhelm broker resources.",
      required::no,
      6000ms)
  , group_max_session_timeout_ms(
      *this,
      "group_max_session_timeout_ms",
      "The maximum allowed session timeout for registered consumers. Longer "
      "timeouts give consumers more time to process messages in between "
      "heartbeats at the cost of a longer time to detect failures. "
      "Default quota tracking window size in milliseconds",
      required::no,
      300s)
  , group_initial_rebalance_delay(
      *this,
      "group_initial_rebalance_delay",
      "Extra delay (ms) added to rebalance phase to wait for new members",
      required::no,
      300ms)
  , group_new_member_join_timeout(
      *this,
      "group_new_member_join_timeout",
      "Timeout for new member joins",
      required::no,
      30'000ms)
  , metadata_dissemination_interval_ms(
      *this,
      "metadata_dissemination_interval_ms",
      "Interaval for metadata dissemination batching",
      required::no,
      3'000ms)
  , metadata_dissemination_retry_delay_ms(
      *this,
      "metadata_dissemination_retry_delay_ms",
      "Delay before retry a topic lookup in a shard or other meta tables",
      required::no,
      0'500ms)
  , metadata_dissemination_retries(
      *this,
      "metadata_dissemination_retries",
      "Number of attempts of looking up a topic's meta data like shard before "
      "failing a request",
      required::no,
      30)
  , tm_sync_timeout_ms(
      *this,
      "tm_sync_timeout_ms",
      "Time to wait state catch up before rejecting a request",
      required::no,
      10s)
  , tm_violation_recovery_policy(
      *this,
      "tm_violation_recovery_policy",
      "Describes how to recover from an invariant violation happened on the "
      "transaction coordinator level",
      required::no,
      model::violation_recovery_policy::crash)
  , rm_sync_timeout_ms(
      *this,
      "rm_sync_timeout_ms",
      "Time to wait state catch up before rejecting a request",
      required::no,
      10s)
  , tx_timeout_delay_ms(
      *this,
      "tx_timeout_delay_ms",
      "Delay before scheduling next check for timed out transactions",
      required::no,
      1000ms)
  , rm_violation_recovery_policy(
      *this,
      "rm_violation_recovery_policy",
      "Describes how to recover from an invariant violation happened on the "
      "partition level",
      required::no,
      model::violation_recovery_policy::crash)
  , fetch_reads_debounce_timeout(
      *this,
      "fetch_reads_debounce_timeout",
      "Time to wait for next read in fetch request when requested min bytes "
      "wasn't reached",
      required::no,
      1ms)
  , alter_topic_cfg_timeout_ms(
      *this,
      "alter_topic_cfg_timeout_ms",
      "Time to wait for entries replication in controller log when executing "
      "alter configuration requst",
      required::no,
      5s)
  , log_cleanup_policy(
      *this,
      "log_cleanup_policy",
      "Default topic cleanup policy",
      required::no,
      model::cleanup_policy_bitflags::deletion)
  , log_message_timestamp_type(
      *this,
      "log_message_timestamp_type",
      "Default topic messages timestamp type",
      required::no,
      model::timestamp_type::create_time)
  , log_compression_type(
      *this,
      "log_compression_type",
      "Default topic compression type",
      required::no,
      model::compression::producer)
  , fetch_max_bytes(
      *this,
      "fetch_max_bytes",
      "Maximum number of bytes returned in fetch request",
      required::no,
      55_MiB)
  , transactional_id_expiration_ms(
      *this,
      "transactional_id_expiration_ms",
      "Producer ids are expired once this time has elapsed after the last "
      "write with the given producer id.",
      required::no,
      10080min)
  , enable_idempotence(
      *this,
      "enable_idempotence",
      "Enable idempotent producer",
      required::no,
      false)
  , enable_transactions(
      *this, "enable_transactions", "Enable transactions", required::no, false)
  , abort_index_segment_size(
      *this,
      "abort_index_segment_size",
      "Capacity (in number of txns) of an abort index segment",
      required::no,
      50000)
  , delete_retention_ms(
      *this,
      "delete_retention_ms",
      "delete segments older than this - default 1 week",
      required::no,
      10080min)
  , log_compaction_interval_ms(
      *this,
      "log_compaction_interval_ms",
      "How often do we trigger background compaction",
      required::no,
      10s)
  , retention_bytes(
      *this,
      "retention_bytes",
      "max bytes per partition on disk before triggering a compaction",
      required::no,
      std::nullopt)
  , group_topic_partitions(
      *this,
      "group_topic_partitions",
      "Number of partitions in the internal group membership topic",
      required::no,
      1)
  , default_topic_replication(
      *this,
      "default_topic_replications",
      "Default replication factor for new topics",
      required::no,
      1)
  , transaction_coordinator_replication(
      *this,
      "transaction_coordinator_replication",
      "Replication factor for a transaction coordinator topic",
      required::no,
      1)
  , id_allocator_replication(
      *this,
      "id_allocator_replication",
      "Replication factor for an id allocator topic",
      required::no,
      1)
  , transaction_coordinator_cleanup_policy(
      *this,
      "transaction_coordinator_cleanup_policy",
      "Cleanup policy for a transaction coordinator topic",
      required::no,
      model::cleanup_policy_bitflags::deletion)
  , transaction_coordinator_delete_retention_ms(
      *this,
      "transaction_coordinator_delete_retention_ms",
      "delete segments older than this - default 1 week",
      required::no,
      10080min)
  , transaction_coordinator_log_segment_size(
      *this,
      "transaction_coordinator_log_segment_size",
      "How large in bytes should each log segment be (default 1G)",
      required::no,
      1_GiB)
  , abort_timed_out_transactions_interval_ms(
      *this,
      "abort_timed_out_transactions_interval_ms",
      "How often look for the inactive transactions and abort them",
      required::no,
      1min)
  , create_topic_timeout_ms(
      *this,
      "create_topic_timeout_ms",
      "Timeout (ms) to wait for new topic creation",
      required::no,
      2'000ms)
  , wait_for_leader_timeout_ms(
      *this,
      "wait_for_leader_timeout_ms",
      "Timeout (ms) to wait for leadership in metadata cache",
      required::no,
      5'000ms)
  , default_topic_partitions(
      *this,
      "default_topic_partitions",
      "Default number of partitions per topic",
      required::no,
      1)
  , disable_batch_cache(
      *this,
      "disable_batch_cache",
      "Disable batch cache in log manager",
      required::no,
      false)
  , raft_election_timeout_ms(
      *this,
      "election_timeout_ms",
      "Election timeout expressed in milliseconds",
      required::no,
      1'500ms)
  , kafka_group_recovery_timeout_ms(
      *this,
      "kafka_group_recovery_timeout_ms",
      "Kafka group recovery timeout expressed in milliseconds",
      required::no,
      30'000ms)
  , replicate_append_timeout_ms(
      *this,
      "replicate_append_timeout_ms",
      "Timeout for append entries requests issued while replicating entries",
      required::no,
      3s)
  , recovery_append_timeout_ms(
      *this,
      "recovery_append_timeout_ms",
      "Timeout for append entries requests issued while updating stale "
      "follower",
      required::no,
      5s)
  , raft_replicate_batch_window_size(
      *this,
      "raft_replicate_batch_window_size",
      "Max size of requests cached for replication",
      required::no,
      1_MiB)
  , raft_learner_recovery_rate(
      *this,
      "raft_learner_recovery_rate",
      "Raft learner recovery rate limit in bytes per sec",
      required::no,
      100_MiB)
  , raft_smp_max_non_local_requests(
      *this,
      "raft_smp_max_non_local_requests",
      "Maximum number of x-core requests pending in Raft seastar::smp group. "
      "(for more details look at `seastar::smp_service_group` documentation)",
      required::no,
      default_raft_non_local_requests())
  , raft_max_concurrent_append_requests_per_follower(
      *this,
      "raft_max_concurrent_append_requests_per_follower",
      "Maximum number of concurrent append entries requests sent by leader to "
      "one follower",
      required::no,
      16)
  , reclaim_min_size(
      *this,
      "reclaim_min_size",
      "Minimum batch cache reclaim size",
      required::no,
      128_KiB)
  , reclaim_max_size(
      *this,
      "reclaim_max_size",
      "Maximum batch cache reclaim size",
      required::no,
      4_MiB)
  , reclaim_growth_window(
      *this,
      "reclaim_growth_window",
      "Length of time in which reclaim sizes grow",
      required::no,
      3'000ms)
  , reclaim_stable_window(
      *this,
      "reclaim_stable_window",
      "Length of time above which growth is reset",
      required::no,
      10'000ms)
  , auto_create_topics_enabled(
      *this,
      "auto_create_topics_enabled",
      "Allow topic auto creation",
      required::no,
      false)
  , enable_pid_file(
      *this,
      "enable_pid_file",
      "Enable pid file. You probably don't want to change this.",
      required::no,
      true)
  , kvstore_flush_interval(
      *this,
      "kvstore_flush_interval",
      "Key-value store flush interval (ms)",
      required::no,
      std::chrono::milliseconds(10))
  , kvstore_max_segment_size(
      *this,
      "kvstore_max_segment_size",
      "Key-value maximum segment size (bytes)",
      required::no,
      16_MiB)
  , max_kafka_throttle_delay_ms(
      *this,
      "max_kafka_throttle_delay_ms",
      "Fail-safe maximum throttle delay on kafka requests",
      required::no,
      60'000ms)
  , raft_io_timeout_ms(
      *this, "raft_io_timeout_ms", "Raft I/O timeout", required::no, 10'000ms)
  , join_retry_timeout_ms(
      *this,
      "join_retry_timeout_ms",
      "Time between cluster join retries in milliseconds",
      required::no,
      5s)
  , raft_timeout_now_timeout_ms(
      *this,
      "raft_timeout_now_timeout_ms",
      "Timeout for a timeout now request",
      required::no,
      1s)
  , raft_transfer_leader_recovery_timeout_ms(
      *this,
      "raft_transfer_leader_recovery_timeout_ms",
      "Timeout waiting for follower recovery when transferring leadership",
      required::no,
      10s)
  , release_cache_on_segment_roll(
      *this,
      "release_cache_on_segment_roll",
      "Free cache when segments roll",
      required::no,
      false)
  , segment_appender_flush_timeout_ms(
      *this,
      "segment_appender_flush_timeout_ms",
      "Maximum delay until buffered data is written",
      required::no,
      std::chrono::milliseconds(1s))
  , fetch_session_eviction_timeout_ms(
      *this,
      "fetch_session_eviction_timeout_ms",
      "Minimum time before which unused session will get evicted from "
      "sessions. Maximum time after which inactive session will be deleted is "
      "two time given configuration value"
      "cache",
      required::no,
      60s)
  , append_chunk_size(
      *this,
      "append_chunk_size",
      "Size of direct write operations to disk",
      required::no,
      16_KiB,
      storage::internal::chunk_cache::validate_chunk_size)
  , max_compacted_log_segment_size(
      *this,
      "max_compacted_log_segment_size",
      "Max compacted segment size after consolidation",
      required::no,
      5_GiB)
  , id_allocator_log_capacity(
      *this,
      "id_allocator_log_capacity",
      "Capacity of the id_allocator log in number of messages. "
      "Once it reached id_allocator_stm should compact the log.",
      required::no,
      100)
  , id_allocator_batch_size(
      *this,
      "id_allocator_batch_size",
      "Id allocator allocates messages in batches (each batch is a "
      "one log record) and then serves requests from memory without "
      "touching the log until the batch is exhausted.",
      required::no,
      1000)
  , enable_sasl(
      *this,
      "enable_sasl",
      "Enable SASL authentication for Kafka connections.",
      required::no,
      false)
  , controller_backend_housekeeping_interval_ms(
      *this,
      "controller_backend_housekeeping_interval_ms",
      "Interval between iterations of controller backend housekeeping loop",
      required::no,
      1s)
  , node_management_operation_timeout_ms(
      *this,
      "node_management_operation_timeout_ms",
      "Timeout for executing node management operations",
      required::no,
      5s)
  , compaction_ctrl_update_interval_ms(
      *this, "compaction_ctrl_update_interval_ms", "", required::no, 30s)
  , compaction_ctrl_p_coeff(
      *this,
      "compaction_ctrl_p_coeff",
      "proportional coefficient for compaction PID controller. This has to be "
      "negative since compaction backlog should decrease when number of "
      "compaction shares increases",
      required::no,
      -12.5)
  , compaction_ctrl_i_coeff(
      *this,
      "compaction_ctrl_i_coeff",
      "integral coefficient for compaction PID controller.",
      required::no,
      0.0)
  , compaction_ctrl_d_coeff(
      *this,
      "compaction_ctrl_d_coeff",
      "derivative coefficient for compaction PID controller.",
      required::no,
      0.2)
  , compaction_ctrl_min_shares(
      *this,
      "compaction_ctrl_min_shares",
      "minimum number of IO and CPU shares that compaction process can use",
      required::no,
      10)
  , compaction_ctrl_max_shares(
      *this,
      "compaction_ctrl_max_shares",
      "maximum number of IO and CPU shares that compaction process can use",
      required::no,
      1000)
  , compaction_ctrl_backlog_size(
      *this,
      "compaction_ctrl_backlog_size",
      "target backlog size for compaction controller. if not set compaction "
      "target compaction backlog would be equal to ",
      required::no,
      std::nullopt)
  , members_backend_retry_ms(
      *this,
      "members_backend_retry_ms",
      "Time between members backend reconciliation loop retries ",
      required::no,
      5s)
  , cloud_storage_enabled(
      *this,
      "cloud_storage_enabled",
      "Enable archival storage",
      required::no,
      false)
  , cloud_storage_access_key(
      *this,
      "cloud_storage_access_key",
      "AWS access key",
      required::no,
      std::nullopt)
  , cloud_storage_secret_key(
      *this,
      "cloud_storage_secret_key",
      "AWS secret key",
      required::no,
      std::nullopt)
  , cloud_storage_region(
      *this,
      "cloud_storage_region",
      "AWS region that houses the bucket used for storage",
      required::no,
      std::nullopt)
  , cloud_storage_bucket(
      *this,
      "cloud_storage_bucket",
      "AWS bucket that should be used to store data",
      required::no,
      std::nullopt)
  , cloud_storage_api_endpoint(
      *this,
      "cloud_storage_api_endpoint",
      "Optional API endpoint",
      required::no,
      std::nullopt)
  , cloud_storage_reconciliation_ms(
      *this,
      "cloud_storage_reconciliation_interval_ms",
      "Interval at which the archival service runs reconciliation (ms)",
      required::no,
      10s)
  , cloud_storage_max_connections(
      *this,
      "cloud_storage_max_connections",
      "Max number of simultaneous uploads to S3",
      required::no,
      20)
  , cloud_storage_disable_tls(
      *this,
      "cloud_storage_disable_tls",
      "Disable TLS for all S3 connections",
      required::no,
      false)
  , cloud_storage_api_endpoint_port(
      *this,
      "cloud_storage_api_endpoint_port",
      "TLS port override",
      required::no,
      443)
  , cloud_storage_trust_file(
      *this,
      "cloud_storage_trust_file",
      "Path to certificate that should be used to validate server certificate "
      "during TLS handshake",
      required::no,
      std::nullopt)
  , cloud_storage_initial_backoff_ms(
      *this,
      "cloud_storage_initial_backoff_ms",
      "Initial backoff time for exponetial backoff algorithm (ms)",
      required::no,
      100ms)
  , cloud_storage_segment_upload_timeout_ms(
      *this,
      "cloud_storage_segment_upload_timeout_ms",
      "Log segment upload timeout (ms)",
      required::no,
      30s)
  , cloud_storage_manifest_upload_timeout_ms(
      *this,
      "cloud_storage_manifest_upload_timeout_ms",
      "Manifest upload timeout (ms)",
      required::no,
      10s)
  , cloud_storage_max_connection_idle_time_ms(
      *this,
      "cloud_storage_max_connection_idle_time_ms",
      "Max https connection idle time (ms)",
      required::no,
      5s)
  , cloud_storage_segment_max_upload_interval_sec(
      *this,
      "cloud_storage_segment_max_upload_interval_sec",
      "Time that segment can be kept locally without uploading it to the "
      "remote storage (sec)",
      required::no,
      std::nullopt)
  , cloud_storage_cache_size(
      *this,
      "cloud_storage_cache_size",
      "Max size of archival cache",
      required::no,
      20_GiB)
  , cloud_storage_cache_check_interval_ms(
      *this,
      "cloud_storage_cache_check_interval",
      "Timeout to check if cache eviction should be triggered",
      required::no,
      30s)
  , superusers(
      *this, "superusers", "List of superuser usernames", required::no, {})
  , kafka_qdc_latency_alpha(
      *this,
      "kafka_qdc_latency_alpha",
      "Smoothing parameter for kafka queue depth control latency tracking.",
      required::no,
      0.002)
  , kafka_qdc_window_size_ms(
      *this,
      "kafka_qdc_window_size_ms",
      "Window size for kafka queue depth control latency tracking.",
      required::no,
      1500ms)
  , kafka_qdc_window_count(
      *this,
      "kafka_qdc_window_count",
      "Number of windows used in kafka queue depth control latency tracking.",
      required::no,
      12)
  , kafka_qdc_enable(
      *this,
      "kafka_qdc_enable",
      "Enable kafka queue depth control.",
      required::no,
      false)
  , kafka_qdc_depth_alpha(
      *this,
      "kafka_qdc_depth_alpha",
      "Smoothing factor for kafka queue depth control depth tracking.",
      required::no,
      0.8)
  , kafka_qdc_max_latency_ms(
      *this,
      "kafka_qdc_max_latency_ms",
      "Max latency threshold for kafka queue depth control depth tracking.",
      required::no,
      80ms)
  , kafka_qdc_idle_depth(
      *this,
      "kafka_qdc_idle_depth",
      "Queue depth when idleness is detected in kafka queue depth control.",
      required::no,
      10)
  , kafka_qdc_min_depth(
      *this,
      "kafka_qdc_min_depth",
      "Minimum queue depth used in kafka queue depth control.",
      required::no,
      1)
  , kafka_qdc_max_depth(
      *this,
      "kafka_qdc_max_depth",
      "Maximum queue depth used in kafka queue depth control.",
      required::no,
      100)
  , kafka_qdc_depth_update_ms(
      *this,
      "kafka_qdc_depth_update_ms",
      "Update frequency for kafka queue depth control.",
      required::no,
      7s)
  , zstd_decompress_workspace_bytes(
      *this,
      "zstd_decompress_workspace_bytes",
      "Size of the zstd decompression workspace",
      required::no,
      8_MiB)
  , full_raft_configuration_recovery_pattern(
      *this,
      "full_raft_configuration_recovery_pattern",
      "Recovery raft configuration on start for NTPs matching pattern",
      required::no,
      {})
  , enable_auto_rebalance_on_node_add(
      *this,
      "enable_auto_rebalance_on_node_add",
      "Enable automatic partition rebalancing when new nodes are added",
      required::no,
      false)
  , enable_leader_balancer(
      *this,
      "enable_leader_balancer",
      "Enable automatic leadership rebalancing",
      required::no,
      true)
  , leader_balancer_idle_timeout(
      *this,
      "leader_balancer_idle_timeout",
      "Leadership rebalancing idle timeout",
      required::no,
      2min)
  , leader_balancer_mute_timeout(
      *this,
      "leader_balancer_mute_timeout",
      "Leadership rebalancing mute timeout",
      required::no,
      5min)
  , leader_balancer_node_mute_timeout(
      *this,
      "leader_balancer_mute_timeout",
      "Leadership rebalancing node mute timeout",
      required::no,
      20s)
  , internal_topic_replication_factor(
      *this,
      "internal_topic_replication_factor",
      "Target replication factor for internal topics",
      required::no,
      3)
  , health_manager_tick_interval(
      *this,
      "health_manager_tick_interval",
      "How often the health manager runs",
      required::no,
      3min) {}

void configuration::load(const YAML::Node& root_node) {
    if (!root_node["redpanda"]) {
        throw std::invalid_argument("'redpanda'root is required");
    }

    const auto& ignore = node().property_names();

    config_store::read_yaml(root_node["redpanda"], ignore);
}

configuration& shard_local_cfg() {
    static thread_local configuration cfg;
    return cfg;
}
} // namespace config
