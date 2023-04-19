// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"

#include "config/base_property.h"
#include "config/bounded_property.h"
#include "config/node_config.h"
#include "config/validators.h"
#include "model/metadata.h"
#include "security/gssapi_principal_mapper.h"
#include "security/mtls.h"
#include "storage/chunk_cache.h"
#include "storage/segment_appender.h"
#include "units.h"
#include "utils/bottomless_token_bucket.h"

#include <cstdint>
#include <optional>

namespace config {
using namespace std::chrono_literals;

configuration::configuration()
  : log_segment_size(
    *this,
    "log_segment_size",
    "Default log segment size in bytes for topics which do not set "
    "segment.bytes",
    {.needs_restart = needs_restart::no,
     .example = "2147483648",
     .visibility = visibility::tunable},
    128_MiB,
    {.min = 1_MiB})
  , log_segment_size_min(
      *this,
      "log_segment_size_min",
      "Lower bound on topic segment.bytes: lower values will be clamped to "
      "this limit",
      {.needs_restart = needs_restart::no,
       .example = "16777216",
       .visibility = visibility::tunable},
      1_MiB)
  , log_segment_size_max(
      *this,
      "log_segment_size_max",
      "Upper bound on topic segment.bytes: higher values will be clamped to "
      "this limit",
      {.needs_restart = needs_restart::no,
       .example = "268435456",
       .visibility = visibility::tunable},
      std::nullopt)
  , log_segment_size_jitter_percent(
      *this,
      "log_segment_size_jitter_percent",
      "Random variation to the segment size limit used for each partition",
      {.needs_restart = needs_restart::yes,
       .example = "2",
       .visibility = visibility::tunable},
      5,
      {.min = 0, .max = 99})
  , compacted_log_segment_size(
      *this,
      "compacted_log_segment_size",
      "How large in bytes should each compacted log segment be (default "
      "256MiB)",
      {.needs_restart = needs_restart::no,
       .example = "268435456",
       .visibility = visibility::tunable},
      256_MiB,
      {.min = 1_MiB})
  , readers_cache_eviction_timeout_ms(
      *this,
      "readers_cache_eviction_timeout_ms",
      "Duration after which inactive readers will be evicted from cache",
      {.visibility = visibility::tunable},
      30s)
  , log_segment_ms(
      *this,
      "log_segment_ms",
      "Default log segment lifetime in ms for topics which do not set "
      "segment.ms",
      {.needs_restart = needs_restart::no,
       .example = "3600000",
       .visibility = visibility::user},
      std::chrono::weeks{2},
      {.min = 60s})
  , log_segment_ms_min(
      *this,
      "log_segment_ms_min",
      "Lower bound on topic segment.ms: lower values will be clamped to this "
      "value",
      {.needs_restart = needs_restart::no,
       .example = "60000",
       .visibility = visibility::tunable},
      60s)
  , log_segment_ms_max(
      *this,
      "log_segment_ms_max",
      "Upper bound on topic segment.ms: higher values will be clamped to this "
      "value",
      {.needs_restart = needs_restart::no,
       .example = "31536000000",
       .visibility = visibility::tunable},
      24h * 365)
  , rpc_server_listen_backlog(
      *this,
      "rpc_server_listen_backlog",
      "TCP connection queue length for Kafka server and internal RPC server",
      {.visibility = visibility::user},
      std::nullopt,
      {.min = 1})
  , rpc_server_tcp_recv_buf(
      *this,
      "rpc_server_tcp_recv_buf",
      "Internal RPC TCP receive buffer size in bytes.",
      {.example = "65536"},
      std::nullopt,
      {.min = 32_KiB, .align = 4_KiB})
  , rpc_server_tcp_send_buf(
      *this,
      "rpc_server_tcp_send_buf",
      "Internal RPC TCP transmit buffer size in bytes.",
      {.example = "65536"},
      std::nullopt,
      {.min = 32_KiB, .align = 4_KiB})
  , enable_coproc(
      *this,
      "enable_coproc",
      "Enable coprocessing mode",
      {.visibility = visibility::user},
      false)
  , coproc_max_inflight_bytes(
      *this,
      "coproc_max_inflight_bytes",
      "Maximum amountt of inflight bytes when sending data to wasm engine",
      {.visibility = visibility::tunable},
      10_MiB)
  , coproc_max_ingest_bytes(
      *this,
      "coproc_max_ingest_bytes",
      "Maximum amount of data to hold from input logs in memory",
      {.visibility = visibility::tunable},
      640_KiB)
  , coproc_max_batch_size(
      *this,
      "coproc_max_batch_size",
      "Maximum amount of bytes to read from one topic read",
      {.visibility = visibility::tunable},
      32_KiB)
  , coproc_offset_flush_interval_ms(
      *this,
      "coproc_offset_flush_interval_ms",
      "Interval for which all coprocessor offsets are flushed to disk",
      {.visibility = visibility::tunable},
      300000ms) // five minutes
  , topic_memory_per_partition(
      *this,
      "topic_memory_per_partition",
      "Required memory per partition when creating topics",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1_MiB,
      {
        .min = 1,      // Must be nonzero, it's a divisor
        .max = 100_MiB // Rough 'sanity' limit: a machine with 1GB RAM must be
                       // able to create at least 10 partitions})
      })
  , topic_fds_per_partition(
      *this,
      "topic_fds_per_partition",
      "Required file handles per partition when creating topics",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5,
      {
        .min = 1,   // At least one FD per partition, required for appender.
        .max = 1000 // A system with 1M ulimit should be allowed to create at
                    // least 1000 partitions
      })
  , topic_partitions_per_shard(
      *this,
      "topic_partitions_per_shard",
      "Maximum number of partitions which may be allocated to one shard (CPU "
      "core)",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1000,
      {
        .min = 16,    // Forbid absurdly small values that would prevent most
                      // practical workloads from running
        .max = 131072 // An upper bound to prevent pathological values, although
                      // systems will most likely hit issues far before reaching
                      // this.  This property is principally intended to be
                      // tuned downward from the default, not upward.
      },
      legacy_default<uint32_t>(7000, legacy_version{9}))
  , topic_partitions_reserve_shard0(
      *this,
      "topic_partitions_reserve_shard0",
      "Reserved partition slots on shard (CPU core) 0 on each node.  If this "
      "is >= topic_partitions_per_core, no data partitions will be scheduled "
      "on shard 0",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      2,
      {
        .min = 0,     // It is not mandatory to reserve any capacity
        .max = 131072 // Same max as topic_partitions_per_shard
      })
  , admin_api_require_auth(
      *this,
      "admin_api_require_auth",
      "Whether admin API clients must provide HTTP Basic authentication "
      "headers",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , seed_server_meta_topic_partitions(
      *this, "seed_server_meta_topic_partitions")
  , raft_heartbeat_interval_ms(
      *this,
      "raft_heartbeat_interval_ms",
      "Milliseconds for raft leader heartbeats",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::chrono::milliseconds(150),
      {.min = std::chrono::milliseconds(1)})
  , raft_heartbeat_timeout_ms(
      *this,
      "raft_heartbeat_timeout_ms",
      "raft heartbeat RPC timeout",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      3s,
      {.min = std::chrono::milliseconds(1)})
  , raft_heartbeat_disconnect_failures(
      *this,
      "raft_heartbeat_disconnect_failures",
      "After how many failed heartbeats to forcibly close an unresponsive TCP "
      "connection.  Set to 0 to disable force disconnection.",
      {.visibility = visibility::tunable},
      3)

  , min_version(*this, "min_version")
  , max_version(*this, "max_version")
  , raft_max_recovery_memory(
      *this,
      "raft_max_recovery_memory",
      "Max memory that can be used for reads in raft recovery process by "
      "default 15% of total memory",
      {.needs_restart = needs_restart::no,
       .example = "41943040",
       .visibility = visibility::tunable},
      std::nullopt,
      {.min = 32_MiB})
  , raft_recovery_default_read_size(
      *this,
      "raft_recovery_default_read_size",
      "default size of read issued during raft follower recovery",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      512_KiB,
      {.min = 128, .max = 5_MiB})
  , enable_usage(
      *this,
      "enable_usage",
      "Enables the usage tracking mechanism, storing windowed history of "
      "kafka/cloud_storage metrics over time",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , usage_num_windows(
      *this,
      "usage_num_windows",
      "The number of windows to persist in memory and disk",
      {.needs_restart = needs_restart::no,
       .example = "24",
       .visibility = visibility::tunable},
      24,
      {.min = 2, .max = 86400})
  , usage_window_width_interval_sec(
      *this,
      "usage_window_width_interval_sec",
      "The width of a usage window, tracking cloud and kafka ingress/egress "
      "traffic each interval",
      {.needs_restart = needs_restart::no,
       .example = "3600",
       .visibility = visibility::tunable},
      std::chrono::seconds(3600),
      {.min = std::chrono::seconds(1)})
  , usage_disk_persistance_interval_sec(
      *this,
      "usage_disk_persistance_interval_sec",
      "The interval in which all usage stats are written to disk",
      {.needs_restart = needs_restart::no,
       .example = "300",
       .visibility = visibility::tunable},
      std::chrono::seconds(60 * 5),
      {.min = std::chrono::seconds(1)})
  , use_scheduling_groups(*this, "use_scheduling_groups")
  , enable_admin_api(*this, "enable_admin_api")
  , default_num_windows(
      *this,
      "default_num_windows",
      "Default number of quota tracking windows",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10,
      {.min = 1})
  , default_window_sec(
      *this,
      "default_window_sec",
      "Default quota tracking window size in milliseconds",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::chrono::milliseconds(1000),
      {.min = std::chrono::milliseconds(1)})
  , quota_manager_gc_sec(
      *this,
      "quota_manager_gc_sec",
      "Quota manager GC frequency in milliseconds",
      {.visibility = visibility::tunable},
      std::chrono::milliseconds(30000))
  , target_quota_byte_rate(
      *this,
      "target_quota_byte_rate",
      "Target request size quota byte rate (bytes per second) - 2GB default",
      {.needs_restart = needs_restart::no,
       .example = "1073741824",
       .visibility = visibility::user},
      2_GiB,
      {.min = 1_MiB})
  , target_fetch_quota_byte_rate(
      *this,
      "target_fetch_quota_byte_rate",
      "Target fetch size quota byte rate (bytes per second) - disabled default",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , kafka_admin_topic_api_rate(
      *this,
      "kafka_admin_topic_api_rate",
      "Target quota rate (partition mutations per default_window_sec)",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt,
      {.min = 1})
  , cluster_id(
      *this,
      "cluster_id",
      "Cluster identifier",
      {.needs_restart = needs_restart::no},
      std::nullopt,
      &validate_non_empty_string_opt)
  , disable_metrics(
      *this,
      "disable_metrics",
      "Disable registering metrics exposed on the internal metrics endpoint "
      "(/metrics)",
      base_property::metadata{},
      false)
  , disable_public_metrics(
      *this,
      "disable_public_metrics",
      "Disable registering metrics exposed on the public metrics endpoint "
      "(/public_metrics)",
      base_property::metadata{},
      false)
  , aggregate_metrics(
      *this,
      "aggregate_metrics",
      "Enable aggregations of metrics returned by the prometheus '/metrics' "
      "endpoint. "
      "Metric aggregation is performed by summing the values of samples by "
      "labels. "
      "Aggregations are performed where it makes sense by the shard and/or "
      "partition labels.",
      {.needs_restart = needs_restart::yes},
      false)
  , group_min_session_timeout_ms(
      *this,
      "group_min_session_timeout_ms",
      "The minimum allowed session timeout for registered consumers. Shorter "
      "timeouts result in quicker failure detection at the cost of more "
      "frequent "
      "consumer heartbeating, which can overwhelm broker resources.",
      {.needs_restart = needs_restart::no},
      6000ms)
  , group_max_session_timeout_ms(
      *this,
      "group_max_session_timeout_ms",
      "The maximum allowed session timeout for registered consumers. Longer "
      "timeouts give consumers more time to process messages in between "
      "heartbeats at the cost of a longer time to detect failures. ",
      {.needs_restart = needs_restart::no},
      300s)
  , group_initial_rebalance_delay(
      *this,
      "group_initial_rebalance_delay",
      "Extra delay (ms) added to rebalance phase to wait for new members",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      3s)
  , group_new_member_join_timeout(
      *this,
      "group_new_member_join_timeout",
      "Timeout for new member joins",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      30'000ms)
  , group_offset_retention_sec(
      *this,
      "group_offset_retention_sec",
      "Consumer group offset retention seconds. Offset retention can be "
      "disabled by setting this value to null.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      24h * 7)
  , group_offset_retention_check_ms(
      *this,
      "group_offset_retention_check_ms",
      "How often the system should check for expired group offsets.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10min)
  , legacy_group_offset_retention_enabled(
      *this,
      "legacy_group_offset_retention_enabled",
      "Group offset retention is enabled by default in versions of Redpanda >= "
      "23.1. To enable offset retention after upgrading from an older version "
      "set this option to true.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , metadata_dissemination_interval_ms(
      *this,
      "metadata_dissemination_interval_ms",
      "Interaval for metadata dissemination batching",
      {.example = "5000", .visibility = visibility::tunable},
      3'000ms)
  , metadata_dissemination_retry_delay_ms(
      *this,
      "metadata_dissemination_retry_delay_ms",
      "Delay before retry a topic lookup in a shard or other meta tables",
      {.visibility = visibility::tunable},
      0'500ms)
  , metadata_dissemination_retries(
      *this,
      "metadata_dissemination_retries",
      "Number of attempts of looking up a topic's meta data like shard before "
      "failing a request",
      {.visibility = visibility::tunable},
      30)
  , tm_sync_timeout_ms(
      *this,
      "tm_sync_timeout_ms",
      "Time to wait state catch up before rejecting a request",
      {.visibility = visibility::user},
      10s)
  , tm_violation_recovery_policy(*this, "tm_violation_recovery_policy")
  , rm_sync_timeout_ms(
      *this,
      "rm_sync_timeout_ms",
      "Time to wait state catch up before rejecting a request",
      {.visibility = visibility::user},
      10s)
  , seq_table_min_size(
      *this,
      "seq_table_min_size",
      "Minimum size of the seq table non affected by compaction",
      {.visibility = visibility::user},
      1000)
  , tx_timeout_delay_ms(
      *this,
      "tx_timeout_delay_ms",
      "Delay before scheduling next check for timed out transactions",
      {.visibility = visibility::user},
      1000ms)
  , rm_violation_recovery_policy(*this, "rm_violation_recovery_policy")
  , fetch_reads_debounce_timeout(
      *this,
      "fetch_reads_debounce_timeout",
      "Time to wait for next read in fetch request when requested min bytes "
      "wasn't reached",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1ms)
  , alter_topic_cfg_timeout_ms(
      *this,
      "alter_topic_cfg_timeout_ms",
      "Time to wait for entries replication in controller log when executing "
      "alter configuration requst",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5s)
  , log_cleanup_policy(
      *this,
      "log_cleanup_policy",
      "Default topic cleanup policy",
      {.needs_restart = needs_restart::no,
       .example = "compact,delete",
       .visibility = visibility::user},
      model::cleanup_policy_bitflags::deletion)
  , log_message_timestamp_type(
      *this,
      "log_message_timestamp_type",
      "Default topic messages timestamp type",
      {.needs_restart = needs_restart::no,
       .example = "LogAppendTime",
       .visibility = visibility::user},
      model::timestamp_type::create_time,
      {model::timestamp_type::create_time, model::timestamp_type::append_time})
  , log_compression_type(
      *this,
      "log_compression_type",
      "Default topic compression type",
      {.needs_restart = needs_restart::no,
       .example = "snappy",
       .visibility = visibility::user},
      model::compression::producer,
      {model::compression::none,
       model::compression::gzip,
       model::compression::snappy,
       model::compression::lz4,
       model::compression::zstd,
       model::compression::producer})
  , fetch_max_bytes(
      *this,
      "fetch_max_bytes",
      "Maximum number of bytes returned in fetch request",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      55_MiB)
  , metadata_status_wait_timeout_ms(
      *this,
      "metadata_status_wait_timeout_ms",
      "Maximum time to wait in metadata request for cluster health to be "
      "refreshed",
      {.visibility = visibility::tunable},
      2s)
  , kafka_connection_rate_limit(
      *this,
      "kafka_connection_rate_limit",
      "Maximum connections per second for one core",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt,
      {.min = 1})
  , kafka_connection_rate_limit_overrides(
      *this,
      "kafka_connection_rate_limit_overrides",
      "Overrides for specific ips for maximum connections per second for one "
      "core",
      {.needs_restart = needs_restart::no,
       .example = R"(['127.0.0.1:90', '50.20.1.1:40'])",
       .visibility = visibility::user},
      {},
      validate_connection_rate)

  , transactional_id_expiration_ms(
      *this,
      "transactional_id_expiration_ms",
      "Producer ids are expired once this time has elapsed after the last "
      "write with the given producer id.",
      {.visibility = visibility::user},
      10080min)
  , max_concurrent_producer_ids(
      *this,
      "max_concurrent_producer_ids",
      "Max cache size for pids which rm_stm stores inside internal state. In "
      "overflow rm_stm will delete old pids and clear their status",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::numeric_limits<uint64_t>::max(),
      {.min = 1})
  , enable_idempotence(
      *this,
      "enable_idempotence",
      "Enable idempotent producer",
      {.visibility = visibility::user},
      true)
  , enable_transactions(
      *this,
      "enable_transactions",
      "Enable transactions",
      {.visibility = visibility::user},
      true)
  , abort_index_segment_size(
      *this,
      "abort_index_segment_size",
      "Capacity (in number of txns) of an abort index segment",
      {.visibility = visibility::tunable},
      50000)
  , delete_retention_ms(
      *this,
      "delete_retention_ms",
      "delete segments older than this - default 1 week",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      10080min)
  , log_compaction_interval_ms(
      *this,
      "log_compaction_interval_ms",
      "How often do we trigger background compaction",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      10s)
  , retention_bytes(
      *this,
      "retention_bytes",
      "Default max bytes per partition on disk before triggering a compaction",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , group_topic_partitions(
      *this,
      "group_topic_partitions",
      "Number of partitions in the internal group membership topic",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      16)
  , default_topic_replication(
      *this,
      "default_topic_replications",
      "Default replication factor for new topics",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      1,
      {.min = 1, .oddeven = odd_even_constraint::odd})
  , transaction_coordinator_replication(
      *this, "transaction_coordinator_replication")
  , id_allocator_replication(*this, "id_allocator_replication")
  , transaction_coordinator_partitions(
      *this,
      "transaction_coordinator_partitions",
      "Amount of partitions for transactions coordinator",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      16)
  , transaction_coordinator_cleanup_policy(
      *this,
      "transaction_coordinator_cleanup_policy",
      "Cleanup policy for a transaction coordinator topic",
      {.needs_restart = needs_restart::no,
       .example = "compact,delete",
       .visibility = visibility::user},
      model::cleanup_policy_bitflags::deletion)
  , transaction_coordinator_delete_retention_ms(
      *this,
      "transaction_coordinator_delete_retention_ms",
      "delete segments older than this - default 1 week",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      10080min)
  , transaction_coordinator_log_segment_size(
      *this,
      "transaction_coordinator_log_segment_size",
      "How large in bytes should each log segment be (default 1G)",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1_GiB)
  , abort_timed_out_transactions_interval_ms(
      *this,
      "abort_timed_out_transactions_interval_ms",
      "How often look for the inactive transactions and abort them",
      {.visibility = visibility::tunable},
      10s)
  , tx_log_stats_interval_s(
      *this,
      "tx_log_stats_interval_s",
      "How often to log per partition tx stats, works only with debug logging "
      "enabled.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10s)
  , create_topic_timeout_ms(
      *this,
      "create_topic_timeout_ms",
      "Timeout (ms) to wait for new topic creation",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      2'000ms)
  , wait_for_leader_timeout_ms(
      *this,
      "wait_for_leader_timeout_ms",
      "Timeout (ms) to wait for leadership in metadata cache",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5'000ms)
  , default_topic_partitions(
      *this,
      "default_topic_partitions",
      "Default number of partitions per topic",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      1)
  , disable_batch_cache(
      *this,
      "disable_batch_cache",
      "Disable batch cache in log manager",
      {.visibility = visibility::tunable},
      false)
  , raft_election_timeout_ms(
      *this,
      "election_timeout_ms",
      "Election timeout expressed in milliseconds",
      {.visibility = visibility::tunable},
      1'500ms)
  , kafka_group_recovery_timeout_ms(
      *this,
      "kafka_group_recovery_timeout_ms",
      "Kafka group recovery timeout expressed in milliseconds",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      30'000ms)
  , replicate_append_timeout_ms(
      *this,
      "replicate_append_timeout_ms",
      "Timeout for append entries requests issued while replicating entries",
      {.visibility = visibility::tunable},
      3s)
  , recovery_append_timeout_ms(
      *this,
      "recovery_append_timeout_ms",
      "Timeout for append entries requests issued while updating stale "
      "follower",
      {.visibility = visibility::tunable},
      5s)
  , raft_replicate_batch_window_size(
      *this,
      "raft_replicate_batch_window_size",
      "Max size of requests cached for replication",
      {.visibility = visibility::tunable},
      1_MiB)
  , raft_learner_recovery_rate(
      *this,
      "raft_learner_recovery_rate",
      "Raft learner recovery rate limit in bytes per sec",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      100_MiB)
  , raft_smp_max_non_local_requests(
      *this,
      "raft_smp_max_non_local_requests",
      "Maximum number of x-core requests pending in Raft seastar::smp group. "
      "(for more details look at `seastar::smp_service_group` documentation)",
      {.visibility = visibility::tunable},
      std::nullopt)
  , raft_max_concurrent_append_requests_per_follower(
      *this,
      "raft_max_concurrent_append_requests_per_follower",
      "Maximum number of concurrent append entries requests sent by leader to "
      "one follower",
      {.visibility = visibility::tunable},
      16)
  , reclaim_min_size(
      *this,
      "reclaim_min_size",
      "Minimum batch cache reclaim size",
      {.visibility = visibility::tunable},
      128_KiB)
  , reclaim_max_size(
      *this,
      "reclaim_max_size",
      "Maximum batch cache reclaim size",
      {.visibility = visibility::tunable},
      4_MiB)
  , reclaim_growth_window(
      *this,
      "reclaim_growth_window",
      "Length of time in which reclaim sizes grow",
      {.visibility = visibility::tunable},
      3'000ms)
  , reclaim_stable_window(
      *this,
      "reclaim_stable_window",
      "Length of time above which growth is reset",
      {.visibility = visibility::tunable},
      10'000ms)
  , reclaim_batch_cache_min_free(
      *this,
      "reclaim_batch_cache_min_free",
      "Free memory limit that will be kept by batch cache background reclaimer",
      {.visibility = visibility::tunable},
      64_MiB)
  , auto_create_topics_enabled(
      *this,
      "auto_create_topics_enabled",
      "Allow topic auto creation",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , enable_pid_file(
      *this,
      "enable_pid_file",
      "Enable pid file. You probably don't want to change this.",
      {.visibility = visibility::tunable},
      true)
  , kvstore_flush_interval(
      *this,
      "kvstore_flush_interval",
      "Key-value store flush interval (ms)",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::chrono::milliseconds(10))
  , kvstore_max_segment_size(
      *this,
      "kvstore_max_segment_size",
      "Key-value maximum segment size (bytes)",
      {.visibility = visibility::tunable},
      16_MiB)
  , max_kafka_throttle_delay_ms(
      *this,
      "max_kafka_throttle_delay_ms",
      "Fail-safe maximum throttle delay on kafka requests",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      30'000ms)
  , kafka_max_bytes_per_fetch(
      *this,
      "kafka_max_bytes_per_fetch",
      "Limit fetch responses to this many bytes, even if total of partition "
      "bytes limits is higher",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      64_MiB)
  , raft_io_timeout_ms(
      *this,
      "raft_io_timeout_ms",
      "Raft I/O timeout",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10'000ms)
  , join_retry_timeout_ms(
      *this,
      "join_retry_timeout_ms",
      "Time between cluster join retries in milliseconds",
      {.visibility = visibility::tunable},
      5s)
  , raft_timeout_now_timeout_ms(
      *this,
      "raft_timeout_now_timeout_ms",
      "Timeout for a timeout now request",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1s)
  , raft_transfer_leader_recovery_timeout_ms(
      *this,
      "raft_transfer_leader_recovery_timeout_ms",
      "Timeout waiting for follower recovery when transferring leadership",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10s)
  , release_cache_on_segment_roll(
      *this,
      "release_cache_on_segment_roll",
      "Free cache when segments roll",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , segment_appender_flush_timeout_ms(
      *this,
      "segment_appender_flush_timeout_ms",
      "Maximum delay until buffered data is written",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::chrono::milliseconds(1s))
  , fetch_session_eviction_timeout_ms(
      *this,
      "fetch_session_eviction_timeout_ms",
      "Minimum time before which unused session will get evicted from "
      "sessions. Maximum time after which inactive session will be deleted is "
      "two time given configuration value"
      "cache",
      {.visibility = visibility::tunable},
      60s)
  , append_chunk_size(
      *this,
      "append_chunk_size",
      "Size of direct write operations to disk in bytes",
      {.example = "32768", .visibility = visibility::tunable},
      16_KiB,
      {.min = 4096, .max = 32_MiB, .align = 4096})
  , storage_read_buffer_size(
      *this,
      "storage_read_buffer_size",
      "Size of each read buffer (one per in-flight read, per log segment)",
      {.example = "31768", .visibility = visibility::tunable},
      128_KiB)
  , storage_read_readahead_count(
      *this,
      "storage_read_readahead_count",
      "How many additional reads to issue ahead of current read location",
      {.example = "1", .visibility = visibility::tunable},
      10)
  , segment_fallocation_step(
      *this,
      "segment_fallocation_step",
      "Size for segments fallocation",
      {.needs_restart = needs_restart::no,
       .example = "32768",
       .visibility = visibility::tunable},
      32_MiB,
      storage::segment_appender::validate_fallocation_step)
  , storage_target_replay_bytes(
      *this,
      "storage_target_replay_bytes",
      "Target bytes to replay from disk on startup after clean shutdown: "
      "controls frequency of snapshots and checkpoints",
      {.needs_restart = needs_restart::no,
       .example = "2147483648",
       .visibility = visibility::tunable},
      10_GiB,
      {.min = 128_MiB, .max = 1_TiB})
  , storage_max_concurrent_replay(
      *this,
      "storage_max_concurrent_replay",
      "Maximum number of partitions' logs that will be replayed concurrently "
      "at startup, or flushed concurrently on shutdown.",
      {.needs_restart = needs_restart::no,
       .example = "2048",
       .visibility = visibility::tunable},
      1024,
      {.min = 128})
  , storage_compaction_index_memory(
      *this,
      "storage_compaction_index_memory",
      "Maximum number of bytes that may be used on each shard by compaction"
      "index writers",
      {.needs_restart = needs_restart::no,
       .example = "1073741824",
       .visibility = visibility::tunable},
      128_MiB,
      {.min = 16_MiB, .max = 100_GiB})
  , max_compacted_log_segment_size(
      *this,
      "max_compacted_log_segment_size",
      "Max compacted segment size after consolidation",
      {.needs_restart = needs_restart::no,
       .example = "10737418240",
       .visibility = visibility::tunable},
      5_GiB)
  , storage_ignore_timestamps_in_future_sec(
      *this,
      "storage_ignore_timestamps_in_future_sec",
      "If set, timestamps more than this many seconds in the future relative to"
      "the server's clock will be ignored for data retention purposes, and "
      "retention will act based on another timestamp in the same segment, or "
      "the mtime of the segment file if no valid timestamp is available",
      {.needs_restart = needs_restart::no,
       .example = "3600",
       .visibility = visibility::tunable},
      std::nullopt)
  , id_allocator_log_capacity(
      *this,
      "id_allocator_log_capacity",
      "Capacity of the id_allocator log in number of messages. "
      "Once it reached id_allocator_stm should compact the log.",
      {.visibility = visibility::tunable},
      100)
  , id_allocator_batch_size(
      *this,
      "id_allocator_batch_size",
      "Id allocator allocates messages in batches (each batch is a "
      "one log record) and then serves requests from memory without "
      "touching the log until the batch is exhausted.",
      {.visibility = visibility::tunable},
      1000)
  , enable_sasl(
      *this,
      "enable_sasl",
      "Enable SASL authentication for Kafka connections, authorization is "
      "required. see also `kafka_enable_authorization`",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , sasl_mechanisms(
      *this,
      "sasl_mechanisms",
      "A list of supported SASL mechanisms. `SCRAM` and `GSSAPI` are allowed.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      {"SCRAM"},
      validate_sasl_mechanisms)
  , sasl_kerberos_config(
      *this,
      "sasl_kerberos_config",
      "The location of the Kerberos krb5.conf file for Redpanda",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      "/etc/krb5.conf")
  , sasl_kerberos_keytab(
      *this,
      "sasl_kerberos_keytab",
      "The location of the Kerberos keytab file for Redpanda",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      "/var/lib/redpanda/redpanda.keytab")
  , sasl_kerberos_principal(
      *this,
      "sasl_kerberos_principal",
      "The primary of the Kerberos Service Principal Name (SPN) for Redpanda",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      "redpanda",
      &validate_non_empty_string_opt)
  , sasl_kerberos_principal_mapping(
      *this,
      "sasl_kerberos_principal_mapping",
      "Rules for mapping Kerberos Principal Names to Redpanda User Principals",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      {"DEFAULT"},
      security::validate_kerberos_mapping_rules)
  , kafka_enable_authorization(
      *this,
      "kafka_enable_authorization",
      "Enable authorization for Kafka connections. Values:"
      "- `nil`: Ignored. Authorization is enabled with `enable_sasl: true`"
      "; `true`: authorization is required"
      "; `false`: authorization is disabled"
      ". See also: `enable_sasl` and `kafka_api[].authentication_method`",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , kafka_mtls_principal_mapping_rules(
      *this,
      "kafka_mtls_principal_mapping_rules",
      "Principal Mapping Rules for mTLS Authentication on the Kafka API",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt,
      security::tls::validate_rules)
  , kafka_enable_partition_reassignment(
      *this,
      "kafka_enable_partition_reassignment",
      "Enable the Kafka partition reassignment API",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , controller_backend_housekeeping_interval_ms(
      *this,
      "controller_backend_housekeeping_interval_ms",
      "Interval between iterations of controller backend housekeeping loop",
      {.visibility = visibility::tunable},
      1s)
  , node_management_operation_timeout_ms(
      *this,
      "node_management_operation_timeout_ms",
      "Timeout for executing node management operations",
      {.visibility = visibility::tunable},
      5s)
  , kafka_request_max_bytes(
      *this,
      "kafka_request_max_bytes",
      "Maximum size of a single request processed via Kafka API",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      100_MiB)
  , kafka_batch_max_bytes(
      *this,
      "kafka_batch_max_bytes",
      "Maximum size of a batch processed by server. If batch is compressed the "
      "limit applies to compressed batch size",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1_MiB)
  , kafka_nodelete_topics(
      *this,
      "kafka_nodelete_topics",
      "Prevents the topics in the list from being deleted via the kafka api",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      {"__audit", "__consumer_offsets", "_schemas"},
      &validate_non_empty_string_vec)
  , kafka_noproduce_topics(
      *this,
      "kafka_noproduce_topics",
      "Prevents the topics in the list from having message produced to them "
      "via the kafka api",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      {"__audit"},
      &validate_non_empty_string_vec)
  , compaction_ctrl_update_interval_ms(
      *this,
      "compaction_ctrl_update_interval_ms",
      "",
      {.visibility = visibility::tunable},
      30s)
  , compaction_ctrl_p_coeff(
      *this,
      "compaction_ctrl_p_coeff",
      "proportional coefficient for compaction PID controller. This has to be "
      "negative since compaction backlog should decrease when number of "
      "compaction shares increases",
      {.visibility = visibility::tunable},
      -12.5)
  , compaction_ctrl_i_coeff(
      *this,
      "compaction_ctrl_i_coeff",
      "integral coefficient for compaction PID controller.",
      {.visibility = visibility::tunable},
      0.0)
  , compaction_ctrl_d_coeff(
      *this,
      "compaction_ctrl_d_coeff",
      "derivative coefficient for compaction PID controller.",
      {.visibility = visibility::tunable},
      0.2)
  , compaction_ctrl_min_shares(
      *this,
      "compaction_ctrl_min_shares",
      "minimum number of IO and CPU shares that compaction process can use",
      {.visibility = visibility::tunable},
      10)
  , compaction_ctrl_max_shares(
      *this,
      "compaction_ctrl_max_shares",
      "maximum number of IO and CPU shares that compaction process can use",
      {.visibility = visibility::tunable},
      1000)
  , compaction_ctrl_backlog_size(
      *this,
      "compaction_ctrl_backlog_size",
      "target backlog size for compaction controller. if not set compaction "
      "target compaction backlog would be equal to ",
      {.visibility = visibility::tunable},
      std::nullopt)
  , members_backend_retry_ms(
      *this,
      "members_backend_retry_ms",
      "Time between members backend reconciliation loop retries ",
      {.visibility = visibility::tunable},
      5s)
  , kafka_connections_max(
      *this,
      "kafka_connections_max",
      "Maximum number of Kafka client connections per broker",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , kafka_connections_max_per_ip(
      *this,
      "kafka_connections_max_per_ip",
      "Maximum number of Kafka client connections from each IP address, per "
      "broker",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , kafka_connections_max_overrides(
      *this,
      "kafka_connections_max_overrides",
      "Per-IP overrides of kafka connection count limit, list of "
      "<ip>:<count> strings",
      {.needs_restart = needs_restart::no,
       .example = R"(['127.0.0.1:90', '50.20.1.1:40'])",
       .visibility = visibility::user},
      {},
      validate_connection_rate)
  , kafka_client_group_byte_rate_quota(
      *this,
      "kafka_client_group_byte_rate_quota",
      "Per-group target produce quota byte rate (bytes per second). "
      "Client is considered part of the group if client_id contains "
      "clients_prefix",
      {.needs_restart = needs_restart::no,
       .example
       = R"([{'group_name': 'first_group','clients_prefix': 'group_1','quota': 10240}])",
       .visibility = visibility::user},
      {},
      validate_client_groups_byte_rate_quota)
  , kafka_client_group_fetch_byte_rate_quota(
      *this,
      "kafka_client_group_fetch_byte_rate_quota",
      "Per-group target fetch quota byte rate (bytes per second). "
      "Client is considered part of the group if client_id contains "
      "clients_prefix",
      {.needs_restart = needs_restart::no,
       .example
       = R"([{'group_name': 'first_group','clients_prefix': 'group_1','quota': 10240}])",
       .visibility = visibility::user},
      {},
      validate_client_groups_byte_rate_quota)
  , kafka_rpc_server_tcp_recv_buf(
      *this,
      "kafka_rpc_server_tcp_recv_buf",
      "Kafka server TCP receive buffer size in bytes.",
      {.example = "65536"},
      std::nullopt,
      {.min = 32_KiB, .align = 4_KiB})
  , kafka_rpc_server_tcp_send_buf(
      *this,
      "kafka_rpc_server_tcp_send_buf",
      "Kafka server TCP transmit buffer size in bytes.",
      {.example = "65536"},
      std::nullopt,
      {.min = 32_KiB, .align = 4_KiB})
  , kafka_rpc_server_stream_recv_buf(
      *this,
      "kafka_rpc_server_stream_recv_buf",
      "Userspace receive buffer max size in bytes",
      {.example = "65536", .visibility = visibility::tunable},
      std::nullopt,
      // The minimum is set to match seastar's min_buffer_size (i.e. don't
      // permit setting a max below the min).  The maximum is set to forbid
      // contiguous allocations beyond that size.
      {.min = 512, .max = 512_KiB, .align = 4_KiB})
  , cloud_storage_enabled(
      *this,
      "cloud_storage_enabled",
      "Enable archival storage",
      {.visibility = visibility::user},
      false)
  , cloud_storage_enable_remote_read(
      *this,
      "cloud_storage_enable_remote_read",
      "Default remote read config value for new topics",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , cloud_storage_enable_remote_write(
      *this,
      "cloud_storage_enable_remote_write",
      "Default remote write value for new topics",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , cloud_storage_access_key(
      *this,
      "cloud_storage_access_key",
      "AWS access key",
      {.visibility = visibility::user},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_secret_key(
      *this,
      "cloud_storage_secret_key",
      "AWS secret key",
      {.visibility = visibility::user, .secret = is_secret::yes},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_region(
      *this,
      "cloud_storage_region",
      "AWS region that houses the bucket used for storage",
      {.visibility = visibility::user},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_bucket(
      *this,
      "cloud_storage_bucket",
      "AWS bucket that should be used to store data",
      {.visibility = visibility::user},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_api_endpoint(
      *this,
      "cloud_storage_api_endpoint",
      "Optional API endpoint",
      {.visibility = visibility::user},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_credentials_source(
      *this,
      "cloud_storage_credentials_source",
      "The source of credentials to connect to cloud services",
      {.needs_restart = needs_restart::yes,
       .example = "config_file",
       .visibility = visibility::user},
      model::cloud_credentials_source::config_file,
      {model::cloud_credentials_source::config_file,
       model::cloud_credentials_source::aws_instance_metadata,
       model::cloud_credentials_source::sts,
       model::cloud_credentials_source::gcp_instance_metadata})
  , cloud_storage_roles_operation_timeout_ms(
      *this,
      "cloud_storage_roles_operation_timeout_ms",
      "Timeout for IAM role related operations (ms)",
      {.visibility = visibility::tunable},
      30s)
  , cloud_storage_reconciliation_ms(
      *this, "cloud_storage_reconciliation_interval_ms")
  , cloud_storage_upload_loop_initial_backoff_ms(
      *this,
      "cloud_storage_upload_loop_initial_backoff_ms",
      "Initial backoff interval when there is nothing to upload for a "
      "partition (ms)",
      {.visibility = visibility::tunable},
      100ms)
  , cloud_storage_upload_loop_max_backoff_ms(
      *this,
      "cloud_storage_upload_loop_max_backoff_ms",
      "Max backoff interval when there is nothing to upload for a "
      "partition (ms)",
      {.visibility = visibility::tunable},
      10s)
  , cloud_storage_max_connections(
      *this,
      "cloud_storage_max_connections",
      "Max number of simultaneous connections to S3 per shard (includes "
      "connections used for both uploads and downloads)",
      {.visibility = visibility::user},
      20)
  , cloud_storage_disable_tls(
      *this,
      "cloud_storage_disable_tls",
      "Disable TLS for all S3 connections",
      {.visibility = visibility::user},
      false)
  , cloud_storage_api_endpoint_port(
      *this,
      "cloud_storage_api_endpoint_port",
      "TLS port override",
      {.visibility = visibility::user},
      443)
  , cloud_storage_trust_file(
      *this,
      "cloud_storage_trust_file",
      "Path to certificate that should be used to validate server certificate "
      "during TLS handshake",
      {.visibility = visibility::user},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_initial_backoff_ms(
      *this,
      "cloud_storage_initial_backoff_ms",
      "Initial backoff time for exponetial backoff algorithm (ms)",
      {.visibility = visibility::tunable},
      100ms)
  , cloud_storage_segment_upload_timeout_ms(
      *this,
      "cloud_storage_segment_upload_timeout_ms",
      "Log segment upload timeout (ms)",
      {.visibility = visibility::tunable},
      30s)
  , cloud_storage_manifest_upload_timeout_ms(
      *this,
      "cloud_storage_manifest_upload_timeout_ms",
      "Manifest upload timeout (ms)",
      {.visibility = visibility::tunable},
      10s)
  , cloud_storage_max_connection_idle_time_ms(
      *this,
      "cloud_storage_max_connection_idle_time_ms",
      "Max https connection idle time (ms)",
      {.visibility = visibility::tunable},
      5s)
  , cloud_storage_segment_max_upload_interval_sec(
      *this,
      "cloud_storage_segment_max_upload_interval_sec",
      "Time that segment can be kept locally without uploading it to the "
      "remote storage (sec)",
      {.visibility = visibility::tunable},
      std::nullopt)
  , cloud_storage_manifest_max_upload_interval_sec(
      *this,
      "cloud_storage_manifest_max_upload_interval_sec",
      "Wait at least this long between partition manifest uploads. Actual time "
      "between uploads may be greater than this interval. If this is null, "
      "metadata will be updated after each segment upload.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      60s)
  , cloud_storage_readreplica_manifest_sync_timeout_ms(
      *this,
      "cloud_storage_readreplica_manifest_sync_timeout_ms",
      "Timeout to check if new data is available for partition in S3 for read "
      "replica",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      30s)
  , cloud_storage_metadata_sync_timeout_ms(
      *this,
      "cloud_storage_metadata_sync_timeout_ms",
      "Timeout for SI metadata synchronization",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10s)
  , cloud_storage_housekeeping_interval_ms(
      *this,
      "cloud_storage_housekeeping_interval_ms",
      "Interval for cloud storage housekeeping tasks",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5min)
  , cloud_storage_idle_timeout_ms(
      *this,
      "cloud_storage_idle_timeout_ms",
      "Timeout used to detect idle state of the cloud storage API. If the "
      "average cloud storage request rate is below this threshold for a "
      "configured amount of time the cloud storage is considered idle and the "
      "housekeeping jobs are started.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10s)
  , cloud_storage_idle_threshold_rps(
      *this,
      "cloud_storage_idle_threshold_rps",
      "The cloud storage request rate threshold for idle state detection. If "
      "the average request rate for the configured period is lower than this "
      "threshold the cloud storage is considered being idle.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1.0)
  , cloud_storage_enable_segment_merging(
      *this,
      "cloud_storage_enable_segment_merging",
      "Enables adjacent segment merging. The segments are reuploaded if there "
      "is an opportunity for that and if it will improve the tiered-storage "
      "performance",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      true)
  , cloud_storage_max_segments_pending_deletion_per_partition(
      *this,
      "cloud_storage_max_segments_pending_deletion_per_partition",
      "The per-partition limit for the number of segments pending deletion "
      "from the cloud. Segments can be deleted due to retention or compaction. "
      "If this limit is breached and deletion fails, then segments will be "
      "orphaned in the cloud and will have to be removed manually",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5000)
  , cloud_storage_enable_compacted_topic_reupload(
      *this,
      "cloud_storage_enable_compacted_topic_reupload",
      "Enable re-uploading data for compacted topics",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      true)
  , cloud_storage_recovery_temporary_retention_bytes_default(
      *this,
      "cloud_storage_recovery_temporary_retention_bytes_default",
      "Retention in bytes for topics created during automated recovery",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1_GiB)
  , cloud_storage_segment_size_target(
      *this,
      "cloud_storage_segment_size_target",
      "Desired segment size in the cloud storage. Default: segment.bytes",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , cloud_storage_segment_size_min(
      *this,
      "cloud_storage_segment_size_min",
      "Smallest acceptable segment size in the cloud storage. Default: "
      "cloud_storage_segment_size_target/2",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , cloud_storage_graceful_transfer_timeout_ms(
      *this,
      "cloud_storage_graceful_transfer_timeout",
      "Time limit on waiting for uploads to complete before a leadership "
      "transfer.  If this is null, leadership transfers will proceed without "
      "waiting.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5s)
  , cloud_storage_backend(
      *this,
      "cloud_storage_backend",
      "Optional cloud storage backend variant used to select API capabilities. "
      "If not supplied, will be inferred from other configuration parameters.",
      {.needs_restart = needs_restart::yes,
       .example = "aws",
       .visibility = visibility::user},
      model::cloud_storage_backend::unknown,
      {model::cloud_storage_backend::aws,
       model::cloud_storage_backend::google_s3_compat,
       model::cloud_storage_backend::azure,
       model::cloud_storage_backend::minio,
       model::cloud_storage_backend::unknown})
  , cloud_storage_credentials_host(
      *this,
      "cloud_storage_credentials_host",
      "The hostname to connect to for retrieving role based credentials. "
      "Derived from cloud_storage_credentials_source if not set. Only required "
      "when using IAM role based access.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_azure_storage_account(
      *this,
      "cloud_storage_azure_storage_account",
      "The name of the Azure storage account to use with Tiered Storage",
      {.needs_restart = needs_restart::yes, .visibility = visibility::user},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_azure_container(
      *this,
      "cloud_storage_azure_container",
      "The name of the Azure container to use with Tiered Storage. Note that "
      "the container must belong to 'cloud_storage_azure_storage_account'",
      {.needs_restart = needs_restart::yes, .visibility = visibility::user},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_azure_shared_key(
      *this,
      "cloud_storage_azure_shared_key",
      "The shared key to be used for Azure Shared Key authentication with the "
      "configured Azure storage account (see "
      "'cloud_storage_azure_storage_account)'. Note that Redpanda expects this "
      "string to be Base64 encoded.",
      {.needs_restart = needs_restart::no,
       .visibility = visibility::user,
       .secret = is_secret::yes},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_upload_ctrl_update_interval_ms(
      *this,
      "cloud_storage_upload_ctrl_update_interval_ms",
      "",
      {.visibility = visibility::tunable},
      60s)
  , cloud_storage_upload_ctrl_p_coeff(
      *this,
      "cloud_storage_upload_ctrl_p_coeff",
      "proportional coefficient for upload PID controller",
      {.visibility = visibility::tunable},
      -2.0)
  , cloud_storage_upload_ctrl_d_coeff(
      *this,
      "cloud_storage_upload_ctrl_d_coeff",
      "derivative coefficient for upload PID controller.",
      {.visibility = visibility::tunable},
      0.0)
  , cloud_storage_upload_ctrl_min_shares(
      *this,
      "cloud_storage_upload_ctrl_min_shares",
      "minimum number of IO and CPU shares that archival upload can use",
      {.visibility = visibility::tunable},
      100)
  , cloud_storage_upload_ctrl_max_shares(
      *this,
      "cloud_storage_upload_ctrl_max_shares",
      "maximum number of IO and CPU shares that archival upload can use",
      {.visibility = visibility::tunable},
      1000)
  , retention_local_target_bytes_default(
      *this,
      "retention_local_target_bytes_default",
      "Local retention size target for partitions of topics with cloud storage "
      "write enabled",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , retention_local_target_ms_default(
      *this,
      "retention_local_target_ms_default",
      "Local retention time target for partitions of topics with cloud storage "
      "write enabled",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      24h)
  , cloud_storage_cache_size(
      *this,
      "cloud_storage_cache_size",
      "Max size of archival cache",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      20_GiB)
  , cloud_storage_cache_check_interval_ms(
      *this,
      "cloud_storage_cache_check_interval",
      "Minimum time between trims of tiered storage cache.  If a fetch "
      "operation requires trimming the cache, and the most recent trim was "
      "within this period, then trimming will be delayed until this period has "
      "elapsed",
      {.visibility = visibility::tunable},
      5s)
  , cloud_storage_max_readers_per_shard(
      *this,
      "cloud_storage_max_readers_per_shard",
      "Maximum concurrent readers of remote data per CPU core.  If unset, "
      "value of `topic_partitions_per_shard` is used, i.e. one reader per "
      "partition if the shard is at its maximum partition capacity.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , cloud_storage_max_materialized_segments_per_shard(
      *this,
      "cloud_storage_max_materialized_segments_per_shard",
      "Maximum concurrent readers of remote data per CPU core.  If unset, "
      "value of `topic_partitions_per_shard` multiplied by 2 is used.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , superusers(
      *this,
      "superusers",
      "List of superuser usernames",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      {})
  , kafka_qdc_latency_alpha(
      *this,
      "kafka_qdc_latency_alpha",
      "Smoothing parameter for kafka queue depth control latency tracking.",
      {.visibility = visibility::tunable},
      0.002)
  , kafka_qdc_window_size_ms(
      *this,
      "kafka_qdc_window_size_ms",
      "Window size for kafka queue depth control latency tracking.",
      {.visibility = visibility::tunable},
      1500ms)
  , kafka_qdc_window_count(
      *this,
      "kafka_qdc_window_count",
      "Number of windows used in kafka queue depth control latency tracking.",
      {.visibility = visibility::tunable},
      12)
  , kafka_qdc_enable(
      *this,
      "kafka_qdc_enable",
      "Enable kafka queue depth control.",
      {.visibility = visibility::user},
      false)
  , kafka_qdc_depth_alpha(
      *this,
      "kafka_qdc_depth_alpha",
      "Smoothing factor for kafka queue depth control depth tracking.",
      {.visibility = visibility::tunable},
      0.8)
  , kafka_qdc_max_latency_ms(
      *this,
      "kafka_qdc_max_latency_ms",
      "Max latency threshold for kafka queue depth control depth tracking.",
      {.visibility = visibility::user},
      80ms)
  , kafka_qdc_idle_depth(
      *this,
      "kafka_qdc_idle_depth",
      "Queue depth when idleness is detected in kafka queue depth control.",
      {.visibility = visibility::tunable},
      10)
  , kafka_qdc_min_depth(
      *this,
      "kafka_qdc_min_depth",
      "Minimum queue depth used in kafka queue depth control.",
      {.visibility = visibility::tunable},
      1)
  , kafka_qdc_max_depth(
      *this,
      "kafka_qdc_max_depth",
      "Maximum queue depth used in kafka queue depth control.",
      {.visibility = visibility::tunable},
      100)
  , kafka_qdc_depth_update_ms(
      *this,
      "kafka_qdc_depth_update_ms",
      "Update frequency for kafka queue depth control.",
      {.visibility = visibility::tunable},
      7s)
  , zstd_decompress_workspace_bytes(
      *this,
      "zstd_decompress_workspace_bytes",
      "Size of the zstd decompression workspace",
      {.visibility = visibility::tunable},
      8_MiB)
  , full_raft_configuration_recovery_pattern(
      *this,
      "full_raft_configuration_recovery_pattern",
      "Recover raft configuration on start for NTPs matching pattern",
      {.visibility = visibility::tunable},
      {})
  , enable_auto_rebalance_on_node_add(
      *this,
      "enable_auto_rebalance_on_node_add",
      "Enable automatic partition rebalancing when new nodes are added",
      {.needs_restart = needs_restart::no,
       .visibility = visibility::deprecated},
      false)

  , partition_autobalancing_mode(
      *this,
      "partition_autobalancing_mode",
      "Partition autobalancing mode",
      {.needs_restart = needs_restart::no,
       .example = "node_add",
       .visibility = visibility::user},
      model::partition_autobalancing_mode::node_add,
      {
        model::partition_autobalancing_mode::off,
        model::partition_autobalancing_mode::node_add,
        model::partition_autobalancing_mode::continuous,
      })
  , partition_autobalancing_node_availability_timeout_sec(
      *this,
      "partition_autobalancing_node_availability_timeout_sec",
      "Node unavailability timeout that triggers moving partitions from the "
      "node",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      15min)
  , partition_autobalancing_max_disk_usage_percent(
      *this,
      "partition_autobalancing_max_disk_usage_percent",
      "Disk usage threshold that triggers moving partitions from the node",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      80,
      {.min = 5, .max = 100})
  , partition_autobalancing_tick_interval_ms(
      *this,
      "partition_autobalancing_tick_interval_ms",
      "Partition autobalancer tick interval",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      30s)
  , partition_autobalancing_movement_batch_size_bytes(
      *this,
      "partition_autobalancing_movement_batch_size_bytes",
      "Total size of partitions that autobalancer is going to move in one "
      "batch",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5_GiB)
  , partition_autobalancing_concurrent_moves(
      *this,
      "partition_autobalancing_concurrent_moves",
      "Number of partitions that can be reassigned at once",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      50)
  , enable_leader_balancer(
      *this,
      "enable_leader_balancer",
      "Enable automatic leadership rebalancing",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , leader_balancer_mode(
      *this,
      "leader_balancer_mode",
      "Leader balancer mode",
      {.needs_restart = needs_restart::no,
       .example = model::leader_balancer_mode_to_string(
         model::leader_balancer_mode::random_hill_climbing),
       .visibility = visibility::user},
      model::leader_balancer_mode::random_hill_climbing,
      {
        model::leader_balancer_mode::greedy_balanced_shards,
        model::leader_balancer_mode::random_hill_climbing,
      })
  , leader_balancer_idle_timeout(
      *this,
      "leader_balancer_idle_timeout",
      "Leadership rebalancing idle timeout",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      2min)
  , leader_balancer_mute_timeout(
      *this,
      "leader_balancer_mute_timeout",
      "Leadership rebalancing mute timeout",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5min)
  , leader_balancer_node_mute_timeout(
      *this,
      "leader_balancer_mute_timeout",
      "Leadership rebalancing node mute timeout",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      20s)
  , leader_balancer_transfer_limit_per_shard(
      *this,
      "leader_balancer_transfer_limit_per_shard",
      "Per shard limit for in progress leadership transfers",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      512,
      {.min = 1, .max = 2048})
  , internal_topic_replication_factor(
      *this,
      "internal_topic_replication_factor",
      "Target replication factor for internal topics",
      {.visibility = visibility::user},
      3)
  , health_manager_tick_interval(
      *this,
      "health_manager_tick_interval",
      "How often the health manager runs",
      {.visibility = visibility::tunable},
      3min)
  , health_monitor_tick_interval(
      *this,
      "health_monitor_tick_interval",
      "How often health monitor refresh cluster state",
      {.needs_restart = needs_restart::no,
       .visibility = visibility::deprecated},
      10s)
  , health_monitor_max_metadata_age(
      *this,
      "health_monitor_max_metadata_age",
      "Max age of metadata cached in the health monitor of non controller node",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10s)
  , storage_space_alert_free_threshold_percent(
      *this,
      "storage_space_alert_free_threshold_percent",
      "Threshold of minimim percent free space before setting storage space "
      "alert",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5,
      {.min = 0, .max = 50})
  , storage_space_alert_free_threshold_bytes(
      *this,
      "storage_space_alert_free_threshold_bytes",
      "Threshold of minimim bytes free space before setting storage space "
      "alert",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      0,
      {.min = 0})
  , storage_min_free_bytes(
      *this,
      "storage_min_free_bytes",
      "Threshold of minimum bytes free space before rejecting producers.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5_GiB,
      {.min = 10_MiB})
  , storage_strict_data_init(
      *this,
      "storage_strict_data_init",
      "Requires that an empty file named `.redpanda_data_dir` be present in "
      "the data directory. Redpanda will refuse to start if it is not found.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , memory_abort_on_alloc_failure(
      *this,
      "memory_abort_on_alloc_failure",
      "If true, the redpanda process will terminate immediately when an "
      "allocation cannot be satisfied due to memory exhasution. If false, an "
      "exception is thrown instead.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      true)
  , enable_metrics_reporter(
      *this,
      "enable_metrics_reporter",
      "Enable cluster metrics reporter",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , metrics_reporter_tick_interval(
      *this,
      "metrics_reporter_tick_interval",
      "Cluster metrics reporter tick interval",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1min)
  , metrics_reporter_report_interval(
      *this,
      "metrics_reporter_report_interval",
      "cluster metrics reporter report interval",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      24h)
  , metrics_reporter_url(
      *this,
      "metrics_reporter_url",
      "cluster metrics reporter url",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      "https://m.rp.vectorized.io/v2")
  , features_auto_enable(
      *this,
      "features_auto_enable",
      "Whether new feature flags may auto-activate after upgrades (true) or "
      "must wait for manual activation via the admin API (false)",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      true)
  , enable_rack_awareness(
      *this,
      "enable_rack_awareness",
      "Enables rack-aware replica assignment",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , node_status_interval(
      *this,
      "node_status_interval",
      "Time interval between two node status messages. Node status messages "
      "establish liveness status outside of the Raft protocol.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      100ms)
  , enable_controller_log_rate_limiting(
      *this,
      "enable_controller_log_rate_limiting",
      "Enables limiting of controller log write rate",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , rps_limit_topic_operations(
      *this,
      "rps_limit_topic_operations",
      "Rate limit for controller topic operations",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1000)
  , controller_log_accummulation_rps_capacity_topic_operations(
      *this,
      "controller_log_accummulation_rps_capacity_topic_operations",
      "Maximum capacity of rate limit accumulation"
      "in controller topic operations limit",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , rps_limit_acls_and_users_operations(
      *this,
      "rps_limit_acls_and_users_operations",
      "Rate limit for controller acls and users operations",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1000)
  , controller_log_accummulation_rps_capacity_acls_and_users_operations(
      *this,
      "controller_log_accummulation_rps_capacity_acls_and_users_operations",
      "Maximum capacity of rate limit accumulation"
      "in controller acls and users operations limit",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , rps_limit_node_management_operations(
      *this,
      "rps_limit_node_management_operations",
      "Rate limit for controller node management operations",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1000)
  , controller_log_accummulation_rps_capacity_node_management_operations(
      *this,
      "controller_log_accummulation_rps_capacity_node_management_operations",
      "Maximum capacity of rate limit accumulation"
      "in controller node management operations limit",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , rps_limit_move_operations(
      *this,
      "rps_limit_move_operations",
      "Rate limit for controller move operations",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1000)
  , controller_log_accummulation_rps_capacity_move_operations(
      *this,
      "controller_log_accummulation_rps_capacity_move_operations",
      "Maximum capacity of rate limit accumulation"
      "in controller move operations limit",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , rps_limit_configuration_operations(
      *this,
      "rps_limit_configuration_operations",
      "Rate limit for controller configuration operations",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1000)
  , controller_log_accummulation_rps_capacity_configuration_operations(
      *this,
      "controller_log_accummulation_rps_capacity_configuration_operations",
      "Maximum capacity of rate limit accumulation"
      "in controller configuration operations limit",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , kafka_throughput_limit_node_in_bps(
      *this,
      "kafka_throughput_limit_node_in_bps",
      "Node wide throughput ingress limit - maximum kafka traffic throughput "
      "allowed on the ingress side of each node, in bytes/s. Default is no "
      "limit.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt,
      {.min = 1})
  , kafka_throughput_limit_node_out_bps(
      *this,
      "kafka_throughput_limit_node_out_bps",
      "Node wide throughput egress limit - maximum kafka traffic throughput "
      "allowed on the egress side of each node, in bytes/s. Default is no "
      "limit.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt,
      {.min = 1})
  , kafka_quota_balancer_window(
      *this,
      "kafka_quota_balancer_window_ms",
      "Time window used to average current throughput measurement for quota "
      "balancer, in milliseconds",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      5000ms,
      {.min = 1ms, .max = bottomless_token_bucket::max_width})
  , kafka_quota_balancer_node_period(
      *this,
      "kafka_quota_balancer_node_period_ms",
      "Intra-node throughput quota balancer invocation period, in "
      "milliseconds. Value of 0 disables the balancer and makes all the "
      "throughput quotas immutable.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      750ms,
      {.min = 0ms})
  , kafka_quota_balancer_min_shard_throughput_ratio(
      *this,
      "kafka_quota_balancer_min_shard_throughput_ratio",
      "The lowest value of the throughput quota a shard can get in the process "
      "of quota balancing, expressed as a ratio of default shard quota. "
      "0 means there is no minimum, 1 means no quota can be taken away "
      "by the balancer.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      0.01,
      &validate_0_to_1_ratio)
  , kafka_quota_balancer_min_shard_throughput_bps(
      *this,
      "kafka_quota_balancer_min_shard_throughput_bps",
      "The lowest value of the throughput quota a shard can get in the process "
      "of quota balancing, in bytes/s. 0 means there is no minimum.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      256,
      {.min = 0})
  , node_isolation_heartbeat_timeout(
      *this,
      "node_isolation_heartbeat_timeout",
      "How long after the last heartbeat request a node will wait before "
      "considering itself to be isolated",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      3000,
      {.min = 100, .max = 10000})
  , controller_snapshot_max_age_sec(
      *this,
      "controller_snapshot_max_age_sec",
      "Max time that will pass before we make an attempt to create a "
      "controller snapshot, after a new controller command appears",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      60s)
  , legacy_permit_unsafe_log_operation(
      *this,
      "legacy_permit_unsafe_log_operation",
      "Permits the use of strings that may induct log injection/modification",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , legacy_unsafe_log_warning_interval_sec(
      *this,
      "legacy_unsafe_log_warning_interval_sec",
      "Interval, in seconds, of how often a message informing the operator "
      "that unsafe strings are permitted",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      300s) {}

configuration::error_map_t configuration::load(const YAML::Node& root_node) {
    if (!root_node["redpanda"]) {
        throw std::invalid_argument("'redpanda' root is required");
    }

    const auto& ignore = node().property_names();

    return config_store::read_yaml(root_node["redpanda"], ignore);
}

configuration& shard_local_cfg() {
    static thread_local configuration cfg;
    return cfg;
}
} // namespace config
