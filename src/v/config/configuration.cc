// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"

#include "cluster/node/constants.h"
#include "config/base_property.h"
#include "config/node_config.h"
#include "config/validators.h"
#include "model/metadata.h"
#include "storage/chunk_cache.h"
#include "storage/segment_appender.h"
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
  : log_segment_size(
    *this,
    "log_segment_size",
    "How large in bytes should each log segment be (default 1G)",
    {.needs_restart = needs_restart::no,
     .example = "2147483648",
     .visibility = visibility::tunable},
    1_GiB,
    {.min = 1_MiB})
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
      "TCP receive buffer size in bytes.",
      {.example = "65536"},
      std::nullopt,
      {.min = 32_KiB, .align = 4_KiB})
  , rpc_server_tcp_send_buf(
      *this,
      "rpc_server_tcp_send_buf",
      "TCP transmit buffer size in bytes.",
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
      10,
      {
        .min = 1,   // At least one FD per partition, required for appender.
        .max = 1000 // A system with 1M ulimit should be allowed to create at
                    // least 1000 partitions
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
      {.visibility = visibility::tunable},
      std::chrono::milliseconds(150),
      {.min = std::chrono::milliseconds(1)})
  , raft_heartbeat_timeout_ms(
      *this,
      "raft_heartbeat_timeout_ms",
      "raft heartbeat RPC timeout",
      {.visibility = visibility::tunable},
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
      "Target quota byte rate (bytes per second) - 2GB default",
      {.needs_restart = needs_restart::no,
       .example = "1073741824",
       .visibility = visibility::user},
      2_GiB,
      {.min = 1_MiB})
  , cluster_id(
      *this,
      "cluster_id",
      "Cluster identifier",
      {.needs_restart = needs_restart::no},
      std::nullopt)
  , disable_metrics(
      *this,
      "disable_metrics",
      "Disable registering metrics",
      base_property::metadata{},
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
      300ms)
  , group_new_member_join_timeout(
      *this,
      "group_new_member_join_timeout",
      "Timeout for new member joins",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      30'000ms)
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
  , tm_violation_recovery_policy(
      *this,
      "tm_violation_recovery_policy",
      "Describes how to recover from an invariant violation happened on the "
      "transaction coordinator level",
      {.example = "best_effort", .visibility = visibility::user},
      model::violation_recovery_policy::crash,
      {model::violation_recovery_policy::crash,
       model::violation_recovery_policy::best_effort})
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
  , rm_violation_recovery_policy(
      *this,
      "rm_violation_recovery_policy",
      "Describes how to recover from an invariant violation happened on the "
      "partition level",
      {.example = "best_effort", .visibility = visibility::user},
      model::violation_recovery_policy::crash,
      {model::violation_recovery_policy::crash,
       model::violation_recovery_policy::best_effort})
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
      false)
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
      1)
  , default_topic_replication(
      *this,
      "default_topic_replications",
      "Default replication factor for new topics",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      1)
  , transaction_coordinator_replication(
      *this,
      "transaction_coordinator_replication",
      "Replication factor for a transaction coordinator topic",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      1)
  , id_allocator_replication(
      *this,
      "id_allocator_replication",
      "Replication factor for an id allocator topic",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      1)
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
      1min)
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
      {.visibility = visibility::user},
      100_MiB)
  , raft_smp_max_non_local_requests(
      *this,
      "raft_smp_max_non_local_requests",
      "Maximum number of x-core requests pending in Raft seastar::smp group. "
      "(for more details look at `seastar::smp_service_group` documentation)",
      {.visibility = visibility::tunable},
      default_raft_non_local_requests())
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
      60'000ms)
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
      {.visibility = visibility::tunable},
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
  , max_compacted_log_segment_size(
      *this,
      "max_compacted_log_segment_size",
      "Max compacted segment size after consolidation",
      {.needs_restart = needs_restart::no,
       .example = "10737418240",
       .visibility = visibility::tunable},
      5_GiB)
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
      "Enable SASL authentication for Kafka connections.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
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
  , cloud_storage_enabled(
      *this,
      "cloud_storage_enabled",
      "Enable archival storage",
      {.visibility = visibility::user},
      false)
  , cloud_storage_enable_remote_read(
      *this,
      "cloud_storage_enable_remote_read",
      "Enable remote read for all topics",
      {.visibility = visibility::tunable},
      false)
  , cloud_storage_enable_remote_write(
      *this,
      "cloud_storage_enable_remote_write",
      "Enable remote write for all topics",
      {.visibility = visibility::tunable},
      false)
  , cloud_storage_access_key(
      *this,
      "cloud_storage_access_key",
      "AWS access key",
      {.visibility = visibility::user},
      std::nullopt)
  , cloud_storage_secret_key(
      *this,
      "cloud_storage_secret_key",
      "AWS secret key",
      {.visibility = visibility::user, .secret = is_secret::yes},
      std::nullopt)
  , cloud_storage_region(
      *this,
      "cloud_storage_region",
      "AWS region that houses the bucket used for storage",
      {.visibility = visibility::user},
      std::nullopt)
  , cloud_storage_bucket(
      *this,
      "cloud_storage_bucket",
      "AWS bucket that should be used to store data",
      {.visibility = visibility::user},
      std::nullopt)
  , cloud_storage_api_endpoint(
      *this,
      "cloud_storage_api_endpoint",
      "Optional API endpoint",
      {.visibility = visibility::user},
      std::nullopt)
  , cloud_storage_reconciliation_ms(
      *this,
      "cloud_storage_reconciliation_interval_ms",
      "Interval at which the archival service runs reconciliation (ms)",
      {.visibility = visibility::tunable},
      1s)
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
      "Max number of simultaneous uploads to S3",
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
      std::nullopt)
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
  , cloud_storage_cache_size(
      *this,
      "cloud_storage_cache_size",
      "Max size of archival cache",
      {.visibility = visibility::user},
      20_GiB)
  , cloud_storage_cache_check_interval_ms(
      *this,
      "cloud_storage_cache_check_interval",
      "Timeout to check if cache eviction should be triggered",
      {.visibility = visibility::tunable},
      30s)
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
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , enable_leader_balancer(
      *this,
      "enable_leader_balancer",
      "Enable automatic leadership rebalancing",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
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
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
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
      {.min = cluster::node::min_percent_free_threshold,
       .max = cluster::node::max_percent_free_threshold})
  , storage_space_alert_free_threshold_bytes(
      *this,
      "storage_space_alert_free_threshold_bytes",
      "Threshold of minimim bytes free space before setting storage space "
      "alert",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1_GiB,
      {.min = cluster::node::min_bytes_free_threshold})
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
      false) {}

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
