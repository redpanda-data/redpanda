// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"

#include "base/units.h"
#include "config/base_property.h"
#include "config/bounded_property.h"
#include "config/node_config.h"
#include "config/validators.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "security/config.h"
#include "security/oidc_url_parser.h"
#include "serde/rw/chrono.h"
#include "ssx/sformat.h"
#include "storage/config.h"

#include <chrono>
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
      "Lower bound on topic `segment.bytes`: lower values will be clamped to "
      "this limit.",
      {.needs_restart = needs_restart::no,
       .example = "16777216",
       .visibility = visibility::tunable},
      1_MiB)
  , log_segment_size_max(
      *this,
      "log_segment_size_max",
      "Upper bound on topic `segment.bytes`: higher values will be clamped to "
      "this limit.",
      {.needs_restart = needs_restart::no,
       .example = "268435456",
       .visibility = visibility::tunable},
      std::nullopt)
  , log_segment_size_jitter_percent(
      *this,
      "log_segment_size_jitter_percent",
      "Random variation to the segment size limit used for each partition.",
      {.needs_restart = needs_restart::yes,
       .example = "2",
       .visibility = visibility::tunable},
      5,
      {.min = 0, .max = 99})
  , compacted_log_segment_size(
      *this,
      "compacted_log_segment_size",
      "Size (in bytes) for each compacted log segment.",
      {.needs_restart = needs_restart::no,
       .example = "268435456",
       .visibility = visibility::tunable},
      256_MiB,
      {.min = 1_MiB})
  , readers_cache_eviction_timeout_ms(
      *this,
      "readers_cache_eviction_timeout_ms",
      "Duration after which inactive readers are evicted from cache.",
      {.visibility = visibility::tunable},
      30s)
  , readers_cache_target_max_size(
      *this,
      "readers_cache_target_max_size",
      "Maximum desired number of readers cached per NTP. This a soft limit, "
      "meaning that a number of readers in cache may temporarily increase as "
      "cleanup is performed in the background.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      200,
      {.min = 0, .max = 10000})
  , log_segment_ms(
      *this,
      "log_segment_ms",
      "Default lifetime of log segments. If `null`, the property is disabled, "
      "and no default lifetime is set. Any value under 60 seconds (60000 ms) "
      "is rejected. This property can also be set in the Kafka API using the "
      "Kafka-compatible alias, `log.roll.ms`. The topic property `segment.ms` "
      "overrides the value of `log_segment_ms` at the topic level.",
      {.needs_restart = needs_restart::no,
       .example = "3600000",
       .visibility = visibility::user},
      std::chrono::weeks{2},
      {.min = 60s})
  , log_segment_ms_min(
      *this,
      "log_segment_ms_min",
      "Lower bound on topic `segment.ms`: lower values will be clamped to this "
      "value.",
      {.needs_restart = needs_restart::no,
       .example = "600000",
       .visibility = visibility::tunable},
      10min,
      {.min = 0ms})
  , log_segment_ms_max(
      *this,
      "log_segment_ms_max",
      "Upper bound on topic `segment.ms`: higher values will be clamped to "
      "this value.",
      {.needs_restart = needs_restart::no,
       .example = "31536000000",
       .visibility = visibility::tunable},
      24h * 365,
      {.min = 0ms})
  , rpc_server_listen_backlog(
      *this,
      "rpc_server_listen_backlog",
      "Maximum TCP connection queue length for Kafka server and internal RPC "
      "server. If `null` (the default value), no queue length is set.",
      {.visibility = visibility::user},
      std::nullopt,
      {.min = 1})
  , rpc_server_tcp_recv_buf(
      *this,
      "rpc_server_tcp_recv_buf",
      "Internal RPC TCP receive buffer size. If `null` (the default value), no "
      "buffer size is set by Redpanda.",
      {.example = "65536"},
      std::nullopt,
      {.min = 32_KiB, .align = 4_KiB})
  , rpc_server_tcp_send_buf(
      *this,
      "rpc_server_tcp_send_buf",
      "Internal RPC TCP send buffer size. If `null` (the default value), then "
      "no buffer size is set by Redpanda.",
      {.example = "65536"},
      std::nullopt,
      {.min = 32_KiB, .align = 4_KiB})
  , rpc_client_connections_per_peer(
      *this,
      "rpc_client_connections_per_peer",
      "The maximum number of connections a broker will open to each of its "
      "peers.",
      {.example = "8"},
      32,
      {.min = 8})
  , rpc_server_compress_replies(
      *this,
      "rpc_server_compress_replies",
      "Enable compression for internal RPC (remote procedure call) server "
      "replies.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , enable_coproc(*this, "enable_coproc")
  , coproc_max_inflight_bytes(*this, "coproc_max_inflight_bytes")
  , coproc_max_ingest_bytes(*this, "coproc_max_ingest_bytes")
  , coproc_max_batch_size(*this, "coproc_max_batch_size")
  , coproc_offset_flush_interval_ms(*this, "coproc_offset_flush_interval_ms")
  , data_transforms_enabled(
      *this,
      "data_transforms_enabled",
      "Enables WebAssembly-powered data transforms directly in the broker. "
      "When `data_transforms_enabled` is set to `true`, Redpanda reserves "
      "memory for data transforms, even if no transform functions are "
      "currently deployed. This memory reservation ensures that adequate "
      "resources are available for transform functions when they are needed, "
      "but it also means that some memory is allocated regardless of usage.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::user},
      false)
  , data_transforms_commit_interval_ms(
      *this,
      "data_transforms_commit_interval_ms",
      "The commit interval at which data transforms progress.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      3s)
  , data_transforms_per_core_memory_reservation(
      *this,
      "data_transforms_per_core_memory_reservation",
      "The amount of memory to reserve per core for data transform (Wasm) "
      "virtual machines. Memory is reserved on boot. The maximum number of "
      "functions that can be deployed to a cluster is equal to "
      "`data_transforms_per_core_memory_reservation` / "
      "`data_transforms_per_function_memory_limit`.",
      {
        .needs_restart = needs_restart::yes,
        .example = std::to_string(25_MiB),
        .visibility = visibility::user,
        .aliases = {"wasm_per_core_memory_reservation"},
      },
      20_MiB,
      {.min = 64_KiB, .max = 100_GiB})
  , data_transforms_per_function_memory_limit(
      *this,
      "data_transforms_per_function_memory_limit",
      "The amount of memory to give an instance of a data transform (Wasm) "
      "virtual machine. The maximum number of functions that can be deployed "
      "to a cluster is equal to `data_transforms_per_core_memory_reservation` "
      "/ `data_transforms_per_function_memory_limit`.",
      {
        .needs_restart = needs_restart::yes,
        .example = std::to_string(5_MiB),
        .visibility = visibility::user,
        .aliases = {"wasm_per_function_memory_limit"},
      },
      2_MiB,
      // WebAssembly uses 64KiB pages and has a 32bit address space
      {.min = 64_KiB, .max = 4_GiB})
  , data_transforms_runtime_limit_ms(
      *this,
      "data_transforms_runtime_limit_ms",
      "The maximum amount of runtime to start up a data transform, and the "
      "time it takes for a single record to be transformed.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      3s)
  , data_transforms_binary_max_size(
      *this,
      "data_transforms_binary_max_size",
      "The maximum size for a deployable WebAssembly binary that the broker "
      "can store.",
      {
        .needs_restart = needs_restart::no,
        .visibility = visibility::tunable,
      },
      10_MiB,
      {.min = 1_MiB, .max = 128_MiB})
  , data_transforms_logging_buffer_capacity_bytes(
      *this,
      "data_transforms_logging_buffer_capacity_bytes",
      "Buffer capacity for transform logs, per shard. Buffer occupancy is "
      "calculated as the total size of buffered log messages; that is, logs "
      "emitted but not yet produced.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      500_KiB,
      {.min = 100_KiB, .max = 2_MiB})
  , data_transforms_logging_flush_interval_ms(
      *this,
      "data_transforms_logging_flush_interval_ms",
      "Flush interval for transform logs. When a timer expires, pending logs "
      "are collected and published to the `transform_logs` topic.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      500ms)
  , data_transforms_logging_line_max_bytes(
      *this,
      "data_transforms_logging_line_max_bytes",
      "Transform log lines truncate to this length. Truncation occurs after "
      "any character escaping.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1_KiB)
  , data_transforms_read_buffer_memory_percentage(
      *this,
      "data_transforms_read_buffer_memory_percentage",
      "The percentage of available memory in the transform subsystem to use "
      "for read buffers.",
      {
        .needs_restart = needs_restart::yes,
        .example = "25",
        .visibility = visibility::tunable,
      },
      45,
      {.min = 1, .max = 99})
  , data_transforms_write_buffer_memory_percentage(
      *this,
      "data_transforms_write_buffer_memory_percentage",
      "The percentage of available memory in the transform subsystem to use "
      "for write buffers.",
      {
        .needs_restart = needs_restart::yes,
        .example = "25",
        .visibility = visibility::tunable,
      },
      45,
      {.min = 1, .max = 99})
  , topic_memory_per_partition(
      *this,
      "topic_memory_per_partition",
      "Required memory per partition when creating topics.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      4_MiB,
      {
        .min = 1,      // Must be nonzero, it's a divisor
        .max = 100_MiB // Rough 'sanity' limit: a machine with 1GB RAM must be
                       // able to create at least 10 partitions})
      })
  , topic_fds_per_partition(
      *this,
      "topic_fds_per_partition",
      "Required file handles per partition when creating topics.",
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
      "core).",
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
      "is >= topic_partitions_per_shard, no data partitions will be scheduled "
      "on shard 0",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      0,
      {
        .min = 0,     // It is not mandatory to reserve any capacity
        .max = 131072 // Same max as topic_partitions_per_shard
      })
  , partition_manager_shutdown_watchdog_timeout(
      *this,
      "partition_manager_shutdown_watchdog_timeout",
      "A threshold value to detect partitions which might have been stuck "
      "while shutting down. After this threshold, a watchdog in partition "
      "manager will log information about partition shutdown not making "
      "progress.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      30s)
  , admin_api_require_auth(
      *this,
      "admin_api_require_auth",
      "Whether Admin API clients must provide HTTP basic authentication "
      "headers.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , seed_server_meta_topic_partitions(
      *this, "seed_server_meta_topic_partitions")
  , raft_heartbeat_interval_ms(
      *this,
      "raft_heartbeat_interval_ms",
      "Number of milliseconds for Raft leader heartbeats.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::chrono::milliseconds(150),
      {.min = std::chrono::milliseconds(1)})
  , raft_heartbeat_timeout_ms(
      *this,
      "raft_heartbeat_timeout_ms",
      "Raft heartbeat RPC (remote procedure call) timeout. Raft uses a "
      "heartbeat mechanism to maintain leadership authority and to trigger "
      "leader elections. The `raft_heartbeat_interval_ms` is a periodic "
      "heartbeat sent by the partition leader to all followers to declare its "
      "leadership. If a follower does not receive a heartbeat within the "
      "`raft_heartbeat_timeout_ms`, then it triggers an election to choose a "
      "new partition leader.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      3s,
      {.min = std::chrono::milliseconds(1)})
  , raft_heartbeat_disconnect_failures(
      *this,
      "raft_heartbeat_disconnect_failures",
      "The number of failed heartbeats after which an unresponsive TCP "
      "connection is forcibly closed. To disable forced disconnection, set to "
      "0.",
      {.visibility = visibility::tunable},
      3)

  , min_version(*this, "min_version")
  , max_version(*this, "max_version")
  , raft_max_recovery_memory(
      *this,
      "raft_max_recovery_memory",
      "Maximum memory that can be used for reads in Raft recovery process by "
      "default 15% of total memory.",
      {.needs_restart = needs_restart::no,
       .example = "41943040",
       .visibility = visibility::tunable},
      std::nullopt,
      {.min = 32_MiB})
  , raft_recovery_default_read_size(
      *this,
      "raft_recovery_default_read_size",
      "Specifies the default size of a read issued during Raft follower "
      "recovery.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      512_KiB,
      {.min = 128, .max = 5_MiB})
  , raft_enable_lw_heartbeat(
      *this,
      "raft_enable_lw_heartbeat",
      "Enables Raft optimization of heartbeats.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      true)
  , raft_recovery_concurrency_per_shard(
      *this,
      "raft_recovery_concurrency_per_shard",
      "Number of partitions that may simultaneously recover data to a "
      "particular shard. This number is limited to avoid overwhelming nodes "
      "when they come back online after an outage.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      64,
      {.min = 1, .max = 16384})
  , raft_replica_max_pending_flush_bytes(
      *this,
      "raft_replica_max_pending_flush_bytes",
      "Maximum number of bytes that are not flushed per partition. If the "
      "configured threshold is reached, the log is automatically flushed even "
      "if it has not been explicitly requested.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      256_KiB)
  , raft_flush_timer_interval_ms(
      *this,
      "raft_flush_timer_interval_ms",
      "Interval of checking partition against the "
      "`raft_replica_max_pending_flush_bytes`, deprecated started 24.1, use "
      "raft_replica_max_flush_delay_ms instead ",
      {.visibility = visibility::deprecated},
      100ms)
  , raft_replica_max_flush_delay_ms(
      *this,
      "raft_replica_max_flush_delay_ms",
      "Maximum delay between two subsequent flushes. After this delay, the log "
      "is automatically force flushed.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      100ms,
      [](const auto& v) -> std::optional<ss::sstring> {
          // maximum duration imposed by serde serialization.
          if (v < 1ms || v > serde::max_serializable_ms) {
              return fmt::format(
                "flush delay should be in range: [1, {}]",
                serde::max_serializable_ms);
          }
          return std::nullopt;
      })
  , raft_enable_longest_log_detection(
      *this,
      "raft_enable_longest_log_detection",
      "Enables an additional step in leader election where a candidate is "
      "allowed to wait for all the replies from the broker it requested votes "
      "from. This may introduce a small delay when recovering from failure, "
      "but it prevents truncation if any of the replicas have more data than "
      "the majority.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      true)
  , enable_usage(
      *this,
      "enable_usage",
      "Enables the usage tracking mechanism, storing windowed history of "
      "kafka/cloud_storage metrics over time.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , usage_num_windows(
      *this,
      "usage_num_windows",
      "The number of windows to persist in memory and disk.",
      {.needs_restart = needs_restart::no,
       .example = "24",
       .visibility = visibility::tunable},
      24,
      {.min = 2, .max = 86400})
  , usage_window_width_interval_sec(
      *this,
      "usage_window_width_interval_sec",
      "The width of a usage window, tracking cloud and kafka ingress/egress "
      "traffic each interval.",
      {.needs_restart = needs_restart::no,
       .example = "3600",
       .visibility = visibility::tunable},
      std::chrono::seconds(3600),
      {.min = std::chrono::seconds(1)})
  , usage_disk_persistance_interval_sec(
      *this,
      "usage_disk_persistance_interval_sec",
      "The interval in which all usage stats are written to disk.",
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
      "Default number of quota tracking windows.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10,
      {.min = 1})
  , default_window_sec(
      *this,
      "default_window_sec",
      "Default quota tracking window size in milliseconds.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::chrono::milliseconds(1000),
      {.min = std::chrono::milliseconds(1)})
  , quota_manager_gc_sec(
      *this,
      "quota_manager_gc_sec",
      "Quota manager GC frequency in milliseconds.",
      {.visibility = visibility::tunable},
      std::chrono::milliseconds(30000))
  , target_quota_byte_rate(
      *this,
      "target_quota_byte_rate",
      "Target request size quota byte rate (bytes per second)",
      {.needs_restart = needs_restart::no,
       .example = "1073741824",
       .visibility = visibility::user},
      target_produce_quota_byte_rate_default,
      {.min = 0})
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
      "Cluster identifier.",
      {.needs_restart = needs_restart::no},
      std::nullopt,
      &validate_non_empty_string_opt)
  , disable_metrics(
      *this,
      "disable_metrics",
      "Disable registering the metrics exposed on the internal `/metrics` "
      "endpoint.",
      base_property::metadata{},
      false)
  , disable_public_metrics(
      *this,
      "disable_public_metrics",
      "Disable registering the metrics exposed on the `/public_metrics` "
      "endpoint.",
      base_property::metadata{},
      false)
  , aggregate_metrics(
      *this,
      "aggregate_metrics",
      "Enable aggregation of metrics returned by the `/metrics` endpoint. "
      "Aggregation can simplify monitoring by providing summarized data "
      "instead of raw, per-instance metrics. Metric aggregation is performed "
      "by summing the values of samples by labels and is done when it makes "
      "sense by the shard and/or partition labels.",
      {.needs_restart = needs_restart::no},
      false)
  , group_min_session_timeout_ms(
      *this,
      "group_min_session_timeout_ms",
      "The minimum allowed session timeout for registered consumers. Shorter "
      "timeouts result in quicker failure detection at the cost of more "
      "frequent consumer heartbeating, which can overwhelm broker resources.",
      {.needs_restart = needs_restart::no},
      6000ms)
  , group_max_session_timeout_ms(
      *this,
      "group_max_session_timeout_ms",
      "The maximum allowed session timeout for registered consumers. Longer "
      "timeouts give consumers more time to process messages in between "
      "heartbeats at the cost of a longer time to detect failures.",
      {.needs_restart = needs_restart::no},
      300s)
  , group_initial_rebalance_delay(
      *this,
      "group_initial_rebalance_delay",
      "Delay added to the rebalance phase to wait for new members.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      3s)
  , group_new_member_join_timeout(
      *this,
      "group_new_member_join_timeout",
      "Timeout for new member joins.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      30'000ms)
  , group_offset_retention_sec(
      *this,
      "group_offset_retention_sec",
      "Consumer group offset retention seconds. To disable offset retention, "
      "set this to null.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      24h * 7)
  , group_offset_retention_check_ms(
      *this,
      "group_offset_retention_check_ms",
      "Frequency rate at which the system should check for expired group "
      "offsets.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10min)
  , legacy_group_offset_retention_enabled(
      *this,
      "legacy_group_offset_retention_enabled",
      "Group offset retention is enabled by default starting in Redpanda "
      "version 23.1. To enable offset retention after upgrading from an older "
      "version, set this option to true.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , metadata_dissemination_interval_ms(
      *this,
      "metadata_dissemination_interval_ms",
      "Interval for metadata dissemination batching.",
      {.example = "5000", .visibility = visibility::tunable},
      3'000ms)
  , metadata_dissemination_retry_delay_ms(
      *this,
      "metadata_dissemination_retry_delay_ms",
      "Delay before retrying a topic lookup in a shard or other meta tables.",
      {.visibility = visibility::tunable},
      0'500ms)
  , metadata_dissemination_retries(
      *this,
      "metadata_dissemination_retries",
      "Number of attempts to look up a topic's metadata-like shard before a "
      "request fails. This configuration controls the number of retries that "
      "request handlers perform when internal topic metadata (for topics like "
      "tx, consumer offsets, etc) is missing. These topics are usually created "
      "on demand when users try to use the cluster for the first time and it "
      "may take some time for the creation to happen and the metadata to "
      "propagate to all the brokers (particularly the broker handling the "
      "request). In the mean time Redpanda waits and retry. This configuration "
      "controls the number retries.",
      {.visibility = visibility::tunable},
      30)
  , tm_sync_timeout_ms(
      *this,
      "tm_sync_timeout_ms",
      "Transaction manager's synchronization timeout. Maximum time to wait for "
      "internal state machine to catch up before rejecting a request.",
      {.visibility = visibility::user},
      10s)
  , tx_registry_sync_timeout_ms(*this, "tx_registry_sync_timeout_ms")
  , tm_violation_recovery_policy(*this, "tm_violation_recovery_policy")
  , rm_sync_timeout_ms(
      *this,
      "rm_sync_timeout_ms",
      "Resource manager's synchronization timeout. Specifies the maximum time "
      "for this node to wait for the internal state machine to catch up with "
      "all events written by previous leaders before rejecting a request.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      10s)
  , find_coordinator_timeout_ms(*this, "find_coordinator_timeout_ms")
  , seq_table_min_size(*this, "seq_table_min_size")
  , tx_timeout_delay_ms(
      *this,
      "tx_timeout_delay_ms",
      "Delay before scheduling the next check for timed out transactions.",
      {.visibility = visibility::user},
      1000ms)
  , rm_violation_recovery_policy(*this, "rm_violation_recovery_policy")
  , fetch_reads_debounce_timeout(
      *this,
      "fetch_reads_debounce_timeout",
      "Time to wait for the next read in fetch requests when the requested "
      "minimum bytes was not reached.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1ms)
  , fetch_read_strategy(
      *this,
      "fetch_read_strategy",
      "The strategy used to fulfill fetch requests. * `polling`: Repeatedly "
      "polls every partition in the request for new data. The polling interval "
      "is set by `fetch_reads_debounce_timeout` (deprecated). * `non_polling`: "
      "The backend is signaled when a partition has new data, so Redpanda does "
      "not need to repeatedly read from every partition in the fetch. Redpanda "
      "Data recommends using this value for most workloads, because it can "
      "improve fetch latency and CPU utilization. * "
      "`non_polling_with_debounce`: This option behaves like `non_polling`, "
      "but it includes a debounce mechanism with a fixed delay specified by "
      "`fetch_reads_debounce_timeout` at the start of each fetch. By "
      "introducing this delay, Redpanda can accumulate more data before "
      "processing, leading to fewer fetch operations and returning larger "
      "amounts of data. Enabling this option reduces reactor utilization, but "
      "it may also increase end-to-end latency.",
      {.needs_restart = needs_restart::no,
       .example = model::fetch_read_strategy_to_string(
         model::fetch_read_strategy::non_polling),
       .visibility = visibility::tunable},
      model::fetch_read_strategy::non_polling,
      {
        model::fetch_read_strategy::polling,
        model::fetch_read_strategy::non_polling,
        model::fetch_read_strategy::non_polling_with_debounce,
        model::fetch_read_strategy::non_polling_with_pid,
      })
  , fetch_pid_p_coeff(
      *this,
      "fetch_pid_p_coeff",
      "Proportional coefficient for fetch PID controller.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      100.0,
      {.min = 0.0, .max = std::numeric_limits<double>::max()})
  , fetch_pid_i_coeff(
      *this,
      "fetch_pid_i_coeff",
      "Integral coefficient for fetch PID controller.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      0.01,
      {.min = 0.0, .max = std::numeric_limits<double>::max()})
  , fetch_pid_d_coeff(
      *this,
      "fetch_pid_d_coeff",
      "Derivative coefficient for fetch PID controller.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      0.0,
      {.min = 0.0, .max = std::numeric_limits<double>::max()})
  , fetch_pid_target_utilization_fraction(
      *this,
      "fetch_pid_target_utilization_fraction",
      "A fraction, between 0 and 1, for the target reactor utilization of the "
      "fetch scheduling group.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      0.2,
      {.min = 0.01, .max = 1.0})
  , fetch_pid_max_debounce_ms(
      *this,
      "fetch_pid_max_debounce_ms",
      "The maximum debounce time the fetch PID controller will apply, in "
      "milliseconds.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      100ms)
  , alter_topic_cfg_timeout_ms(
      *this,
      "alter_topic_cfg_timeout_ms",
      "The duration, in milliseconds, that Redpanda waits for the replication "
      "of entries in the controller log when executing a request to alter "
      "topic configurations. This timeout ensures that configuration changes "
      "are replicated across the cluster before the alteration request is "
      "considered complete.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5s)
  , log_cleanup_policy(
      *this,
      "log_cleanup_policy",
      "Default cleanup policy for topic logs. The topic property "
      "`cleanup.policy` overrides the value of `log_cleanup_policy` at the "
      "topic level.",
      {.needs_restart = needs_restart::no,
       .example = "compact,delete",
       .visibility = visibility::user},
      model::cleanup_policy_bitflags::deletion)
  , log_message_timestamp_type(
      *this,
      "log_message_timestamp_type",
      "Default timestamp type for topic messages (CreateTime or "
      "LogAppendTime). The topic property `message.timestamp.type` overrides "
      "the value of `log_message_timestamp_type` at the topic level.",
      {.needs_restart = needs_restart::no,
       .example = "LogAppendTime",
       .visibility = visibility::user},
      model::timestamp_type::create_time,
      {model::timestamp_type::create_time, model::timestamp_type::append_time})
  , log_message_timestamp_alert_before_ms(
      *this,
      "log_message_timestamp_alert_before_ms",
      "Threshold in milliseconds for alerting on messages with a timestamp "
      "before the broker's time, meaning the messages are in the past relative "
      "to the broker's clock. To disable this check, set to `null`.",
      {.needs_restart = needs_restart::no,
       .example = "604800000",
       .visibility = visibility::tunable},
      std::nullopt,
      {.min = 5min})
  , log_message_timestamp_alert_after_ms(
      *this,
      "log_message_timestamp_alert_after_ms",
      "Threshold in milliseconds for alerting on messages with a timestamp "
      "after the broker's time, meaning the messages are in the future "
      "relative to the broker's clock.",
      {.needs_restart = needs_restart::no,
       .example = "3600000",
       .visibility = visibility::tunable},
      2h,
      {.min = 5min})
  , log_compression_type(
      *this,
      "log_compression_type",
      "Default topic compression type. The topic property `compression.type` "
      "overrides the value of `log_compression_type` at the topic level.",
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
      "Maximum number of bytes returned in a fetch request.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      55_MiB)
  , use_fetch_scheduler_group(
      *this,
      "use_fetch_scheduler_group",
      "Use a separate scheduler group for fetch processing.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      true)
  , metadata_status_wait_timeout_ms(
      *this,
      "metadata_status_wait_timeout_ms",
      "Maximum time to wait in metadata request for cluster health to be "
      "refreshed.",
      {.visibility = visibility::tunable},
      2s)
  , kafka_tcp_keepalive_idle_timeout_seconds(
      *this,
      "kafka_tcp_keepalive_timeout",
      "TCP keepalive idle timeout in seconds for Kafka connections. This "
      "describes the timeout between TCP keepalive probes that the remote site "
      "successfully acknowledged. Refers to the TCP_KEEPIDLE socket option. "
      "When changed, applies to new connections only.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      120s)
  , kafka_tcp_keepalive_probe_interval_seconds(
      *this,
      "kafka_tcp_keepalive_probe_interval_seconds",
      "TCP keepalive probe interval in seconds for Kafka connections. This "
      "describes the timeout between unacknowledged TCP keepalives. Refers to "
      "the TCP_KEEPINTVL socket option. When changed, applies to new "
      "connections only.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      60s)
  , kafka_tcp_keepalive_probes(
      *this,
      "kafka_tcp_keepalive_probes",
      "TCP keepalive unacknowledged probes until the connection is considered "
      "dead for Kafka connections. Refers to the TCP_KEEPCNT socket option. "
      "When changed, applies to new connections only.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      3)
  , kafka_connection_rate_limit(
      *this,
      "kafka_connection_rate_limit",
      "Maximum connections per second for one core. If `null` (the default), "
      "then the number of connections per second is unlimited.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt,
      {.min = 1})
  , kafka_connection_rate_limit_overrides(
      *this,
      "kafka_connection_rate_limit_overrides",
      "Overrides the maximum connections per second for one core for the "
      "specified IP addresses (for example, `['127.0.0.1:90', "
      "'50.20.1.1:40']`)",
      {.needs_restart = needs_restart::no,
       .example = R"(['127.0.0.1:90', '50.20.1.1:40'])",
       .visibility = visibility::user},
      {},
      validate_connection_rate)

  , transactional_id_expiration_ms(
      *this,
      "transactional_id_expiration_ms",
      "Expiration time of producer IDs. Measured starting from the time of the "
      "last write until now for a given ID.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      10080min)
  , max_concurrent_producer_ids(
      *this,
      "max_concurrent_producer_ids",
      "Maximum number of the active producers sessions. When the threshold is "
      "passed, Redpanda terminates old sessions. When an idle producer "
      "corresponding to the terminated session wakes up and produces, its "
      "message batches are rejected, and an out of order sequence error is "
      "emitted. Consumers don't affect this setting.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::numeric_limits<uint64_t>::max(),
      {.min = 1})
  , max_transactions_per_coordinator(
      *this,
      "max_transactions_per_coordinator",
      "Specifies the maximum number of active transaction sessions per "
      "coordinator. When the threshold is passed Redpanda terminates old "
      "sessions. When an idle producer corresponding to the terminated session "
      "wakes up and produces, it leads to its batches being rejected with "
      "invalid producer epoch or invalid_producer_id_mapping error (depends on "
      "the transaction execution phase).",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::numeric_limits<uint64_t>::max(),
      {.min = 1})
  , enable_idempotence(
      *this,
      "enable_idempotence",
      "Enable idempotent producers.",
      {.visibility = visibility::user},
      true)
  , enable_transactions(
      *this,
      "enable_transactions",
      "Enable transactions (atomic writes).",
      {.visibility = visibility::user},
      true)
  , abort_index_segment_size(
      *this,
      "abort_index_segment_size",
      "Capacity (in number of txns) of an abort index segment. Each partition "
      "tracks the aborted transaction offset ranges to help service client "
      "requests.If the number transactions increase beyond this threshold, "
      "they are flushed to disk to easy memory pressure.Then they're loaded on "
      "demand. This configuration controls the maximum number of aborted "
      "transactions  before they are flushed to disk.",
      {.visibility = visibility::tunable},
      50000)
  , log_retention_ms(
      *this,
      "log_retention_ms",
      "The amount of time to keep a log file before deleting it (in "
      "milliseconds). If set to `-1`, no time limit is applied. This is a "
      "cluster-wide default when a topic does not set or disable "
      "`retention.ms`.",
      {.needs_restart = needs_restart::no,
       .visibility = visibility::user,
       .aliases = {"delete_retention_ms"}},
      7 * 24h)
  , log_compaction_interval_ms(
      *this,
      "log_compaction_interval_ms",
      "How often to trigger background compaction.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      10s)
  , log_disable_housekeeping_for_tests(
      *this,
      "log_disable_housekeeping_for_tests",
      "Disables the housekeeping loop for local storage. This property is used "
      "to simplify testing, and should not be set in production.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      false)
  , log_compaction_use_sliding_window(
      *this,
      "log_compaction_use_sliding_window",
      "Use sliding window compaction.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      true)
  , retention_bytes(
      *this,
      "retention_bytes",
      "Default maximum number of bytes per partition on disk before triggering "
      "deletion of the oldest messages. If `null` (the default value), no "
      "limit is applied. The topic property `retention.bytes` overrides the "
      "value of `retention_bytes` at the topic level.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , group_topic_partitions(
      *this,
      "group_topic_partitions",
      "Number of partitions in the internal group membership topic.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      16)
  , default_topic_replication(
      *this,
      "default_topic_replications",
      "Default replication factor for new topics.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      1,
      {.min = 1, .oddeven = odd_even_constraint::odd})
  , minimum_topic_replication(
      *this,
      "minimum_topic_replications",
      "Minimum allowable replication factor for topics in this cluster. The "
      "set value must be positive, odd, and equal to or less than the number "
      "of available brokers. Changing this parameter only restricts "
      "newly-created topics. Redpanda returns an `INVALID_REPLICATION_FACTOR` "
      "error on any attempt to create a topic with a replication factor less "
      "than this property. If you change the `minimum_topic_replications` "
      "setting, the replication factor of existing topics remains unchanged. "
      "However, Redpanda will log a warning on start-up with a list of any "
      "topics that have fewer replicas than this minimum. For example, you "
      "might see a message such as `Topic X has a replication factor less than "
      "specified minimum: 1 < 3`.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      1,
      {.min = 1, .oddeven = odd_even_constraint::odd})
  , transaction_coordinator_replication(
      *this, "transaction_coordinator_replication")
  , id_allocator_replication(*this, "id_allocator_replication")
  , transaction_coordinator_partitions(
      *this,
      "transaction_coordinator_partitions",
      "Number of partitions for transactions coordinator.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      50)
  , transaction_coordinator_cleanup_policy(
      *this,
      "transaction_coordinator_cleanup_policy",
      "Cleanup policy for a transaction coordinator topic. Accepted Values: "
      "`compact`, `delete`, `[\"compact\",\"delete\"]`, `none`",
      {.needs_restart = needs_restart::no,
       .example = "compact,delete",
       .visibility = visibility::user},
      model::cleanup_policy_bitflags::deletion)
  , transaction_coordinator_delete_retention_ms(
      *this,
      "transaction_coordinator_delete_retention_ms",
      "Delete segments older than this age. To ensure transaction state is "
      "retained as long as the longest-running transaction, make sure this is "
      "no less than `transactional_id_expiration_ms`.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      10080min)
  , transaction_coordinator_log_segment_size(
      *this,
      "transaction_coordinator_log_segment_size",
      "The size (in bytes) each log segment should be.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1_GiB)
  , abort_timed_out_transactions_interval_ms(
      *this,
      "abort_timed_out_transactions_interval_ms",
      "Interval, in milliseconds, at which Redpanda looks for inactive "
      "transactions and aborts them.",
      {.visibility = visibility::tunable},
      10s)
  , transaction_max_timeout_ms(
      *this,
      "transaction_max_timeout_ms",
      "The maximum allowed timeout for transactions. If a client-requested "
      "transaction timeout exceeds this configuration, the broker returns an "
      "error during transactional producer initialization. This guardrail "
      "prevents hanging transactions from blocking consumer progress.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      15min)
  , tx_log_stats_interval_s(
      *this,
      "tx_log_stats_interval_s",
      "How often to log per partition tx stats, works only with debug logging "
      "enabled.",
      {.needs_restart = needs_restart::no,
       .visibility = visibility::deprecated},
      10s)
  , create_topic_timeout_ms(
      *this,
      "create_topic_timeout_ms",
      "Timeout, in milliseconds, to wait for new topic creation.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      2'000ms)
  , wait_for_leader_timeout_ms(
      *this,
      "wait_for_leader_timeout_ms",
      "Timeout to wait for leadership in metadata cache.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5'000ms)
  , default_topic_partitions(
      *this,
      "default_topic_partitions",
      "Default number of partitions per topic.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      1)
  , disable_batch_cache(
      *this,
      "disable_batch_cache",
      "Disable batch cache in log manager.",
      {.visibility = visibility::tunable},
      false)
  , raft_election_timeout_ms(
      *this,
      "election_timeout_ms",
      "Election timeout expressed in milliseconds.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1'500ms)
  , kafka_group_recovery_timeout_ms(
      *this,
      "kafka_group_recovery_timeout_ms",
      "Kafka group recovery timeout.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      30'000ms)
  , replicate_append_timeout_ms(
      *this,
      "replicate_append_timeout_ms",
      "Timeout for append entry requests issued while replicating entries.",
      {.visibility = visibility::tunable},
      3s)
  , recovery_append_timeout_ms(
      *this,
      "recovery_append_timeout_ms",
      "Timeout for append entry requests issued while updating a stale "
      "follower.",
      {.visibility = visibility::tunable},
      5s)
  , raft_replicate_batch_window_size(
      *this,
      "raft_replicate_batch_window_size",
      "Maximum size of requests cached for replication.",
      {.visibility = visibility::tunable},
      1_MiB)
  , raft_learner_recovery_rate(
      *this,
      "raft_learner_recovery_rate",
      "Raft learner recovery rate limit. Throttles the rate of data "
      "communicated to nodes (learners) that need to catch up to leaders. This "
      "rate limit is placed on a node sending data to a recovering node. Each "
      "sending node is limited to this rate. The recovering node accepts data "
      "as fast as possible according to the combined limits of all healthy "
      "nodes in the cluster. For example, if two nodes are sending data to the "
      "recovering node, and `raft_learner_recovery_rate` is 100 MB/sec, then "
      "the recovering node will recover at a rate of 200 MB/sec.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      100_MiB)
  , raft_recovery_throttle_disable_dynamic_mode(
      *this,
      "raft_recovery_throttle_disable_dynamic_mode",
      "Disables cross shard sharing used to throttle recovery traffic. Should "
      "only be used to debug unexpected problems.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , raft_smp_max_non_local_requests(
      *this,
      "raft_smp_max_non_local_requests",
      "Maximum number of Cross-core(Inter-shard communication) requests "
      "pending in Raft seastar::smp group. For details, refer to the "
      "`seastar::smp_service_group` documentation).",
      {.visibility = visibility::tunable},
      std::nullopt)
  , raft_max_concurrent_append_requests_per_follower(
      *this,
      "raft_max_concurrent_append_requests_per_follower",
      "Maximum number of concurrent append entry requests sent by the leader "
      "to one follower.",
      {.visibility = visibility::tunable},
      16)
  , write_caching_default(
      *this,
      "write_caching_default",
      "The default write caching mode to apply to user topics. Write caching "
      "acknowledges a message as soon as it is received and acknowledged on a "
      "majority of brokers, without waiting for it to be written to disk. With "
      "`acks=all`, this provides lower latency while still ensuring that a "
      "majority of brokers acknowledge the write. Fsyncs follow "
      "`raft_replica_max_pending_flush_bytes` and "
      "`raft_replica_max_flush_delay_ms`, whichever is reached first. The "
      "`write_caching_default` cluster property can be overridden with the "
      "`write.caching` topic property. Accepted values: * `true` * `false` * "
      "`disabled`: This takes precedence over topic overrides and disables "
      "write caching for the entire cluster.",
      {.needs_restart = needs_restart::no,
       .example = "true",
       .visibility = visibility::user},
      model::write_caching_mode::default_false,
      {model::write_caching_mode::default_true,
       model::write_caching_mode::default_false,
       model::write_caching_mode::disabled})
  , reclaim_min_size(
      *this,
      "reclaim_min_size",
      "Minimum batch cache reclaim size.",
      {.visibility = visibility::tunable},
      128_KiB)
  , reclaim_max_size(
      *this,
      "reclaim_max_size",
      "Maximum batch cache reclaim size.",
      {.visibility = visibility::tunable},
      4_MiB)
  , reclaim_growth_window(
      *this,
      "reclaim_growth_window",
      "Starting from the last point in time when memory was reclaimed from the "
      "batch cache, this is the duration during which the amount of memory to "
      "reclaim grows at a significant rate, based on heuristics about the "
      "amount of available memory.",
      {.visibility = visibility::tunable},
      3'000ms)
  , reclaim_stable_window(
      *this,
      "reclaim_stable_window",
      "If the duration since the last time memory was reclaimed is longer than "
      "the amount of time specified in this property, the memory usage of the "
      "batch cache is considered stable, so only the minimum size "
      "(`reclaim_min_size`) is set to be reclaimed.",
      {.visibility = visibility::tunable},
      10'000ms)
  , reclaim_batch_cache_min_free(
      *this,
      "reclaim_batch_cache_min_free",
      "Minimum amount of free memory maintained by the batch cache background "
      "reclaimer.",
      {.visibility = visibility::tunable},
      64_MiB)
  , auto_create_topics_enabled(
      *this,
      "auto_create_topics_enabled",
      "Allow automatic topic creation. To prevent excess topics, this property "
      "is not supported on Redpanda Cloud BYOC and Dedicated clusters. You "
      "should explicitly manage topic creation for these Redpanda Cloud "
      "clusters. If you produce to a topic that doesn't exist, the topic will "
      "be created with defaults if this property is enabled.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , enable_pid_file(
      *this,
      "enable_pid_file",
      "Enable PID file. You should not need to change.",
      {.visibility = visibility::tunable},
      true)
  , kvstore_flush_interval(
      *this,
      "kvstore_flush_interval",
      "Key-value store flush interval (in milliseconds).",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::chrono::milliseconds(10))
  , kvstore_max_segment_size(
      *this,
      "kvstore_max_segment_size",
      "Key-value maximum segment size (in bytes).",
      {.visibility = visibility::tunable},
      16_MiB)
  , max_kafka_throttle_delay_ms(
      *this,
      "max_kafka_throttle_delay_ms",
      "Fail-safe maximum throttle delay on Kafka requests.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      30'000ms)
  , kafka_max_bytes_per_fetch(
      *this,
      "kafka_max_bytes_per_fetch",
      "Limit fetch responses to this many bytes, even if the total of "
      "partition bytes limits is higher.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      64_MiB)
  , raft_io_timeout_ms(
      *this,
      "raft_io_timeout_ms",
      "Raft I/O timeout.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10'000ms)
  , join_retry_timeout_ms(
      *this,
      "join_retry_timeout_ms",
      "Time between cluster join retries in milliseconds.",
      {.visibility = visibility::tunable},
      5s)
  , raft_timeout_now_timeout_ms(
      *this,
      "raft_timeout_now_timeout_ms",
      "Timeout for Raft's timeout_now RPC. This RPC is used to force a "
      "follower to dispatch a round of votes immediately.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1s)
  , raft_transfer_leader_recovery_timeout_ms(
      *this,
      "raft_transfer_leader_recovery_timeout_ms",
      "Follower recovery timeout waiting period when transferring leadership.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10s)
  , release_cache_on_segment_roll(
      *this,
      "release_cache_on_segment_roll",
      "Flag for specifying whether or not to release cache when a full segment "
      "is rolled.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , segment_appender_flush_timeout_ms(
      *this,
      "segment_appender_flush_timeout_ms",
      "Maximum delay until buffered data is written.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::chrono::milliseconds(1s))
  , fetch_session_eviction_timeout_ms(
      *this,
      "fetch_session_eviction_timeout_ms",
      "Time duration after which the inactive fetch session is removed from "
      "the fetch session cache. Fetch sessions are used to implement the "
      "incremental fetch requests where a consumer does not send all requested "
      "partitions to the server but the server tracks them for the consumer.",
      {.visibility = visibility::tunable},
      60s)
  , append_chunk_size(
      *this,
      "append_chunk_size",
      "Size of direct write operations to disk in bytes. A larger chunk size "
      "can improve performance for write-heavy workloads, but increase latency "
      "for these writes as more data is collected before each write operation. "
      "A smaller chunk size can decrease write latency, but potentially "
      "increase the number of disk I/O operations.",
      {.example = "32768", .visibility = visibility::tunable},
      16_KiB,
      {.min = 4096, .max = 32_MiB, .align = 4096})
  , storage_read_buffer_size(
      *this,
      "storage_read_buffer_size",
      "Size of each read buffer (one per in-flight read, per log segment).",
      {.needs_restart = needs_restart::no,
       .example = "31768",
       .visibility = visibility::tunable},
      128_KiB)
  , storage_read_readahead_count(
      *this,
      "storage_read_readahead_count",
      "How many additional reads to issue ahead of current read location.",
      {.needs_restart = needs_restart::no,
       .example = "1",
       .visibility = visibility::tunable},
      10)
  , segment_fallocation_step(
      *this,
      "segment_fallocation_step",
      "Size for segments fallocation.",
      {.needs_restart = needs_restart::no,
       .example = "32768",
       .visibility = visibility::tunable},
      32_MiB,
      storage::validate_fallocation_step)
  , storage_target_replay_bytes(
      *this,
      "storage_target_replay_bytes",
      "Target bytes to replay from disk on startup after clean shutdown: "
      "controls frequency of snapshots and checkpoints.",
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
      "Maximum number of bytes that may be used on each shard by compaction "
      "index writers.",
      {.needs_restart = needs_restart::no,
       .example = "1073741824",
       .visibility = visibility::tunable},
      128_MiB,
      {.min = 16_MiB, .max = 100_GiB})
  , storage_compaction_key_map_memory(
      *this,
      "storage_compaction_key_map_memory",
      "Maximum number of bytes that may be used on each shard by compaction "
      "key-offset maps. Only applies when `log_compaction_use_sliding_window` "
      "is set to `true`.",
      {.needs_restart = needs_restart::yes,
       .example = "1073741824",
       .visibility = visibility::tunable},
      128_MiB,
      {.min = 16_MiB, .max = 100_GiB})
  , storage_compaction_key_map_memory_limit_percent(
      *this,
      "storage_compaction_key_map_memory_limit_percent",
      "Limit on `storage_compaction_key_map_memory`, expressed as a percentage "
      "of memory per shard, that bounds the amount of memory used by "
      "compaction key-offset maps. Memory per shard is computed after "
      "`data_transforms_per_core_memory_reservation`, and only applies when "
      "`log_compaction_use_sliding_window` is set to `true`.",
      {.needs_restart = needs_restart::yes,
       .example = "12.0",
       .visibility = visibility::tunable},
      12.0,
      {.min = 1.0, .max = 100.0})
  , max_compacted_log_segment_size(
      *this,
      "max_compacted_log_segment_size",
      "Maximum compacted segment size after consolidation.",
      {.needs_restart = needs_restart::no,
       .example = "10737418240",
       .visibility = visibility::tunable},
      5_GiB)
  , storage_ignore_timestamps_in_future_sec(
      *this,
      "storage_ignore_timestamps_in_future_sec",
      "The maximum number of seconds that a record's timestamp can be ahead of "
      "a Redpanda broker's clock and still be used when deciding whether to "
      "clean up the record for data retention. This property makes possible "
      "the timely cleanup of records from clients with clocks that are "
      "drastically unsynchronized relative to Redpanda. When determining "
      "whether to clean up a record with timestamp more than "
      "`storage_ignore_timestamps_in_future_sec` seconds ahead of the broker, "
      "Redpanda ignores the record's timestamp and instead uses a valid "
      "timestamp of another record in the same segment, or (if another "
      "record's valid timestamp is unavailable) the timestamp of when the "
      "segment file was last modified (mtime). By default, "
      "`storage_ignore_timestamps_in_future_sec` is disabled (null). To figure "
      "out whether to set `storage_ignore_timestamps_in_future_sec` for your "
      "system: . Look for logs with segments that are unexpectedly large and "
      "not being cleaned up. . In the logs, search for records with "
      "unsynchronized timestamps that are further into the future than "
      "tolerable by your data retention and storage settings. For example, "
      "timestamps 60 seconds or more into the future can be considered to be "
      "too unsynchronized. . If you find unsynchronized timestamps throughout "
      "your logs, determine the number of seconds that the timestamps are "
      "ahead of their actual time, and set "
      "`storage_ignore_timestamps_in_future_sec` to that value so data "
      "retention can proceed. . If you only find unsynchronized timestamps "
      "that are the result of transient behavior, you can disable "
      "`storage_ignore_timestamps_in_future_sec`.",
      {.needs_restart = needs_restart::no,
       .example = "3600",
       .visibility = visibility::tunable},
      std::nullopt)
  , storage_ignore_cstore_hints(
      *this,
      "storage_ignore_cstore_hints",
      "When set, cstore hints are ignored and not used for data access (but "
      "are otherwise generated).",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , storage_reserve_min_segments(
      *this,
      "storage_reserve_min_segments",
      "The number of segments per partition that the system will attempt to "
      "reserve disk capacity for. For example, if the maximum segment size is "
      "configured to be 100 MB, and the value of this option is 2, then in a "
      "system with 10 partitions Redpanda will attempt to reserve at least 2 "
      "GB of disk space.",
      {.needs_restart = needs_restart::no,
       .example = "4",
       .visibility = visibility::tunable},
      2,
      {.min = 1})
  , debug_load_slice_warning_depth(
      *this,
      "debug_load_slice_warning_depth",
      "The recursion depth after which debug logging is enabled automatically "
      "for the log reader.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , tx_registry_log_capacity(*this, "tx_registry_log_capacity")
  , id_allocator_log_capacity(
      *this,
      "id_allocator_log_capacity",
      "Capacity of the `id_allocator` log in number of batches. After it "
      "reaches `id_allocator_stm`, it truncates the log's prefix.",
      {.visibility = visibility::tunable},
      100)
  , id_allocator_batch_size(
      *this,
      "id_allocator_batch_size",
      "The ID allocator allocates messages in batches (each batch is a one log "
      "record) and then serves requests from memory without touching the log "
      "until the batch is exhausted.",
      {.visibility = visibility::tunable},
      1000)
  , enable_sasl(
      *this,
      "enable_sasl",
      "Enable SASL authentication for Kafka connections. Authorization is "
      "required to modify this property. See also "
      "`kafka_enable_authorization`.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , sasl_mechanisms(
      *this,
      "sasl_mechanisms",
      "A list of supported SASL mechanisms. Accepted values: `SCRAM`, "
      "`GSSAPI`, `OAUTHBEARER`.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      {"SCRAM"},
      validate_sasl_mechanisms)
  , sasl_kerberos_config(
      *this,
      "sasl_kerberos_config",
      "The location of the Kerberos `krb5.conf` file for Redpanda.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      "/etc/krb5.conf")
  , sasl_kerberos_keytab(
      *this,
      "sasl_kerberos_keytab",
      "The location of the Kerberos keytab file for Redpanda.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      "/var/lib/redpanda/redpanda.keytab")
  , sasl_kerberos_principal(
      *this,
      "sasl_kerberos_principal",
      "The primary of the Kerberos Service Principal Name (SPN) for Redpanda.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      "redpanda",
      &validate_non_empty_string_opt)
  , sasl_kerberos_principal_mapping(
      *this,
      "sasl_kerberos_principal_mapping",
      "Rules for mapping Kerberos principal names to Redpanda user principals.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      {"DEFAULT"},
      security::validate_kerberos_mapping_rules)
  , kafka_sasl_max_reauth_ms(
      *this,
      "kafka_sasl_max_reauth_ms",
      "The maximum time between Kafka client reauthentications. If a client "
      "has not reauthenticated a connection within this time frame, that "
      "connection is torn down. If this property is not set (or set to "
      "`null`), session expiry is disabled, and a connection could live long "
      "after the client's credentials are expired or revoked.",
      {.needs_restart = needs_restart::no,
       .example = "1000",
       .visibility = visibility::user},
      std::nullopt,
      {.min = 1000ms})
  , kafka_enable_authorization(
      *this,
      "kafka_enable_authorization",
      "Flag to require authorization for Kafka connections. If `null`, the "
      "property is disabled, and authorization is instead enabled by "
      "enable_sasl. * `null`: Ignored. Authorization is enabled with "
      "`enable_sasl`: `true` * `true`: authorization is required. * `false`: "
      "authorization is disabled.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , kafka_mtls_principal_mapping_rules(
      *this,
      "kafka_mtls_principal_mapping_rules",
      "Principal mapping rules for mTLS authentication on the Kafka API. If "
      "`null`, the property is disabled.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt,
      security::tls::validate_rules)
  , kafka_enable_partition_reassignment(
      *this,
      "kafka_enable_partition_reassignment",
      "Enable the Kafka partition reassignment API.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , controller_backend_housekeeping_interval_ms(
      *this,
      "controller_backend_housekeeping_interval_ms",
      "Interval between iterations of controller backend housekeeping loop.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1s)
  , node_management_operation_timeout_ms(
      *this,
      "node_management_operation_timeout_ms",
      "Timeout for executing node management operations.",
      {.visibility = visibility::tunable},
      5s)
  , kafka_request_max_bytes(
      *this,
      "kafka_request_max_bytes",
      "Maximum size of a single request processed using the Kafka API.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      100_MiB)
  , kafka_batch_max_bytes(
      *this,
      "kafka_batch_max_bytes",
      "Maximum size of a batch processed by the server. If the batch is "
      "compressed, the limit applies to the compressed batch size.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1_MiB)
  , kafka_nodelete_topics(
      *this,
      "kafka_nodelete_topics",
      "A list of topics that are protected from deletion and configuration "
      "changes by Kafka clients. Set by default to a list of Redpanda internal "
      "topics.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      {model::kafka_audit_logging_topic(), "__consumer_offsets", "_schemas"},
      &validate_non_empty_string_vec)
  , kafka_noproduce_topics(
      *this,
      "kafka_noproduce_topics",
      "A list of topics that are protected from being produced to by Kafka "
      "clients. Set by default to a list of Redpanda internal topics.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      {},
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
      "Proportional coefficient for compaction PID controller. This must be "
      "negative, because the compaction backlog should decrease when the "
      "number of compaction shares increases.",
      {.visibility = visibility::tunable},
      -12.5)
  , compaction_ctrl_i_coeff(
      *this,
      "compaction_ctrl_i_coeff",
      "Integral coefficient for compaction PID controller.",
      {.visibility = visibility::tunable},
      0.0)
  , compaction_ctrl_d_coeff(
      *this,
      "compaction_ctrl_d_coeff",
      "Derivative coefficient for compaction PID controller.",
      {.visibility = visibility::tunable},
      0.2)
  , compaction_ctrl_min_shares(
      *this,
      "compaction_ctrl_min_shares",
      "Minimum number of I/O and CPU shares that compaction process can use.",
      {.visibility = visibility::tunable},
      10)
  , compaction_ctrl_max_shares(
      *this,
      "compaction_ctrl_max_shares",
      "Maximum number of I/O and CPU shares that compaction process can use.",
      {.visibility = visibility::tunable},
      1000)
  , compaction_ctrl_backlog_size(
      *this,
      "compaction_ctrl_backlog_size",
      "Target backlog size for compaction controller. If not set the max "
      "backlog size is configured to 80% of total disk space available.",
      {.visibility = visibility::tunable},
      std::nullopt)
  , members_backend_retry_ms(
      *this,
      "members_backend_retry_ms",
      "Time between members backend reconciliation loop retries.",
      {.visibility = visibility::tunable},
      5s)
  , kafka_connections_max(
      *this,
      "kafka_connections_max",
      "Maximum number of Kafka client connections per broker. If `null`, the "
      "property is disabled.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , kafka_connections_max_per_ip(
      *this,
      "kafka_connections_max_per_ip",
      "Maximum number of Kafka client connections per IP address, per broker. "
      "If `null`, the property is disabled.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , kafka_connections_max_overrides(
      *this,
      "kafka_connections_max_overrides",
      "A list of IP addresses for which Kafka client connection limits are "
      "overridden and don't apply. For example, `(['127.0.0.1:90', "
      "'50.20.1.1:40']).`",
      {.needs_restart = needs_restart::no,
       .example = R"(['127.0.0.1:90', '50.20.1.1:40'])",
       .visibility = visibility::user},
      {},
      validate_connection_rate)
  , kafka_client_group_byte_rate_quota(
      *this,
      "kafka_client_group_byte_rate_quota",
      "Per-group target produce quota byte rate (bytes per second). Client is "
      "considered part of the group if client_id contains clients_prefix.",
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
      "Size of the Kafka server TCP receive buffer. If `null`, the property is "
      "disabled.",
      {.example = "65536"},
      std::nullopt,
      {.min = 32_KiB, .align = 4_KiB})
  , kafka_rpc_server_tcp_send_buf(
      *this,
      "kafka_rpc_server_tcp_send_buf",
      "Size of the Kafka server TCP transmit buffer. If `null`, the property "
      "is disabled.",
      {.example = "65536"},
      std::nullopt,
      {.min = 32_KiB, .align = 4_KiB})
  , kafka_rpc_server_stream_recv_buf(
      *this,
      "kafka_rpc_server_stream_recv_buf",
      "Maximum size of the user-space receive buffer. If `null`, this limit is "
      "not applied.",
      {.example = "65536", .visibility = visibility::tunable},
      std::nullopt,
      // The minimum is set to match seastar's min_buffer_size (i.e. don't
      // permit setting a max below the min).  The maximum is set to forbid
      // contiguous allocations beyond that size.
      {.min = 512, .max = 512_KiB, .align = 4_KiB})
  , kafka_enable_describe_log_dirs_remote_storage(
      *this,
      "kafka_enable_describe_log_dirs_remote_storage",
      "Whether to include Tiered Storage as a special remote:// directory in "
      "`DescribeLogDirs Kafka` API requests.",
      {.needs_restart = needs_restart::no,
       .example = "false",
       .visibility = visibility::user},
      true)
  , audit_enabled(
      *this,
      "audit_enabled",
      "Enables or disables audit logging. When you set this to true, Redpanda "
      "checks for an existing topic named `_redpanda.audit_log`. If none is "
      "found, Redpanda automatically creates one for you.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , audit_log_num_partitions(
      *this,
      "audit_log_num_partitions",
      "Defines the number of partitions used by a newly-created audit topic. "
      "This configuration applies only to the audit log topic and may be "
      "different from the cluster or other topic configurations. This cannot "
      "be altered for existing audit log topics.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      12)
  , audit_log_replication_factor(
      *this,
      "audit_log_replication_factor",
      "Defines the replication factor for a newly-created audit log topic. "
      "This configuration applies only to the audit log topic and may be "
      "different from the cluster or other topic configurations. This cannot "
      "be altered for existing audit log topics. Setting this value is "
      "optional. If a value is not provided, Redpanda will use the value "
      "specified for `internal_topic_replication_factor`.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , audit_client_max_buffer_size(
      *this,
      "audit_client_max_buffer_size",
      "Defines the number of bytes allocated by the internal audit client for "
      "audit messages. When changing this, you must disable audit logging and "
      "then re-enable it for the change to take effect. Consider increasing "
      "this if your system generates a very large number of audit records in a "
      "short amount of time.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      16_MiB)
  , audit_queue_drain_interval_ms(
      *this,
      "audit_queue_drain_interval_ms",
      "Interval, in milliseconds, at which Redpanda flushes the queued audit "
      "log messages to the audit log topic. Longer intervals may help prevent "
      "duplicate messages, especially in high throughput scenarios, but they "
      "also increase the risk of data loss during shutdowns where the queue is "
      "lost.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      500ms)
  , audit_queue_max_buffer_size_per_shard(
      *this,
      "audit_queue_max_buffer_size_per_shard",
      "Defines the maximum amount of memory in bytes used by the audit buffer "
      "in each shard. Once this size is reached, requests to log additional "
      "audit messages will return a non-retryable error. Limiting the buffer "
      "size per shard helps prevent any single shard from consuming excessive "
      "memory due to audit log messages.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      1_MiB)
  , audit_enabled_event_types(
      *this,
      "audit_enabled_event_types",
      "List of strings in JSON style identifying the event types to include in "
      "the audit log. This may include any of the following: `management, "
      "produce, consume, describe, heartbeat, authenticate, schema_registry, "
      "admin`.",
      {
        .needs_restart = needs_restart::no,
        .example = R"(["management", "describe"])",
        .visibility = visibility::user,
      },
      {"management", "authenticate", "admin"},
      validate_audit_event_types)
  , audit_excluded_topics(
      *this,
      "audit_excluded_topics",
      "List of topics to exclude from auditing.",
      {
        .needs_restart = needs_restart::no,
        .example = R"(["topic1","topic2"])",
        .visibility = visibility::user,
      },
      {},
      validate_audit_excluded_topics)
  , audit_excluded_principals(
      *this,
      "audit_excluded_principals",
      "List of user principals to exclude from auditing.",
      {
        .needs_restart = needs_restart::no,
        .example = R"(["User:principal1","User:principal2"])",
        .visibility = visibility::user,
      },
      {})
  , cloud_storage_enabled(
      *this,
      "cloud_storage_enabled",
      "Enable object storage. Must be set to `true` to use Tiered Storage or "
      "Remote Read Replicas.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::user},
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
  , cloud_storage_disable_archiver_manager(
      *this,
      "cloud_storage_disable_archiver_manager",
      "Use legacy upload mode and do not start archiver_manager.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::user},
      true)
  , cloud_storage_access_key(
      *this,
      "cloud_storage_access_key",
      "AWS or GCP access key. This access key is part of the credentials that "
      "Redpanda requires to authenticate with object storage services for "
      "Tiered Storage. This access key is used with the "
      "<<cloud_storage_secret_key>> to form the complete credentials required "
      "for authentication. To authenticate using IAM roles, see "
      "cloud_storage_credentials_source.",
      {.visibility = visibility::user},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_secret_key(
      *this,
      "cloud_storage_secret_key",
      "Cloud provider secret key.",
      {.visibility = visibility::user, .secret = is_secret::yes},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_region(
      *this,
      "cloud_storage_region",
      "Cloud provider region that houses the bucket or container used for "
      "storage.",
      {.visibility = visibility::user},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_bucket(
      *this,
      "cloud_storage_bucket",
      "AWS or GCP bucket or container that should be used to store data.",
      {.visibility = visibility::user},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_api_endpoint(
      *this,
      "cloud_storage_api_endpoint",
      "Optional API endpoint. - AWS: When blank, this is automatically "
      "generated using <<cloud_storage_region,region>> and "
      "<<cloud_storage_bucket,bucket>>. Otherwise, this uses the value "
      "assigned. - GCP: Uses `storage.googleapis.com`.",
      {.visibility = visibility::user},
      std::nullopt,
      &validate_cloud_storage_api_endpoint)
  , cloud_storage_url_style(
      *this,
      "cloud_storage_url_style",
      "Specifies the addressing style to use for Amazon S3 requests. This "
      "configuration determines how S3 bucket URLs are formatted. You can "
      "choose between: `virtual_host`, (for example, "
      "`<bucket-name>.s3.amazonaws.com`), `path`, (for example, "
      "`s3.amazonaws.com/<bucket-name>`), and `null`. Path style is supported "
      "for backward compatibility with legacy systems. When this property is "
      "not set or is `null`, the client tries to use `virtual_host` "
      "addressing. If the initial request fails, the client automatically "
      "tries the `path` style. If neither addressing style works, Redpanda "
      "terminates the startup, requiring manual configuration to proceed.",
      {.needs_restart = needs_restart::yes,
       .example = "virtual_host",
       .visibility = visibility::user},
      std::nullopt,
      {s3_url_style::virtual_host, s3_url_style::path, std::nullopt})
  , cloud_storage_credentials_source(
      *this,
      "cloud_storage_credentials_source",
      "The source of credentials used to authenticate to object storage "
      "services. Required for cluster provider authentication with IAM roles. "
      "To authenticate using access keys, see cloud_storage_access_key`. "
      "Accepted values: `config_file`, `aws_instance_metadata`, `sts, "
      "gcp_instance_metadata`, `azure_vm_instance_metadata`, "
      "`azure_aks_oidc_federation` ",
      {.needs_restart = needs_restart::yes,
       .example = "config_file",
       .visibility = visibility::user},
      model::cloud_credentials_source::config_file,
      {
        model::cloud_credentials_source::config_file,
        model::cloud_credentials_source::aws_instance_metadata,
        model::cloud_credentials_source::sts,
        model::cloud_credentials_source::gcp_instance_metadata,
        model::cloud_credentials_source::azure_aks_oidc_federation,
        model::cloud_credentials_source::azure_vm_instance_metadata,
      })
  , cloud_storage_azure_managed_identity_id(
      *this,
      "cloud_storage_azure_managed_identity_id",
      "The managed identity ID to use for access to the Azure storage account. "
      "To use Azure managed identities, you must set "
      "`cloud_storage_credentials_source` to `azure_vm_instance_metadata`.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt,
      &validate_non_empty_string_opt)
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
      "partition (ms).",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      100ms)
  , cloud_storage_upload_loop_max_backoff_ms(
      *this,
      "cloud_storage_upload_loop_max_backoff_ms",
      "Max backoff interval when there is nothing to upload for a partition "
      "(ms).",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10s)
  , cloud_storage_max_connections(
      *this,
      "cloud_storage_max_connections",
      "Maximum simultaneous object storage connections per shard, applicable "
      "to upload and download activities.",
      {.visibility = visibility::user},
      20)
  , cloud_storage_disable_tls(
      *this,
      "cloud_storage_disable_tls",
      "Disable TLS for all object storage connections.",
      {.visibility = visibility::user},
      false)
  , cloud_storage_api_endpoint_port(
      *this,
      "cloud_storage_api_endpoint_port",
      "TLS port override.",
      {.visibility = visibility::user},
      443)
  , cloud_storage_trust_file(
      *this,
      "cloud_storage_trust_file",
      "Path to certificate that should be used to validate server certificate "
      "during TLS handshake.",
      {.visibility = visibility::user},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_crl_file(
      *this,
      "cloud_storage_crl_file",
      "Path to certificate revocation list for `cloud_storage_trust_file`.",
      {.visibility = visibility::user},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_initial_backoff_ms(
      *this,
      "cloud_storage_initial_backoff_ms",
      "Initial backoff time for exponential backoff algorithm (ms)",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      100ms)
  , cloud_storage_segment_upload_timeout_ms(
      *this,
      "cloud_storage_segment_upload_timeout_ms",
      "Log segment upload timeout (ms)",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      30s)
  , cloud_storage_manifest_upload_timeout_ms(
      *this,
      "cloud_storage_manifest_upload_timeout_ms",
      "Manifest upload timeout (ms).",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10s)
  , cloud_storage_garbage_collect_timeout_ms(
      *this,
      "cloud_storage_garbage_collect_timeout_ms",
      "Timeout for running the cloud storage garbage collection (ms).",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      30s)
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
      "remote storage (sec).",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1h)
  , cloud_storage_manifest_max_upload_interval_sec(
      *this,
      "cloud_storage_manifest_max_upload_interval_sec",
      "Wait at least this long between partition manifest uploads. Actual time "
      "between uploads may be greater than this interval. If this property is "
      "not set, or null, metadata will be updated after each segment upload.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      60s)
  , cloud_storage_readreplica_manifest_sync_timeout_ms(
      *this,
      "cloud_storage_readreplica_manifest_sync_timeout_ms",
      "Timeout to check if new data is available for partition in S3 for read "
      "replica.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      30s)
  , cloud_storage_metadata_sync_timeout_ms(
      *this,
      "cloud_storage_metadata_sync_timeout_ms",
      "Timeout for SI metadata synchronization.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10s)
  , cloud_storage_housekeeping_interval_ms(
      *this,
      "cloud_storage_housekeeping_interval_ms",
      "Interval for cloud storage housekeeping tasks.",
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
  , cloud_storage_cluster_metadata_upload_interval_ms(
      *this,
      "cloud_storage_cluster_metadata_upload_interval_ms",
      "Time interval to wait between cluster metadata uploads.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1h)
  , cloud_storage_cluster_metadata_upload_timeout_ms(
      *this,
      "cloud_storage_cluster_metadata_upload_timeout_ms",
      "Timeout for cluster metadata uploads.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      60s)
  , cloud_storage_cluster_metadata_num_consumer_groups_per_upload(
      *this,
      "cloud_storage_cluster_metadata_num_consumer_groups_per_upload",
      "Number of groups to upload in a single snapshot object during consumer "
      "offsets upload. Setting a lower value will mean a larger number of "
      "smaller snapshots are uploaded.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1000)
  , cloud_storage_cluster_metadata_retries(
      *this,
      "cloud_storage_cluster_metadata_retries",
      "Number of attempts metadata operations may be retried.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      5)
  , cloud_storage_attempt_cluster_restore_on_bootstrap(
      *this,
      "cloud_storage_attempt_cluster_restore_on_bootstrap",
      "When set to `true`, Redpanda automatically retrieves cluster metadata "
      "from a specified object storage bucket at the cluster's first startup. "
      "This option is ideal for orchestrated deployments, such as Kubernetes. "
      "Ensure any previous cluster linked to the bucket is fully "
      "decommissioned to prevent conflicts between Tiered Storage subsystems.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      false)
  , cloud_storage_idle_threshold_rps(
      *this,
      "cloud_storage_idle_threshold_rps",
      "The cloud storage request rate threshold for idle state detection. If "
      "the average request rate for the configured period is lower than this "
      "threshold the cloud storage is considered being idle.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10.0)
  , cloud_storage_background_jobs_quota(
      *this,
      "cloud_storage_background_jobs_quota",
      "The total number of requests the object storage background jobs can "
      "make during one background housekeeping run. This is a per-shard limit. "
      "Adjusting this limit can optimize object storage traffic and impact "
      "shard performance.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5000)
  , cloud_storage_enable_segment_merging(
      *this,
      "cloud_storage_enable_segment_merging",
      "Enables adjacent segment merging. The segments are reuploaded if there "
      "is an opportunity for that and if it will improve the tiered-storage "
      "performance",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      true)
  , cloud_storage_enable_scrubbing(
      *this,
      "cloud_storage_enable_scrubbing",
      "Enable scrubbing of cloud storage partitions. The scrubber validates "
      "the integrity of data and metadata uploaded to cloud storage.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , cloud_storage_partial_scrub_interval_ms(
      *this,
      "cloud_storage_partial_scrub_interval_ms",
      "Time interval between two partial scrubs of the same partition.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1h)
  , cloud_storage_full_scrub_interval_ms(
      *this,
      "cloud_storage_full_scrub_interval_ms",
      "Time interval between a final scrub and the next.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      12h)
  , cloud_storage_scrubbing_interval_jitter_ms(
      *this,
      "cloud_storage_scrubbing_interval_jitter_ms",
      "Jitter applied to the cloud storage scrubbing interval.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10min)
  , cloud_storage_disable_upload_loop_for_tests(
      *this,
      "cloud_storage_disable_upload_loop_for_tests",
      "Begins the upload loop in tiered-storage-enabled topic partitions. The "
      "property exists to simplify testing and shouldn't be set in production.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , cloud_storage_disable_read_replica_loop_for_tests(
      *this,
      "cloud_storage_disable_read_replica_loop_for_tests",
      "Begins the read replica sync loop in tiered-storage-enabled topic "
      "partitions. The property exists to simplify testing and shouldn't be "
      "set in production.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , disable_cluster_recovery_loop_for_tests(
      *this,
      "disable_cluster_recovery_loop_for_tests",
      "Disables the cluster recovery loop. This property is used to simplify "
      "testing and should not be set in production.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , enable_cluster_metadata_upload_loop(
      *this,
      "enable_cluster_metadata_upload_loop",
      "Enables cluster metadata uploads. Required for whole cluster restore.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      // TODO: make this runtime configurable.
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
  , cloud_storage_recovery_topic_validation_mode(
      *this,
      "cloud_storage_recovery_topic_validation_mode",
      "Validation performed before recovering a topic from object storage. In "
      "case of failure, the reason for the failure appears as `ERROR` lines in "
      "the Redpanda application log. For each topic, this reports errors for "
      "all partitions, but for each partition, only the first error is "
      "reported. This property accepts the following parameters: `no_check`: "
      "Skips the checks for topic recovery. `check_manifest_existence`:  Runs "
      "an existence check on each `partition_manifest`. Fails if there are "
      "connection issues to the object storage. "
      "`check_manifest_and_segment_metadata`: Downloads the manifest and runs "
      "a consistency check, comparing the metadata with the cloud storage "
      "objects. The process fails if metadata references any missing cloud "
      "storage objects.",
      {.needs_restart = needs_restart::no,
       .example = "check_manifest_existence",
       .visibility = visibility::tunable},
      model::recovery_validation_mode::check_manifest_existence,
      {model::recovery_validation_mode::check_manifest_existence,
       model::recovery_validation_mode::check_manifest_and_segment_metadata,
       model::recovery_validation_mode::no_check})
  , cloud_storage_recovery_topic_validation_depth(
      *this,
      "cloud_storage_recovery_topic_validation_depth",
      "Number of metadata segments to validate, from newest to oldest, when "
      "`cloud_storage_recovery_topic_validation_mode` is set to "
      "`check_manifest_and_segment_metadata`.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10)
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
  , cloud_storage_max_throughput_per_shard(
      *this,
      "cloud_storage_max_throughput_per_shard",
      "Max throughput used by tiered-storage per shard in bytes per second. "
      "This value is an upper bound of the throughput available to the "
      "tiered-storage subsystem. This parameter is intended to be used as a "
      "safeguard and in tests when "
      "we need to set precise throughput value independent of actual storage "
      "media. "
      "Please use 'cloud_storage_throughput_limit_percent' instead of this "
      "parameter in the production environment.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1_GiB)
  , cloud_storage_throughput_limit_percent(
      *this,
      "cloud_storage_throughput_limit_percent",
      "Max throughput used by tiered-storage per node expressed as a "
      "percentage of the disk bandwidth. If the server has several disks "
      "Redpanda will take into account only the one which is used to store "
      "tiered-storage cache. Note that even if the tiered-storage is allowed "
      "to use full bandwidth of the disk (100%) it won't necessary use it in "
      "full. The actual usage depend on your workload and the state of the "
      "tiered-storage cache. This parameter is a safeguard that prevents "
      "tiered-storage from using too many system resources and not a "
      "performance tuning knob.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      50,
      {.min = 0, .max = 100})
  , cloud_storage_graceful_transfer_timeout_ms(
      *this,
      "cloud_storage_graceful_transfer_timeout_ms",
      "Time limit on waiting for uploads to complete before a leadership "
      "transfer.  If this is null, leadership transfers will proceed without "
      "waiting.",
      {.needs_restart = needs_restart::no,
       .visibility = visibility::tunable,
       .aliases = {"cloud_storage_graceful_transfer_timeout"}},
      5s)
  , cloud_storage_backend(
      *this,
      "cloud_storage_backend",
      "Optional object storage backend variant used to select API "
      "capabilities. If not supplied, this will be inferred from other "
      "configuration properties. Accepted values: [`unknown`, `aws`, "
      "`google_s3_compat`, `azure`, `minio`]",
      {.needs_restart = needs_restart::yes,
       .example = "aws",
       .visibility = visibility::user},
      model::cloud_storage_backend::unknown,
      {model::cloud_storage_backend::aws,
       model::cloud_storage_backend::google_s3_compat,
       model::cloud_storage_backend::azure,
       model::cloud_storage_backend::minio,
       model::cloud_storage_backend::oracle_s3_compat,
       model::cloud_storage_backend::unknown})
  , cloud_storage_credentials_host(
      *this,
      "cloud_storage_credentials_host",
      "The hostname to connect to for retrieving role based credentials. "
      "Derived from cloud_storage_credentials_source if not set. Only required "
      "when using IAM role based access. To authenticate using access keys, "
      "see `cloud_storage_access_key`.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_spillover_manifest_size(
      *this,
      "cloud_storage_spillover_manifest_size",
      "The size of the manifest which can be offloaded to the cloud. If the "
      "size of the local manifest stored in redpanda exceeds "
      "cloud_storage_spillover_manifest_size x2 the spillover mechanism will "
      "split the manifest into two parts and one of them will be uploaded to "
      "S3.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      64_KiB,
      {.min = 4_KiB, .max = 4_MiB})
  , cloud_storage_spillover_manifest_max_segments(
      *this,
      "cloud_storage_spillover_manifest_max_segments",
      "Maximum number of elements in the spillover manifest that can be "
      "offloaded to the cloud storage. This property is similar to "
      "'cloud_storage_spillover_manifest_size' but "
      "it triggers spillover based on number of segments instead of the size "
      "of the manifest in bytes. The property exists to simplify testing and "
      "shouldn't be set in the production "
      "environment",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , cloud_storage_manifest_cache_size(
      *this,
      "cloud_storage_manifest_cache_size",
      "Amount of memory that can be used to handle tiered-storage metadata",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1_MiB,
      {.min = 64_KiB, .max = 64_MiB})
  , cloud_storage_manifest_cache_ttl_ms(
      *this,
      "cloud_storage_materialized_manifest_ttl_ms",
      "The time interval that determines how long the materialized manifest "
      "can "
      "stay in cache under contention. This parameter is used for performance "
      "tuning. "
      "When the spillover manifest is materialized and stored in cache and the "
      "cache needs to evict it it will use "
      "'cloud_storage_materialized_manifest_ttl_ms' value as a timeout. "
      "The cursor that uses the spillover manifest uses this value as a TTL "
      "interval after which it stops referencing the manifest making it "
      "available for eviction. This only affects spillover manifests under "
      "contention.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10s)
  , cloud_storage_topic_purge_grace_period_ms(
      *this,
      "cloud_storage_topic_purge_grace_period_ms",
      "Grace period during which the purger will refuse to purge the topic.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      30s)
  , cloud_storage_disable_upload_consistency_checks(
      *this,
      "cloud_storage_disable_upload_consistency_checks",
      "Disable all upload consistency checks. This will allow redpanda to "
      "upload logs with gaps and replicate metadata with consistency "
      "violations. Normally, this options should be disabled.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , cloud_storage_disable_metadata_consistency_checks(
      *this,
      "cloud_storage_disable_metadata_consistency_checks",
      "Disable all metadata consistency checks. This will allow redpanda to "
      "replay logs with inconsistent tiered-storage metadata. Normally, this "
      "option should be disabled.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      true)
  , cloud_storage_hydration_timeout_ms(
      *this,
      "cloud_storage_hydration_timeout_ms",
      "Duration to wait for a hydration request to be fulfilled, if hydration "
      "is not completed within this time, the consumer will be notified with a "
      "timeout error.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      600s)
  , cloud_storage_disable_remote_labels_for_tests(
      *this,
      "cloud_storage_disable_remote_labels_for_tests",
      "If 'true', Redpanda disables remote labels and falls back on the "
      "hash-based object naming scheme for new topics. This property exists to "
      "simplify testing "
      "and shouldn't be set in production.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , cloud_storage_azure_storage_account(
      *this,
      "cloud_storage_azure_storage_account",
      "The name of the Azure storage account to use with Tiered Storage. If "
      "`null`, the property is disabled.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::user},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_azure_container(
      *this,
      "cloud_storage_azure_container",
      "The name of the Azure container to use with Tiered Storage. If `null`, "
      "the property is disabled. The container must belong to "
      "cloud_storage_azure_storage_account.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::user},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_azure_shared_key(
      *this,
      "cloud_storage_azure_shared_key",
      "The shared key to be used for Azure Shared Key authentication with the "
      "Azure storage account configured by "
      "`cloud_storage_azure_storage_account`.  If `null`, the property is "
      "disabled. Redpanda expects this key string to be Base64 encoded.",
      {.needs_restart = needs_restart::no,
       .visibility = visibility::user,
       .secret = is_secret::yes},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_azure_adls_endpoint(
      *this,
      "cloud_storage_azure_adls_endpoint",
      "Azure Data Lake Storage v2 endpoint override. Use when hierarchical "
      "namespaces are enabled on your storage account and you have set up a "
      "custom endpoint.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::user},
      std::nullopt,
      &validate_non_empty_string_opt)
  , cloud_storage_azure_adls_port(
      *this,
      "cloud_storage_azure_adls_port",
      "Azure Data Lake Storage v2 port override. See also "
      "`cloud_storage_azure_adls_endpoint`. Use when Hierarchical Namespaces "
      "are enabled on your storage account and you have set up a custom "
      "endpoint.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::user},
      std::nullopt)
  , cloud_storage_azure_hierarchical_namespace_enabled(
      *this,
      "cloud_storage_azure_hierarchical_namespace_enabled",
      "Whether or not an Azure hierarchical namespace is enabled on the "
      "`cloud_storage_azure_storage_account`. If this property is not set, "
      "´cloud_storage_azure_shared_key` must be set, and each node tries to "
      "determine at startup if a hierarchical namespace is enabled. Setting "
      "this property to `true` disables the check and treats a hierarchical "
      "namespace as active. Setting to `false` disables the check and treats a "
      "hierarchical namespace as not active.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
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
  , retention_local_target_bytes_default(
      *this,
      "retention_local_target_bytes_default",
      "Local retention size target for partitions of topics with object "
      "storage write enabled. If `null`, the property is disabled. This "
      "property can be overridden on a per-topic basis by setting "
      "`retention.local.target.bytes` in each topic enabled for Tiered "
      "Storage. Both `retention_local_target_bytes_default` and "
      "`retention_local_target_ms_default` can be set. The limit that is "
      "reached earlier is applied.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , retention_local_target_ms_default(
      *this,
      "retention_local_target_ms_default",
      "Local retention time target for partitions of topics with object "
      "storage write enabled. This property can be overridden on a per-topic "
      "basis by setting `retention.local.target.ms` in each topic enabled for "
      "Tiered Storage. Both `retention_local_target_bytes_default` and "
      "`retention_local_target_ms_default` can be set. The limit that is "
      "reached first is applied.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      24h)
  , retention_local_strict(
      *this,
      "retention_local_strict",
      "Flag to allow Tiered Storage topics to expand to consumable retention "
      "policy limits. When this flag is enabled, non-local retention settings "
      "are used, and local retention settings are used to inform data removal "
      "policies in low-disk space scenarios.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false,
      property<bool>::noop_validator,
      legacy_default<bool>(true, legacy_version{9}))
  , retention_local_strict_override(
      *this,
      "retention_local_strict_override",
      "Trim log data when a cloud topic reaches its local retention limit. "
      "When this option is disabled Redpanda will allow partitions to grow "
      "past the local retention limit, and will be trimmed automatically as "
      "storage reaches the configured target size.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , retention_local_target_capacity_bytes(
      *this,
      "retention_local_target_capacity_bytes",
      "The target capacity (in bytes) that log storage will try to use before "
      "additional retention rules take over to trim data to meet the target. "
      "When no target is specified, storage usage is unbounded. Redpanda Data "
      "recommends setting only one of `retention_local_target_capacity_bytes` "
      "or `retention_local_target_capacity_percent`. If both are set, the "
      "minimum of the two is used as the effective target capacity.",
      {.needs_restart = needs_restart::no,
       .example = "2147483648000",
       .visibility = visibility::user},
      std::nullopt,
      property<std::optional<size_t>>::noop_validator,
      legacy_default<std::optional<size_t>>(std::nullopt, legacy_version{9}))
  , retention_local_target_capacity_percent(
      *this,
      "retention_local_target_capacity_percent",
      "The target capacity in percent of unreserved space "
      "(`disk_reservation_percent`) that log storage will try to use before "
      "additional retention rules will take over to trim data in order to meet "
      "the target. When no target is specified storage usage is unbounded. "
      "Redpanda Data recommends setting only one of "
      "`retention_local_target_capacity_bytes` or "
      "`retention_local_target_capacity_percent`. If both are set, the minimum "
      "of the two is used as the effective target capacity.",
      {.needs_restart = needs_restart::no,
       .example = "80.0",
       .visibility = visibility::user},
      80.0,
      {.min = 0.0, .max = 100.0},
      legacy_default<std::optional<double>>(std::nullopt, legacy_version{9}))
  , retention_local_trim_interval(
      *this,
      "retention_local_trim_interval",
      "The period during which disk usage is checked for disk pressure, and "
      "data is optionally trimmed to meet the target.",
      {.needs_restart = needs_restart::no,
       .example = "31536000000",
       .visibility = visibility::tunable},
      30s)
  , retention_local_trim_overage_coeff(
      *this,
      "retention_local_trim_overage_coeff",
      "The space management control loop reclaims the overage multiplied by "
      "this this coefficient to compensate for data that is written during the "
      "idle period between control loop invocations.",
      {.needs_restart = needs_restart::no,
       .example = "1.8",
       .visibility = visibility::tunable},
      2.0)
  , space_management_enable(
      *this,
      "space_management_enable",
      "Option to explicitly disable automatic disk space management. If this "
      "property was explicitly disabled while using v23.2, it will remain "
      "disabled following an upgrade.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , space_management_enable_override(
      *this,
      "space_management_enable_override",
      "Enable automatic space management. This option is ignored and "
      "deprecated in versions >= v23.3.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , disk_reservation_percent(
      *this,
      "disk_reservation_percent",
      "The percentage of total disk capacity that Redpanda will avoid using. "
      "This applies both when cloud cache and log data share a disk, as well "
      "as when cloud cache uses a dedicated disk. It is recommended to not run "
      "disks near capacity to avoid blocking I/O due to low disk space, as "
      "well as avoiding performance issues associated with SSD garbage "
      "collection.",
      {.needs_restart = needs_restart::no,
       .example = "25.0",
       .visibility = visibility::tunable},
      25.0,
      {.min = 0.0, .max = 100.0},
      legacy_default<double>(0.0, legacy_version{9}))
  , space_management_max_log_concurrency(
      *this,
      "space_management_max_log_concurrency",
      "Maximum parallel logs inspected during space management process.",
      {.needs_restart = needs_restart::no,
       .example = "20",
       .visibility = visibility::tunable},
      20,
      {.min = 1})
  , space_management_max_segment_concurrency(
      *this,
      "space_management_max_segment_concurrency",
      "Maximum parallel segments inspected during space management process.",
      {.needs_restart = needs_restart::no,
       .example = "10",
       .visibility = visibility::tunable},
      10,
      {.min = 1})
  , initial_retention_local_target_bytes_default(
      *this,
      "initial_retention_local_target_bytes_default",
      "Initial local retention size target for partitions of topics with "
      "Tiered Storage enabled. If no initial local target retention is "
      "configured all locally retained data will be delivered to learner when "
      "joining partition replica set.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , initial_retention_local_target_ms_default(
      *this,
      "initial_retention_local_target_ms_default",
      "Initial local retention time target for partitions of topics with "
      "Tiered Storage enabled. If no initial local target retention is "
      "configured all locally retained data will be delivered to learner when "
      "joining partition replica set.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , cloud_storage_cache_size(
      *this,
      "cloud_storage_cache_size",
      "Maximum size of object storage cache. If both this property and "
      "cloud_storage_cache_size_percent are set, Redpanda uses the minimum of "
      "the two.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      0,
      property<uint64_t>::noop_validator,
      legacy_default<uint64_t>(20_GiB, legacy_version{9}))
  , cloud_storage_cache_size_percent(
      *this,
      "cloud_storage_cache_size_percent",
      "Maximum size of the cloud cache as a percentage of unreserved disk "
      "space disk_reservation_percent. The default value for this option is "
      "tuned for a shared disk configuration. Consider increasing the value if "
      "using a dedicated cache disk. The property "
      "<<cloud_storage_cache_size,`cloud_storage_cache_size`>> controls the "
      "same limit expressed as a fixed number of bytes. If both "
      "`cloud_storage_cache_size` and `cloud_storage_cache_size_percent` are "
      "set, Redpanda uses the minimum of the two.",
      {.needs_restart = needs_restart::no,
       .example = "20.0",
       .visibility = visibility::user},
      20.0,
      {.min = 0.0, .max = 100.0},
      legacy_default<std::optional<double>>(std::nullopt, legacy_version{9}))
  , cloud_storage_cache_max_objects(
      *this,
      "cloud_storage_cache_max_objects",
      "Maximum number of objects that may be held in the Tiered Storage cache. "
      " This applies simultaneously with `cloud_storage_cache_size`, and "
      "whichever limit is hit first will trigger trimming of the cache.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      // Enough for a >1TiB cache of 16MiB objects.  Decrease this in case
      // of issues with trim performance.
      100000)
  , cloud_storage_cache_trim_carryover_bytes(
      *this,
      "cloud_storage_cache_trim_carryover_bytes",
      "The cache performs a recursive directory inspection during the cache "
      "trim. The information obtained during the inspection can be carried "
      "over to the next trim operation. This parameter sets a limit on the "
      "memory occupied by objects that can be carried over from one trim to "
      "next, and allows cache to quickly unblock readers before starting the "
      "directory inspection (deprecated)",
      {.needs_restart = needs_restart::no,
       .visibility = visibility::deprecated},
      // This roughly translates to around 1000 carryover file names
      0_KiB)
  , cloud_storage_cache_check_interval_ms(
      *this,
      "cloud_storage_cache_check_interval",
      "Minimum interval between Tiered Storage cache trims, measured in "
      "milliseconds. This setting dictates the cooldown period after a cache "
      "trim operation before another trim can occur. If a cache fetch "
      "operation requests a trim but the interval since the last trim has not "
      "yet passed, the trim will be postponed until this cooldown expires. "
      "Adjusting this interval helps manage the balance between cache size and "
      "retrieval performance.",
      {.visibility = visibility::tunable},
      5s)
  , cloud_storage_cache_trim_walk_concurrency(
      *this,
      "cloud_storage_cache_trim_walk_concurrency",
      "The maximum number of concurrent tasks launched for directory walk "
      "during cache trimming. A higher number allows cache trimming to run "
      "faster but can cause latency spikes due to increased pressure on I/O "
      "subsystem and syscall threads.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1,
      {.min = 1, .max = 1000})
  , cloud_storage_max_segment_readers_per_shard(
      *this,
      "cloud_storage_max_segment_readers_per_shard",
      "Maximum concurrent I/O cursors of materialized remote segments per CPU "
      "core.  If unset, "
      "value of `topic_partitions_per_shard` is used, i.e. one segment reader "
      "per "
      "partition if the shard is at its maximum partition capacity.  These "
      "readers are cached"
      "across Kafka consume requests and store a readahead buffer.",
      {.needs_restart = needs_restart::no,
       .visibility = visibility::tunable,
       .aliases = {"cloud_storage_max_readers_per_shard"}},
      std::nullopt)
  , cloud_storage_max_partition_readers_per_shard(
      *this,
      "cloud_storage_max_partition_readers_per_shard",
      "Maximum partition readers per shard (deprecated)",
      {.needs_restart = needs_restart::no,
       .visibility = visibility::deprecated},
      std::nullopt)
  , cloud_storage_max_concurrent_hydrations_per_shard(
      *this,
      "cloud_storage_max_concurrent_hydrations_per_shard",
      "Maximum concurrent segment hydrations of remote data per CPU core.  If "
      "unset, value of `cloud_storage_max_connections / 2` is used, which "
      "means that half of available S3 bandwidth could be used to download "
      "data from S3. If the cloud storage "
      "cache is empty every new segment reader will require a download. This "
      "will lead to 1:1 mapping between number of partitions scanned by the "
      "fetch request and number of parallel "
      "downloads. If this value is too large the downloads can affect other "
      "workloads. In case of any problem caused by the tiered-storage reads "
      "this value can be lowered. "
      "This will only affect segment hydrations (downloads) but won't affect "
      "cached segments. If fetch request is reading from the tiered-storage "
      "cache its concurrency will only "
      "be limited by available memory.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , cloud_storage_max_materialized_segments_per_shard(
      *this,
      "cloud_storage_max_materialized_segments_per_shard",
      "Maximum concurrent readers of remote data per CPU core.  If unset, "
      "value of `topic_partitions_per_shard` multiplied by 2 is used.",
      {.visibility = visibility::deprecated},
      std::nullopt)
  , cloud_storage_cache_chunk_size(
      *this,
      "cloud_storage_cache_chunk_size",
      "Size of chunks of segments downloaded into object storage cache. "
      "Reduces space usage by only downloading the necessary chunk from a "
      "segment.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      16_MiB)
  , cloud_storage_hydrated_chunks_per_segment_ratio(
      *this,
      "cloud_storage_hydrated_chunks_per_segment_ratio",
      "The maximum number of chunks per segment that can be hydrated at a "
      "time. Above this number, unused chunks will be trimmed.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      0.7)
  , cloud_storage_min_chunks_per_segment_threshold(
      *this,
      "cloud_storage_min_chunks_per_segment_threshold",
      "The minimum number of chunks per segment for trimming to be enabled. If "
      "the number of chunks in a segment is below this threshold, the segment "
      "is small enough that all chunks in it can be hydrated at any given time",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5)
  , cloud_storage_disable_chunk_reads(
      *this,
      "cloud_storage_disable_chunk_reads",
      "Disable chunk reads and switch back to legacy mode where full segments "
      "are downloaded.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , cloud_storage_chunk_eviction_strategy(
      *this,
      "cloud_storage_chunk_eviction_strategy",
      "Selects a strategy for evicting unused cache chunks.",
      {.needs_restart = needs_restart::no,
       .example = "eager",
       .visibility = visibility::tunable},
      model::cloud_storage_chunk_eviction_strategy::eager,
      {model::cloud_storage_chunk_eviction_strategy::eager,
       model::cloud_storage_chunk_eviction_strategy::capped,
       model::cloud_storage_chunk_eviction_strategy::predictive})
  , cloud_storage_chunk_prefetch(
      *this,
      "cloud_storage_chunk_prefetch",
      "Number of chunks to prefetch ahead of every downloaded chunk",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      0)
  , cloud_storage_cache_num_buckets(
      *this,
      "cloud_storage_cache_num_buckets",
      "Divide the object storage cache across the specified number of buckets. "
      "This only works for objects with randomized prefixes. The names are not "
      "changed when the value is set to zero.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      0,
      {.min = 0, .max = 1024})
  , cloud_storage_cache_trim_threshold_percent_size(
      *this,
      "cloud_storage_cache_trim_threshold_percent_size",
      "Trim is triggered when the cache reaches this percent of the maximum "
      "cache size. If this is unset, the default behavior"
      "is to start trim when the cache is about 100% full.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt,
      {.min = 1.0, .max = 100.0})
  , cloud_storage_cache_trim_threshold_percent_objects(
      *this,
      "cloud_storage_cache_trim_threshold_percent_objects",
      "Trim is triggered when the cache reaches this percent of the maximum "
      "object count. If this is unset, the default behavior"
      "is to start trim when the cache is about 100% full.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt,
      {.min = 1.0, .max = 100.0})
  , cloud_storage_inventory_based_scrub_enabled(
      *this,
      "cloud_storage_inventory_based_scrub_enabled",
      "Scrubber uses the latest cloud storage inventory report, if available, "
      "to check if the required objects exist in the bucket or container.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      false)
  , cloud_storage_inventory_id(
      *this,
      "cloud_storage_inventory_id",
      "The name of the scheduled inventory job created by Redpanda to generate "
      "bucket or container inventory reports.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      "redpanda_scrubber_inventory")
  , cloud_storage_inventory_reports_prefix(
      *this,
      "cloud_storage_inventory_reports_prefix",
      "The prefix to the path in the cloud storage bucket or container where "
      "inventory reports will be placed.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      "redpanda_scrubber_inventory")
  , cloud_storage_inventory_self_managed_report_config(
      *this,
      "cloud_storage_inventory_self_managed_report_config",
      "If enabled, Redpanda will not attempt to create the scheduled report "
      "configuration using cloud storage APIs. The scrubbing process will "
      "look for reports in the expected paths in the bucket or container, and "
      "use the latest report found. Primarily intended for use in testing and "
      "on backends where scheduled inventory reports are not supported.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      false)
  , cloud_storage_inventory_report_check_interval_ms(
      *this,
      "cloud_storage_inventory_report_check_interval_ms",
      "Time interval between checks for a new inventory report in the cloud "
      "storage bucket or container.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      6h)
  , cloud_storage_inventory_max_hash_size_during_parse(
      *this,
      "cloud_storage_inventory_max_hash_size_during_parse",
      "Maximum bytes of hashes which will be held in memory before writing "
      "data to disk during inventory report parsing. Affects the number of "
      "files written by inventory service to disk during report parsing, as "
      "when this limit is reached new files are written to disk.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      64_MiB)
  , superusers(
      *this,
      "superusers",
      "List of superuser usernames.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      {})
  , kafka_qdc_latency_alpha(
      *this,
      "kafka_qdc_latency_alpha",
      "Smoothing parameter for Kafka queue depth control latency tracking.",
      {.visibility = visibility::tunable},
      0.002)
  , kafka_qdc_window_size_ms(
      *this,
      "kafka_qdc_window_size_ms",
      "Window size for Kafka queue depth control latency tracking.",
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
      "Smoothing factor for Kafka queue depth control depth tracking.",
      {.visibility = visibility::tunable},
      0.8)
  , kafka_qdc_max_latency_ms(
      *this,
      "kafka_qdc_max_latency_ms",
      "Maximum latency threshold for Kafka queue depth control depth tracking.",
      {.visibility = visibility::user},
      80ms)
  , kafka_qdc_idle_depth(
      *this,
      "kafka_qdc_idle_depth",
      "Queue depth when idleness is detected in Kafka queue depth control.",
      {.visibility = visibility::tunable},
      10)
  , kafka_qdc_min_depth(
      *this,
      "kafka_qdc_min_depth",
      "Minimum queue depth used in Kafka queue depth control.",
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
      "Update frequency for Kafka queue depth control.",
      {.visibility = visibility::tunable},
      7s)
  , zstd_decompress_workspace_bytes(
      *this,
      "zstd_decompress_workspace_bytes",
      "Size of the zstd decompression workspace.",
      {.visibility = visibility::tunable},
      8_MiB)
  , lz4_decompress_reusable_buffers_disabled(
      *this,
      "lz4_decompress_reusable_buffers_disabled",
      "Disable reusable preallocated buffers for LZ4 decompression.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      false)
  , full_raft_configuration_recovery_pattern(
      *this, "full_raft_configuration_recovery_pattern")
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
      "Mode of partition balancing for a cluster. * `node_add`: partition "
      "balancing happens when a node is added. * `continuous`: partition "
      "balancing happens automatically to maintain optimal performance and "
      "availability, based on continuous monitoring for node changes (same as "
      "`node_add`) and also high disk usage. This option requires an "
      "Enterprise license, and it is customized by "
      "`partition_autobalancing_node_availability_timeout_sec` and "
      "`partition_autobalancing_max_disk_usage_percent` properties. * `off`: "
      "partition balancing is disabled. This option is not recommended for "
      "production clusters.",
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
      "When a node is unavailable for at least this timeout duration, it "
      "triggers Redpanda to move partitions off of the node. This property "
      "applies only when `partition_autobalancing_mode` is set to "
      "`continuous`.      ",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      15min)
  , partition_autobalancing_max_disk_usage_percent(
      *this,
      "partition_autobalancing_max_disk_usage_percent",
      "When the disk usage of a node exceeds this threshold, it triggers "
      "Redpanda to move partitions off of the node. This property applies only "
      "when partition_autobalancing_mode is set to `continuous`.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      80,
      {.min = 5, .max = 100})
  , partition_autobalancing_tick_interval_ms(
      *this,
      "partition_autobalancing_tick_interval_ms",
      "Partition autobalancer tick interval.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      30s)
  , partition_autobalancing_movement_batch_size_bytes(
      *this,
      "partition_autobalancing_movement_batch_size_bytes",
      "Total size of partitions that autobalancer is going to move in one "
      "batch (deprecated, use partition_autobalancing_concurrent_moves to "
      "limit the autobalancer concurrency)",
      {.needs_restart = needs_restart::no,
       .visibility = visibility::deprecated},
      5_GiB)
  , partition_autobalancing_concurrent_moves(
      *this,
      "partition_autobalancing_concurrent_moves",
      "Number of partitions that can be reassigned at once.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      50)
  , partition_autobalancing_tick_moves_drop_threshold(
      *this,
      "partition_autobalancing_tick_moves_drop_threshold",
      "If the number of scheduled tick moves drops by this ratio, a new tick "
      "is scheduled immediately. Valid values are (0, 1]. For example, with a "
      "value of 0.2 and 100 scheduled moves in a tick, a new tick is scheduled "
      "when the in-progress moves are fewer than 80.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      0.2,
      &validate_0_to_1_ratio)
  , partition_autobalancing_min_size_threshold(
      *this,
      "partition_autobalancing_min_size_threshold",
      "Minimum size of partition that is going to be prioritized when "
      "rebalancing a cluster due to the disk size threshold being breached. "
      "This value is calculated automatically by default.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , partition_autobalancing_topic_aware(
      *this,
      "partition_autobalancing_topic_aware",
      "If `true`, Redpanda prioritizes balancing a topic’s partition replica "
      "count evenly across all brokers while it’s balancing the cluster’s "
      "overall partition count. Because different topics in a cluster can have "
      "vastly different load profiles, this better distributes the workload of "
      "the most heavily-used topics evenly across brokers.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , enable_leader_balancer(
      *this,
      "enable_leader_balancer",
      "Enable automatic leadership rebalancing.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , leader_balancer_mode(*this, "leader_balancer_mode")
  , leader_balancer_idle_timeout(
      *this,
      "leader_balancer_idle_timeout",
      "Leadership rebalancing idle timeout.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      2min)
  , leader_balancer_mute_timeout(
      *this,
      "leader_balancer_mute_timeout",
      "Leadership rebalancing mute timeout.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5min)
  , leader_balancer_node_mute_timeout(
      *this,
      "leader_balancer_mute_timeout",
      "Leadership rebalancing node mute timeout.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      20s)
  , leader_balancer_transfer_limit_per_shard(
      *this,
      "leader_balancer_transfer_limit_per_shard",
      "Per shard limit for in-progress leadership transfers.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      512,
      {.min = 1, .max = 2048})
  , default_leaders_preference(
      *this,
      "default_leaders_preference",
      "Default settings for preferred location of topic partition leaders. "
      "It can be either \"none\" (no preference), "
      "or \"racks:<rack1>,<rack2>,...\" (prefer brokers with rack id from the "
      "list).",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      config::leaders_preference{})
  , core_balancing_on_core_count_change(
      *this,
      "core_balancing_on_core_count_change",
      "If set to `true`, and if after a restart the number of cores changes, "
      "Redpanda will move partitions between cores to maintain balanced "
      "partition distribution.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , core_balancing_continuous(
      *this,
      "core_balancing_continuous",
      "If set to `true`, move partitions between cores in runtime to maintain "
      "balanced partition distribution.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , core_balancing_debounce_timeout(
      *this,
      "core_balancing_debounce_timeout",
      "Interval, in milliseconds, between trigger and invocation of core "
      "balancing.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10s)
  , internal_topic_replication_factor(
      *this,
      "internal_topic_replication_factor",
      "Target replication factor for internal topics.",
      {.visibility = visibility::user},
      3)
  , health_manager_tick_interval(
      *this,
      "health_manager_tick_interval",
      "How often the health manager runs.",
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
      "Maximum age of the metadata cached in the health monitor of a "
      "non-controller broker.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      10s)
  , storage_space_alert_free_threshold_percent(
      *this,
      "storage_space_alert_free_threshold_percent",
      "Threshold of minimum percent free space before setting storage space "
      "alert.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5,
      {.min = 0, .max = 50})
  , storage_space_alert_free_threshold_bytes(
      *this,
      "storage_space_alert_free_threshold_bytes",
      "Threshold of minimum bytes free space before setting storage space "
      "alert.",
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
      "the broker configuration `data_directory`. If set to `true`, Redpanda "
      "will refuse to start if the file is not found in the data directory.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , alive_timeout_ms(
      *this,
      "alive_timeout_ms",
      "The amount of time since the last broker status heartbeat. After this "
      "time, a broker is considered offline and not alive.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      5s)
  , memory_abort_on_alloc_failure(
      *this,
      "memory_abort_on_alloc_failure",
      "If `true`, the Redpanda process will terminate immediately when an "
      "allocation cannot be satisfied due to memory exhaustion. If false, an "
      "exception is thrown.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      true)
  , sampled_memory_profile(
      *this,
      "memory_enable_memory_sampling",
      "When `true`, memory allocations are sampled and tracked. A sampled live "
      "set of allocations can then be retrieved from the Admin API. "
      "Additionally, Redpanda will periodically log the top-n allocation "
      "sites.",
      {// Enabling/Disabling this dynamically doesn't make much sense as for the
       // memory profile to be meaning full you'll want to have this on from the
       // beginning. However, we still provide the option to be able to disable
       // it dynamically in case something goes wrong
       .needs_restart = needs_restart::no,
       .visibility = visibility::tunable},
      true)
  , enable_metrics_reporter(
      *this,
      "enable_metrics_reporter",
      "Enable the cluster metrics reporter. If `true`, the metrics reporter "
      "collects and exports to Redpanda Data a set of customer usage metrics "
      "at the interval set by `metrics_reporter_report_interval`. The cluster "
      "metrics of the metrics reporter are different from the monitoring "
      "metrics. * The metrics reporter exports customer usage metrics for "
      "consumption by Redpanda Data.* Monitoring metrics are exported for "
      "consumption by Redpanda users.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , metrics_reporter_tick_interval(
      *this,
      "metrics_reporter_tick_interval",
      "Cluster metrics reporter tick interval.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1min)
  , metrics_reporter_report_interval(
      *this,
      "metrics_reporter_report_interval",
      "Cluster metrics reporter report interval.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      24h)
  , metrics_reporter_url(
      *this,
      "metrics_reporter_url",
      "URL of the cluster metrics reporter.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      "https://m.rp.vectorized.io/v2")
  , features_auto_enable(
      *this,
      "features_auto_enable",
      "Whether new feature flags auto-activate after upgrades (true) or must "
      "wait for manual activation via the Admin API (false).",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      true)
  , enable_rack_awareness(
      *this,
      "enable_rack_awareness",
      "Enable rack-aware replica assignment.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , node_status_interval(
      *this,
      "node_status_interval",
      "Time interval between two node status messages. Node status messages "
      "establish liveness status outside of the Raft protocol.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      100ms)
  , node_status_reconnect_max_backoff_ms(
      *this,
      "node_status_reconnect_max_backoff_ms",
      "Maximum backoff (in milliseconds) to reconnect to an unresponsive peer "
      "during node status liveness checks.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      15s)
  , enable_controller_log_rate_limiting(
      *this,
      "enable_controller_log_rate_limiting",
      "Limits the write rate for the controller log.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , rps_limit_topic_operations(
      *this,
      "rps_limit_topic_operations",
      "Rate limit for controller topic operations.",
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
      "Rate limit for controller ACLs and user's operations.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1000)
  , controller_log_accummulation_rps_capacity_acls_and_users_operations(
      *this,
      "controller_log_accummulation_rps_capacity_acls_and_users_operations",
      "Maximum capacity of rate limit accumulation in controller ACLs and "
      "users operations limit.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , rps_limit_node_management_operations(
      *this,
      "rps_limit_node_management_operations",
      "Rate limit for controller node management operations.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1000)
  , controller_log_accummulation_rps_capacity_node_management_operations(
      *this,
      "controller_log_accummulation_rps_capacity_node_management_operations",
      "Maximum capacity of rate limit accumulation in controller node "
      "management operations limit.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , rps_limit_move_operations(
      *this,
      "rps_limit_move_operations",
      "Rate limit for controller move operations.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1000)
  , controller_log_accummulation_rps_capacity_move_operations(
      *this,
      "controller_log_accummulation_rps_capacity_move_operations",
      "Maximum capacity of rate limit accumulation in controller move "
      "operations limit.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , rps_limit_configuration_operations(
      *this,
      "rps_limit_configuration_operations",
      "Rate limit for controller configuration operations.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      1000)
  , controller_log_accummulation_rps_capacity_configuration_operations(
      *this,
      "controller_log_accummulation_rps_capacity_configuration_operations",
      "Maximum capacity of rate limit accumulation in controller configuration "
      "operations limit.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt)
  , kafka_throughput_limit_node_in_bps(
      *this,
      "kafka_throughput_limit_node_in_bps",
      "The maximum rate of all ingress Kafka API traffic for a node. Includes "
      "all Kafka API traffic (requests, responses, headers, fetched data, "
      "produced data, etc.). If `null`, the property is disabled, and traffic "
      "is not limited.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt,
      {.min = 1})
  , kafka_throughput_limit_node_out_bps(
      *this,
      "kafka_throughput_limit_node_out_bps",
      "The maximum rate of all egress Kafka traffic for a node. Includes all "
      "Kafka API traffic (requests, responses, headers, fetched data, produced "
      "data, etc.). If `null`, the property is disabled, and traffic is not "
      "limited.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt,
      {.min = 1})
  , kafka_throughput_throttling_v2(
      *this,
      "kafka_throughput_throttling_v2",
      "Enables an updated algorithm for enforcing node throughput limits based "
      "on a shared token bucket, introduced with Redpanda v23.3.8. Set this "
      "property to `false` if you need to use the quota balancing algorithm "
      "from Redpanda v23.3.7 and older.  This property defaults to `true` for "
      "all new or upgraded Redpanda clusters. Disabling this property is not "
      "recommended. It causes your Redpanda cluster to use an outdated "
      "throughput throttling mechanism. Only set this to `false` when advised "
      "to do so by Redpanda support.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      true)
  , kafka_throughput_replenish_threshold(
      *this,
      "kafka_throughput_replenish_threshold",
      "Threshold for refilling the token bucket as part of enforcing "
      "throughput limits. This only applies when "
      "kafka_throughput_throttling_v2 is `true`. This threshold is evaluated "
      "with each request for data. When the number of tokens to replenish "
      "exceeds this threshold, then tokens are added to the token bucket. This "
      "ensures that the atomic is not being updated for the token count with "
      "each request. The range for this threshold is automatically clamped to "
      "the corresponding throughput limit for ingress and egress.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::nullopt,
      {.min = 1})
  , kafka_quota_balancer_window(
      *this,
      "kafka_quota_balancer_window_ms",
      "Time window used to average current throughput measurement for quota "
      "balancer, in milliseconds.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      5000ms,
      {.min = 1ms,
       .max = std::chrono::milliseconds(std::numeric_limits<int32_t>::max())})
  , kafka_quota_balancer_node_period(
      *this,
      "kafka_quota_balancer_node_period_ms",
      "Intra-node throughput quota balancer invocation period, in "
      "milliseconds. When set to 0, the balancer is disabled and makes all the "
      "throughput quotas immutable.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      0ms,
      {.min = 0ms})
  , kafka_quota_balancer_min_shard_throughput_ratio(
      *this,
      "kafka_quota_balancer_min_shard_throughput_ratio",
      "The minimum value of the throughput quota a shard can get in the "
      "process of quota balancing, expressed as a ratio of default shard "
      "quota. While the value applies equally to ingress and egress traffic, "
      "the default shard quota can be different for ingress and egress and "
      "therefore result in different minimum throughput bytes-per-second (bps) "
      "values. Both `kafka_quota_balancer_min_shard_throughput_ratio` and "
      "`kafka_quota_balancer_min_shard_throughput_bps` can be specified at the "
      "same time. In this case, the balancer will not decrease the effective "
      "shard quota below the largest bps value of each of these two "
      "properties. If set to `0.0`, the minimum is disabled. If set to `1.0`, "
      "the balancer won't be able to rebalance quota without violating this "
      "ratio, preventing the balancer from adjusting shards' quotas.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      0.01,
      &validate_0_to_1_ratio)
  , kafka_quota_balancer_min_shard_throughput_bps(
      *this,
      "kafka_quota_balancer_min_shard_throughput_bps",
      "The minimum value of the throughput quota a shard can get in the "
      "process of quota balancing, expressed in bytes per second. The value "
      "applies equally to ingress and egress traffic. "
      "`kafka_quota_balancer_min_shard_throughput_bps` doesn't override the "
      "limit settings, `kafka_throughput_limit_node_in_bps` and "
      "`kafka_throughput_limit_node_out_bps`. Consequently, the value of "
      "`kafka_throughput_limit_node_in_bps` or "
      "`kafka_throughput_limit_node_out_bps` can result in lesser throughput "
      "than kafka_quota_balancer_min_shard_throughput_bps. Both "
      "`kafka_quota_balancer_min_shard_throughput_ratio` and "
      "`kafka_quota_balancer_min_shard_throughput_bps` can be specified at the "
      "same time. In this case, the balancer will not decrease the effective "
      "shard quota below the largest bytes-per-second (bps) value of each of "
      "these two properties. If set to `0`, no minimum is enforced.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      256,
      {.min = 0})
  , kafka_throughput_controlled_api_keys(
      *this,
      "kafka_throughput_controlled_api_keys",
      "List of Kafka API keys that are subject to cluster-wide and node-wide "
      "throughput limit control.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      {"produce", "fetch"})
  , kafka_throughput_control(
      *this,
      "kafka_throughput_control",
      "List of throughput control groups that define exclusions from node-wide "
      "throughput limits. Clients excluded from node-wide throughput limits "
      "are still potentially subject to client-specific throughput limits. For "
      "more information see "
      "https://docs.redpanda.com/current/reference/properties/"
      "cluster-properties/#kafka_throughput_control.",
      {
        .needs_restart = needs_restart::no,
        .example
        = R"([{'name': 'first_group','client_id': 'client1'}, {'client_id': 'consumer-\d+'}, {'name': 'catch all'}])",
        .visibility = visibility::user,
      },
      {},
      [](auto& v) {
          return validate_throughput_control_groups(v.cbegin(), v.cend());
      })
  , node_isolation_heartbeat_timeout(
      *this,
      "node_isolation_heartbeat_timeout",
      "How long after the last heartbeat request a node will wait before "
      "considering itself to be isolated.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      3000,
      {.min = 100, .max = 10000})
  , controller_snapshot_max_age_sec(
      *this,
      "controller_snapshot_max_age_sec",
      "Maximum amount of time before Redpanda attempts to create a controller "
      "snapshot after a new controller command appears.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      60s)
  , legacy_permit_unsafe_log_operation(
      *this,
      "legacy_permit_unsafe_log_operation",
      "Flag to enable a Redpanda cluster operator to use unsafe control "
      "characters within strings, such as consumer group names or user names. "
      "This flag applies only for Redpanda clusters that were originally on "
      "version 23.1 or earlier and have been upgraded to version 23.2 or "
      "later. Starting in version 23.2, newly-created Redpanda clusters ignore "
      "this property.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      true)
  , legacy_unsafe_log_warning_interval_sec(
      *this,
      "legacy_unsafe_log_warning_interval_sec",
      "Period at which to log a warning about using unsafe strings containing "
      "control characters. If unsafe strings are permitted by "
      "`legacy_permit_unsafe_log_operation`, a warning will be logged at an "
      "interval specified by this property.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      300s)
  , enable_schema_id_validation(
      *this,
      "enable_schema_id_validation",
      "Mode to enable server-side schema ID validation. Accepted Values: * "
      "`none`: Schema validation is disabled (no schema ID checks are done). "
      "Associated topic properties cannot be modified. * `redpanda`: Schema "
      "validation is enabled. Only Redpanda topic properties are accepted. * "
      "`compat`: Schema validation is enabled. Both Redpanda and compatible "
      "topic properties are accepted.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      pandaproxy::schema_registry::schema_id_validation_mode::none,
      {pandaproxy::schema_registry::schema_id_validation_mode::none,
       pandaproxy::schema_registry::schema_id_validation_mode::redpanda,
       pandaproxy::schema_registry::schema_id_validation_mode::compat})
  , kafka_schema_id_validation_cache_capacity(
      *this,
      "kafka_schema_id_validation_cache_capacity",
      "Per-shard capacity of the cache for validating schema IDs.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      128)
  , schema_registry_normalize_on_startup(
      *this,
      "schema_registry_normalize_on_startup",
      "Normalize schemas as they are read from the topic on startup.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::user},
      false)
  , pp_sr_smp_max_non_local_requests(
      *this,
      "pp_sr_smp_max_non_local_requests",
      "Maximum number of Cross-core(Inter-shard communication) requests "
      "pending in HTTP Proxy and Schema Registry seastar::smp group. (For more "
      "details, see the `seastar::smp_service_group` documentation).",
      {.needs_restart = needs_restart::yes, .visibility = visibility::tunable},
      std::nullopt)
  , max_in_flight_schema_registry_requests_per_shard(
      *this,
      "max_in_flight_schema_registry_requests_per_shard",
      "Maximum number of in-flight HTTP requests to Schema Registry permitted "
      "per shard.  Any additional requests above this limit will be rejected "
      "with a 429 error.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      500,
      {.min = 1})
  , max_in_flight_pandaproxy_requests_per_shard(
      *this,
      "max_in_flight_pandaproxy_requests_per_shard",
      "Maximum number of in-flight HTTP requests to HTTP Proxy permitted per "
      "shard.  Any additional requests above this limit will be rejected with "
      "a 429 error.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      500,
      {.min = 1})
  , kafka_memory_share_for_fetch(
      *this,
      "kafka_memory_share_for_fetch",
      "The share of Kafka subsystem memory that can be used for fetch read "
      "buffers, as a fraction of the Kafka subsystem memory amount.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::user},
      0.5,
      {.min = 0.0, .max = 1.0})
  , kafka_memory_batch_size_estimate_for_fetch(
      *this,
      "kafka_memory_batch_size_estimate_for_fetch",
      "The size of the batch used to estimate memory consumption for fetch "
      "requests, in bytes. Smaller sizes allow more concurrent fetch requests "
      "per shard. Larger sizes prevent running out of memory because of too "
      "many concurrent fetch requests.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      1_MiB)
  , cpu_profiler_enabled(
      *this,
      "cpu_profiler_enabled",
      "Enables CPU profiling for Redpanda.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , cpu_profiler_sample_period_ms(
      *this,
      "cpu_profiler_sample_period_ms",
      "The sample period for the CPU profiler.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      100ms,
      {.min = 1ms})
  , rpk_path(
      *this,
      "rpk_path",
      "Path to RPK binary",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      "/usr/bin/rpk")
  , debug_bundle_storage_dir(
      *this,
      "debug_bundle_storage_dir",
      "Path to the debug bundle storage directory. Note: Changing this path "
      "does not clean up existing debug bundles. If not set, the debug bundle "
      "is stored in the Redpanda data directory specified in the redpanda.yaml "
      "broker configuration file.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , debug_bundle_auto_removal_seconds(
      *this,
      "debug_bundle_auto_removal_seconds",
      "If set, how long debug bundles are kept in the debug bundle storage "
      "directory after they are created. If not set, debug bundles are kept "
      "indefinitely.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::nullopt)
  , oidc_discovery_url(
      *this,
      "oidc_discovery_url",
      "The URL pointing to the well-known discovery endpoint for the OIDC "
      "provider.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      "https://auth.prd.cloud.redpanda.com/.well-known/openid-configuration",
      [](const auto& v) -> std::optional<ss::sstring> {
          auto res = security::oidc::parse_url(v);
          if (res.has_error()) {
              return res.error().message();
          }
          return std::nullopt;
      })
  , oidc_token_audience(
      *this,
      "oidc_token_audience",
      "A string representing the intended recipient of the token.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      "redpanda")
  , oidc_clock_skew_tolerance(
      *this,
      "oidc_clock_skew_tolerance",
      "The amount of time (in seconds) to allow for when validating the expiry "
      "claim in the token.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      std::chrono::seconds{} * 30)
  , oidc_principal_mapping(
      *this,
      "oidc_principal_mapping",
      "Rule for mapping JWT payload claim to a Redpanda user principal.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      "$.sub",
      security::oidc::validate_principal_mapping_rule)
  , oidc_keys_refresh_interval(
      *this,
      "oidc_keys_refresh_interval",
      "The frequency of refreshing the JSON Web Keys (JWKS) used to validate "
      "access tokens.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      1h)
  , http_authentication(
      *this,
      "http_authentication",
      "A list of supported HTTP authentication mechanisms. Accepted Values: "
      "`BASIC`, `OIDC`",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      {"BASIC"},
      validate_http_authn_mechanisms)
  , enable_mpx_extensions(
      *this,
      "enable_mpx_extensions",
      "Enable Redpanda extensions for MPX.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      false)
  , virtual_cluster_min_producer_ids(
      *this,
      "virtual_cluster_min_producer_ids",
      "Minimum number of active producers per virtual cluster.",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      std::numeric_limits<uint64_t>::max(),
      {.min = 1})
  , unsafe_enable_consumer_offsets_delete_retention(
      *this,
      "unsafe_enable_consumer_offsets_delete_retention",
      "Enables delete retention of consumer offsets topic. This is an "
      "internal-only configuration and should be enabled only after consulting "
      "with Redpanda support.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::user},
      false)
  , tls_min_version(
      *this,
      "tls_min_version",
      "The minimum TLS version that Redpanda clusters support. This property "
      "prevents client applications from negotiating a downgrade to the TLS "
      "version when they make a connection to a Redpanda cluster.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::user},
      tls_version::v1_2,
      {tls_version::v1_0,
       tls_version::v1_1,
       tls_version::v1_2,
       tls_version::v1_3})
  , iceberg_enabled(
      *this,
      "iceberg_enabled",
      "Enables the translation of topic data into Iceberg tables. Setting "
      "iceberg_enabled to true activates the feature at the cluster level, but "
      "each topic must also set the redpanda.iceberg.enabled topic-level "
      "property to true to use it. If iceberg_enabled is set to false, the "
      "feature is disabled for all topics in the cluster, overriding any "
      "topic-level settings.",
      {.needs_restart = needs_restart::yes, .visibility = visibility::user},
      false)
  , development_enable_cloud_topics(
      *this,
      "development_enable_cloud_topics",
      "Enable cloud topics.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , development_feature_property_testing_only(
      *this,
      "development_feature_property_testing_only",
      "Development feature property for testing only.",
      {.needs_restart = needs_restart::no, .visibility = visibility::user},
      false)
  , enable_developmental_unrecoverable_data_corrupting_features(
      *this,
      "enable_developmental_unrecoverable_data_corrupting_features",
      "Development features should never be enabled in a production cluster, "
      "or any cluster where stability, data loss, or the ability to upgrade "
      "are a concern. To enable experimental features, set the value of this "
      "configuration option to the current unix epoch expressed in seconds. "
      "The value must be within one hour of the current time on the broker."
      "Once experimental features are enabled they cannot be disabled",
      {.needs_restart = needs_restart::no, .visibility = visibility::tunable},
      "",
      [this](const ss::sstring& v) -> std::optional<ss::sstring> {
          if (development_features_enabled()) {
              return fmt::format(
                "Development feature flag cannot be changed once enabled.");
          }

          const auto time_since_epoch
            = std::chrono::system_clock::now().time_since_epoch();

          try {
              const auto key = std::chrono::seconds(
                boost::lexical_cast<int64_t>(v));

              const auto dur = std::chrono::abs(time_since_epoch - key);
              if (dur > std::chrono::hours(1)) {
                  return fmt::format(
                    "Invalid key '{}'. Must be within 1 hour of the current "
                    "unix epoch in seconds.",
                    key.count());
              }
          } catch (const boost::bad_lexical_cast&) {
              return fmt::format("Could not convert '{}' to integer", v);
          }

          return std::nullopt;
      }) {}

configuration::error_map_t configuration::load(const YAML::Node& root_node) {
    if (!root_node["redpanda"]) {
        throw std::invalid_argument("'redpanda' root is required");
    }

    auto ignore = node().property_names_and_aliases();

    return config_store::read_yaml(root_node["redpanda"], std::move(ignore));
}

configuration& shard_local_cfg() {
    static thread_local configuration cfg;
    return cfg;
}
} // namespace config
