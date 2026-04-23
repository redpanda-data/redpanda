/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include <string_view>

namespace kafka {

/**
 * Topic property names.
 */
inline constexpr std::string_view topic_property_retention_bytes
  = "retention.bytes";

inline constexpr std::string_view topic_property_retention_duration
  = "retention.ms";

inline constexpr std::string_view topic_property_cleanup_policy
  = "cleanup.policy";

inline constexpr std::string_view topic_property_compression
  = "compression.type";

inline constexpr std::string_view topic_property_retention_local_target_bytes
  = "retention.local.target.bytes";

inline constexpr std::string_view topic_property_retention_local_target_ms
  = "retention.local.target.ms";

inline constexpr std::string_view
  topic_property_initial_retention_local_target_bytes
  = "initial.retention.local.target.bytes";

inline constexpr std::string_view
  topic_property_initial_retention_local_target_ms
  = "initial.retention.local.target.ms";

inline constexpr std::string_view topic_property_remote_allow_gaps
  = "redpanda.remote.allowgaps";

inline constexpr std::string_view topic_property_replication_factor
  = "replication.factor";

inline constexpr std::string_view topic_property_max_message_bytes
  = "max.message.bytes";

inline constexpr std::string_view topic_property_timestamp_type
  = "message.timestamp.type";

inline constexpr std::string_view topic_property_delete_retention_ms
  = "delete.retention.ms";

inline constexpr std::string_view topic_property_min_compaction_lag_ms
  = "min.compaction.lag.ms";

inline constexpr std::string_view topic_property_max_compaction_lag_ms
  = "max.compaction.lag.ms";

inline constexpr std::string_view topic_property_read_replica
  = "redpanda.remote.readreplica";

inline constexpr std::string_view topic_property_recovery
  = "redpanda.remote.recovery";

inline constexpr std::string_view topic_property_mpx_virtual_cluster_id
  = "redpanda.virtual.cluster.id";

inline constexpr std::string_view topic_property_leaders_preference
  = "redpanda.leaders.preference";

inline constexpr std::string_view topic_property_replicas_preference
  = "redpanda.replicas.preference";

inline constexpr std::string_view topic_property_redpanda_storage_mode
  = "redpanda.storage.mode";

} // namespace kafka
