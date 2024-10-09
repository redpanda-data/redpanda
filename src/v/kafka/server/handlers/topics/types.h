/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "cluster/types.h"
#include "container/fragmented_vector.h"
#include "kafka/protocol/schemata/create_topics_request.h"
#include "kafka/protocol/schemata/create_topics_response.h"
#include "kafka/protocol/topic_properties.h"
#include "kafka/server/errors.h"
#include "kafka/server/handlers/configs/config_response_utils.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "utils/absl_sstring_hash.h"

#include <absl/container/flat_hash_map.h>
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include <array>

namespace kafka {

/**
 * We use `flat_hash_map` in here since those map are small and short lived.
 */
using config_map_t
  = absl::flat_hash_map<ss::sstring, ss::sstring, sstring_hash, sstring_eq>;
/**
 * Topic properties string names
 */
inline constexpr std::string_view topic_property_compaction_strategy
  = "compaction.strategy";
inline constexpr std::string_view topic_property_timestamp_type
  = "message.timestamp.type";
inline constexpr std::string_view topic_property_segment_size = "segment.bytes";
inline constexpr std::string_view topic_property_max_message_bytes
  = "max.message.bytes";
inline constexpr std::string_view topic_property_recovery
  = "redpanda.remote.recovery";
inline constexpr std::string_view topic_property_remote_write
  = "redpanda.remote.write";
inline constexpr std::string_view topic_property_remote_read
  = "redpanda.remote.read";
inline constexpr std::string_view topic_property_read_replica
  = "redpanda.remote.readreplica";
inline constexpr std::string_view topic_property_replication_factor
  = "replication.factor";
inline constexpr std::string_view topic_property_remote_delete
  = "redpanda.remote.delete";
inline constexpr std::string_view topic_property_segment_ms = "segment.ms";
inline constexpr std::string_view topic_property_write_caching
  = "write.caching";

inline constexpr std::string_view topic_property_flush_ms = "flush.ms";
inline constexpr std::string_view topic_property_flush_bytes = "flush.bytes";

// Server side schema id validation
inline constexpr std::string_view topic_property_record_key_schema_id_validation
  = "redpanda.key.schema.id.validation";
inline constexpr std::string_view
  topic_property_record_key_subject_name_strategy
  = "redpanda.key.subject.name.strategy";
inline constexpr std::string_view
  topic_property_record_value_schema_id_validation
  = "redpanda.value.schema.id.validation";
inline constexpr std::string_view
  topic_property_record_value_subject_name_strategy
  = "redpanda.value.subject.name.strategy";

// Server side schema id validation (compat names)
inline constexpr std::string_view
  topic_property_record_key_schema_id_validation_compat
  = "confluent.key.schema.validation";
inline constexpr std::string_view
  topic_property_record_key_subject_name_strategy_compat
  = "confluent.key.subject.name.strategy";
inline constexpr std::string_view
  topic_property_record_value_schema_id_validation_compat
  = "confluent.value.schema.validation";
inline constexpr std::string_view
  topic_property_record_value_subject_name_strategy_compat
  = "confluent.value.subject.name.strategy";

inline constexpr std::string_view topic_property_mpx_virtual_cluster_id
  = "redpanda.virtual.cluster.id";

inline constexpr std::string_view topic_property_iceberg_enabled
  = "redpanda.iceberg.enabled";

inline constexpr std::string_view topic_property_leaders_preference
  = "redpanda.leaders.preference";

inline constexpr std::string_view topic_property_cloud_topic_enabled
  = "redpanda.cloud_topic.enabled";

// Kafka topic properties that is not relevant for Redpanda
// Or cannot be altered with kafka alter handler
inline constexpr std::array<std::string_view, 20> allowlist_topic_noop_confs = {

  // Not used in Redpanda
  "unclean.leader.election.enable",
  "message.downconversion.enable",
  "segment.index.bytes",
  "segment.jitter.ms",
  "min.insync.replicas",
  "min.compaction.lag.ms",
  "min.cleanable.dirty.ratio",
  "message.timestamp.difference.max.ms",
  "message.format.version",
  "max.compaction.lag.ms",
  "leader.replication.throttled.replicas",
  "index.interval.bytes",
  "follower.replication.throttled.replicas",
  "flush.messages",
  "file.delete.delay.ms",
  "delete.retention.ms",
  "preallocate",
};

/// \brief Type representing Kafka protocol response from
/// CreateTopics, DeleteTopics and CreatePartitions requests
/// the types used here match the types used in Kafka protocol specification
struct topic_op_result {
    model::topic topic;
    error_code ec;
    std::optional<ss::sstring> err_msg;
};

inline creatable_topic_result
from_cluster_topic_result(const cluster::topic_result& err) {
    return {
      .name = err.tp_ns.tp,
      .error_code = map_topic_error_code(err.ec),
      .error_message = cluster::make_error_code(err.ec).message()};
}

config_map_t config_map(const std::vector<createable_topic_config>& config);
config_map_t config_map(const std::vector<creatable_topic_configs>& config);

cluster::custom_assignable_topic_configuration
to_cluster_type(const creatable_topic& t);

std::vector<kafka::creatable_topic_configs> report_topic_configs(
  const cluster::metadata_cache& metadata_cache,
  const cluster::topic_properties& topic_properties);

} // namespace kafka
