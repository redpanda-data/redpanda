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
#include "absl/container/flat_hash_map.h"
#include "cluster/types.h"
#include "container/chunked_vector.h"
#include "kafka/protocol/schemata/create_topics_request.h"
#include "kafka/protocol/schemata/create_topics_response.h"
#include "kafka/protocol/topic_properties.h"
#include "kafka/server/errors.h"
#include "kafka/server/handlers/configs/config_response_utils.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "utils/absl_sstring_hash.h"

#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>

#include <array>
#include <type_traits>

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
inline constexpr std::string_view topic_property_segment_size = "segment.bytes";

inline constexpr std::string_view topic_property_remote_write
  = "redpanda.remote.write";
inline constexpr std::string_view topic_property_remote_read
  = "redpanda.remote.read";

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

inline constexpr std::string_view topic_property_iceberg_mode
  = "redpanda.iceberg.mode";

inline constexpr std::string_view topic_property_iceberg_delete
  = "redpanda.iceberg.delete";

inline constexpr std::string_view topic_property_iceberg_partition_spec
  = "redpanda.iceberg.partition.spec";

inline constexpr std::string_view topic_property_iceberg_invalid_record_action
  = "redpanda.iceberg.invalid.record.action";

inline constexpr std::string_view topic_property_iceberg_target_lag_ms
  = "redpanda.iceberg.target.lag.ms";

inline constexpr std::string_view topic_property_min_cleanable_dirty_ratio
  = "min.cleanable.dirty.ratio";

inline constexpr std::string_view topic_property_message_timestamp_before_max_ms
  = "message.timestamp.before.max.ms";

inline constexpr std::string_view topic_property_message_timestamp_after_max_ms
  = "message.timestamp.after.max.ms";

// Kafka topic properties that is not relevant for Redpanda
// Or cannot be altered with kafka alter handler
inline constexpr std::array<std::string_view, 20> allowlist_topic_noop_confs = {

  // Not used in Redpanda
  "unclean.leader.election.enable",
  "message.downconversion.enable",
  "segment.index.bytes",
  "segment.jitter.ms",
  "min.insync.replicas",
  "message.timestamp.difference.max.ms",
  "message.format.version",
  "leader.replication.throttled.replicas",
  "index.interval.bytes",
  "follower.replication.throttled.replicas",
  "flush.messages",
  "file.delete.delay.ms",
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
      .error_message = err.error_message.value_or(
        cluster::make_error_code(err.ec).message())};
}

config_map_t config_map(const std::vector<createable_topic_config>& config);
config_map_t config_map(const std::vector<creatable_topic_configs>& config);

cluster::custom_assignable_topic_configuration
to_cluster_type(const creatable_topic& t);

cluster::topic_configuration to_topic_config(
  model::ns ns,
  model::topic topic,
  int32_t partition_count,
  int16_t replication_factor,
  const config_map_t& config_map);

std::vector<kafka::creatable_topic_configs> report_topic_configs(
  const cluster::metadata_cache& metadata_cache,
  const cluster::topic_properties& topic_properties);

// Either parse configuration or return nullopt
template<typename T>
std::optional<T>
get_config_value(const config_map_t& config, std::string_view key) {
    if (auto it = config.find(key); it != config.end()) {
        return boost::lexical_cast<T>(it->second);
    }
    return std::nullopt;
}

// Special case for options where Kafka allows -1
// In redpanda the mapping is following
//
// -1 (feature disabled)   =>  tristate.is_disabled() == true;
// no value                =>  tristate.has_value() == false;
// value present           =>  tristate.has_value() == true;

template<typename T>
tristate<T>
get_tristate_value(const config_map_t& config, std::string_view key) {
    using config_t
      = std::conditional_t<std::is_floating_point_v<T>, T, int64_t>;
    auto v = get_config_value<config_t>(config, key);
    // no value set
    if (!v) {
        return tristate<T>(std::nullopt);
    }
    // disabled case
    if (v < 0) {
        return tristate<T>(disable_tristate);
    }
    return tristate<T>(std::make_optional<T>(*v));
}

std::optional<bool>
get_bool_value(const config_map_t& config, std::string_view key);

model::shadow_indexing_mode
get_shadow_indexing_mode(const config_map_t& config);

} // namespace kafka
