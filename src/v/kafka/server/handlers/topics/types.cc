// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/topics/types.h"

#include "base/units.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "kafka/server/handlers/configs/config_utils.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/timestamp.h"
#include "pandaproxy/schema_registry/subject_name_strategy.h"
#include "strings/string_switch.h"

#include <seastar/core/sstring.hh>

#include <bits/stdint-intn.h>
#include <bits/stdint-uintn.h>

#include <chrono>
#include <cstddef>
#include <limits>
#include <optional>
#include <ratio>
#include <string_view>
#include <vector>

namespace kafka {

template<typename T>
concept CreatableTopicCfg = std::is_same_v<T, creatable_topic_configs>
                            || std::is_same_v<T, createable_topic_config>;

template<CreatableTopicCfg T>
config_map_t make_config_map(const std::vector<T>& config) {
    config_map_t ret;
    ret.reserve(config.size());
    for (const auto& c : config) {
        if (c.value) {
            ret.emplace(c.name, *c.value);
        }
    }
    return ret;
}

config_map_t config_map(const std::vector<createable_topic_config>& config) {
    return make_config_map(config);
}

config_map_t config_map(const std::vector<creatable_topic_configs>& config) {
    return make_config_map(config);
}

// Either parse configuration or return nullopt
template<typename T>
static std::optional<T>
get_config_value(const config_map_t& config, std::string_view key) {
    if (auto it = config.find(key); it != config.end()) {
        return boost::lexical_cast<T>(it->second);
    }
    return std::nullopt;
}

static std::optional<ss::sstring>
get_string_value(const config_map_t& config, std::string_view key) {
    if (auto it = config.find(key); it != config.end()) {
        return it->second;
    }
    return std::nullopt;
}

// Either parse configuration or return nullopt
static std::optional<bool>
get_bool_value(const config_map_t& config, std::string_view key) {
    if (auto it = config.find(key); it != config.end()) {
        return string_switch<std::optional<bool>>(it->second)
          .match("true", true)
          .match("false", false)
          .default_match(std::nullopt);
    }
    return std::nullopt;
}

static model::shadow_indexing_mode
get_shadow_indexing_mode(const config_map_t& config) {
    auto arch_enabled = get_bool_value(config, topic_property_remote_write);
    auto si_enabled = get_bool_value(config, topic_property_remote_read);

    // If topic properties are missing, patch them with the cluster config.
    if (!arch_enabled) {
        arch_enabled
          = config::shard_local_cfg().cloud_storage_enable_remote_write();
    }

    if (!si_enabled) {
        si_enabled
          = config::shard_local_cfg().cloud_storage_enable_remote_read();
    }

    model::shadow_indexing_mode mode = model::shadow_indexing_mode::disabled;
    if (*arch_enabled) {
        mode = model::shadow_indexing_mode::archival;
    }
    if (*si_enabled) {
        mode = mode == model::shadow_indexing_mode::archival
                 ? model::shadow_indexing_mode::full
                 : model::shadow_indexing_mode::fetch;
    }
    return mode;
}

// Special case for options where Kafka allows -1
// In redpanda the mapping is following
//
// -1 (feature disabled)   =>  tristate.is_disabled() == true;
// no value                =>  tristate.has_value() == false;
// value present           =>  tristate.has_value() == true;

template<typename T>
static tristate<T>
get_tristate_value(const config_map_t& config, std::string_view key) {
    auto v = get_config_value<int64_t>(config, key);
    // no value set
    if (!v) {
        return tristate<T>(std::nullopt);
    }
    // disabled case
    if (v <= 0) {
        return tristate<T>{};
    }
    return tristate<T>(std::make_optional<T>(*v));
}

cluster::custom_assignable_topic_configuration
to_cluster_type(const creatable_topic& t) {
    auto cfg = cluster::topic_configuration(
      model::kafka_namespace, t.name, t.num_partitions, t.replication_factor);

    auto config_entries = config_map(t.configs);
    // Parse topic configuration
    cfg.properties.compression = get_config_value<model::compression>(
      config_entries, topic_property_compression);
    cfg.properties.cleanup_policy_bitflags
      = get_config_value<model::cleanup_policy_bitflags>(
        config_entries, topic_property_cleanup_policy);
    cfg.properties.timestamp_type = get_config_value<model::timestamp_type>(
      config_entries, topic_property_timestamp_type);
    cfg.properties.segment_size = get_config_value<size_t>(
      config_entries, topic_property_segment_size);
    cfg.properties.compaction_strategy
      = get_config_value<model::compaction_strategy>(
        config_entries, topic_property_compaction_strategy);
    cfg.properties.retention_bytes = get_tristate_value<size_t>(
      config_entries, topic_property_retention_bytes);
    cfg.properties.retention_duration
      = get_tristate_value<std::chrono::milliseconds>(
        config_entries, topic_property_retention_duration);
    cfg.properties.recovery = get_bool_value(
      config_entries, topic_property_recovery);
    cfg.properties.shadow_indexing = get_shadow_indexing_mode(config_entries);
    cfg.properties.read_replica_bucket = get_string_value(
      config_entries, topic_property_read_replica);
    cfg.properties.batch_max_bytes = get_config_value<uint32_t>(
      config_entries, topic_property_max_message_bytes);
    if (cfg.properties.read_replica_bucket.has_value()) {
        cfg.properties.read_replica = true;
    }

    cfg.properties.retention_local_target_bytes = get_tristate_value<size_t>(
      config_entries, topic_property_retention_local_target_bytes);
    cfg.properties.retention_local_target_ms
      = get_tristate_value<std::chrono::milliseconds>(
        config_entries, topic_property_retention_local_target_ms);

    cfg.properties.remote_delete
      = get_bool_value(config_entries, topic_property_remote_delete)
          .value_or(storage::ntp_config::default_remote_delete);

    cfg.properties.segment_ms = get_tristate_value<std::chrono::milliseconds>(
      config_entries, topic_property_segment_ms);

    cfg.properties.initial_retention_local_target_bytes
      = get_tristate_value<size_t>(
        config_entries, topic_property_initial_retention_local_target_bytes);
    cfg.properties.initial_retention_local_target_ms
      = get_tristate_value<std::chrono::milliseconds>(
        config_entries, topic_property_initial_retention_local_target_ms);

    cfg.properties.mpx_virtual_cluster_id
      = get_config_value<model::vcluster_id>(
        config_entries, topic_property_mpx_virtual_cluster_id);

    schema_id_validation_config_parser schema_id_validation_config_parser{
      cfg.properties};

    for (auto& p : t.configs) {
        schema_id_validation_config_parser(
          p, kafka::config_resource_operation::set);
    }

    /// Final topic_property not decoded here is \ref remote_topic_properties,
    /// is more of an implementation detail no need to ever show user

    auto ret = cluster::custom_assignable_topic_configuration(cfg);
    /**
     * handle custom assignments
     */
    if (!t.assignments.empty()) {
        /**
         * custom assigned partitions must have the same replication factor
         */
        ret.cfg.partition_count = t.assignments.size();
        ret.cfg.replication_factor = t.assignments.front().broker_ids.size();
        for (auto& assignment : t.assignments) {
            ret.custom_assignments.push_back(
              cluster::custom_partition_assignment{
                .id = assignment.partition_index,
                .replicas = assignment.broker_ids});
        }
    }
    return ret;
}

} // namespace kafka
