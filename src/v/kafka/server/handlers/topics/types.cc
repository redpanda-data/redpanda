// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/topics/types.h"

#include "cluster/types.h"
#include "config/configuration.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/timestamp.h"
#include "units.h"
#include "utils/string_switch.h"

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

config_map_t config_map(const std::vector<createable_topic_config>& config) {
    config_map_t ret;
    ret.reserve(config.size());
    for (const auto& c : config) {
        if (c.value) {
            ret.emplace(c.name, *c.value);
        }
    }
    return ret;
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

cluster::topic_configuration to_cluster_type(const creatable_topic& t) {
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

    return cfg;
}

} // namespace kafka
