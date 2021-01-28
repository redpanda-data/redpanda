// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/topics/types.h"

#include "config/configuration.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/namespace.h"
#include "model/timestamp.h"
#include "units.h"

#include <absl/container/flat_hash_map.h>
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

absl::flat_hash_map<ss::sstring, ss::sstring>
config_map(const std::vector<createable_topic_config>& config) {
    absl::flat_hash_map<ss::sstring, ss::sstring> ret;
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
static std::optional<T> get_config_value(
  const absl::flat_hash_map<ss::sstring, ss::sstring>& config,
  const ss::sstring& key) {
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
static tristate<T> get_tristate_value(
  const absl::flat_hash_map<ss::sstring, ss::sstring>& config,
  const ss::sstring& key) {
    auto v = get_config_value<int64_t>(config, key);
    // diabled case
    if (v && *v <= 0) {
        return tristate<T>{};
    }
    return tristate<T>(std::make_optional<T>(*v));
}

cluster::topic_configuration to_cluster_type(const creatable_topic& t) {
    auto cfg = cluster::topic_configuration(
      model::kafka_namespace, t.name, t.num_partitions, t.replication_factor);

    auto config_entries = config_map(t.configs);
    // Parse topic configuration
    cfg.compression = get_config_value<model::compression>(
      config_entries, "compression.type");
    cfg.cleanup_policy_bitflags
      = get_config_value<model::cleanup_policy_bitflags>(
        config_entries, "cleanup.policy");
    cfg.timestamp_type = get_config_value<model::timestamp_type>(
      config_entries, "message.timestamp.type");
    cfg.segment_size = get_config_value<size_t>(
      config_entries, "segment.bytes");
    cfg.compaction_strategy = get_config_value<model::compaction_strategy>(
      config_entries, "compaction.strategy");
    cfg.retention_bytes = get_tristate_value<size_t>(
      config_entries, "retention.bytes");
    cfg.retention_duration = get_tristate_value<std::chrono::milliseconds>(
      config_entries, "retention.ms");

    return cfg;
}

} // namespace kafka
