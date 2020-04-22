#include "kafka/requests/topics/types.h"

#include "config/configuration.h"
#include "model/compression.h"
#include "model/fundamental.h"
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
new_topic_configuration::config_map() const {
    absl::flat_hash_map<ss::sstring, ss::sstring> ret;
    ret.reserve(config.size());
    for (const auto& c : config) {
        ret.emplace(c.name, c.value);
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
// -1 (feature distabled)  =>  std::nullopt
// no value                =>  default_from_config
// value present           =>  used given value

template<typename T>
static std::optional<T> get_optional_value(
  const absl::flat_hash_map<ss::sstring, ss::sstring>& config,
  const ss::sstring& key,
  std::optional<T> default_from_config) {
    auto v = get_config_value<int64_t>(config, key);
    if (v) {
        return *v <= 0 ? std::nullopt : std::make_optional<T>(*v);
    }
    return default_from_config;
}

cluster::topic_configuration new_topic_configuration::to_cluster_type() const {
    auto config_entries = config_map();

    // Parse topic configuration
    auto compression = get_config_value<model::compression>(
                         config_entries, "compression.type")
                         .value_or(model::compression::none);

    auto cleanup_policy = get_config_value<model::cleanup_policy>(
                            config_entries, "cleanup.policy")
                            .value_or(model::cleanup_policy::del);

    auto timestamp_type = get_config_value<model::timestamp_type>(
                            config_entries, "message.timestamp.type")
                            .value_or(model::timestamp_type::create_time);

    auto segment_size
      = get_config_value<size_t>(config_entries, "segment.bytes")
          .value_or(config::shard_local_cfg().log_segment_size());

    auto compaction_strategy = get_config_value<model::compaction_strategy>(
                                 config_entries, "compaction.strategy")
                                 .value_or(model::compaction_strategy::offset);

    std::optional<size_t> retention_bytes = get_optional_value(
      config_entries,
      "retention.bytes",
      config::shard_local_cfg().retention_bytes());

    std::optional<std::chrono::milliseconds> retention_duration_ms
      = get_optional_value(
        config_entries,
        "retention.ms",
        std::optional(config::shard_local_cfg().delete_retention_ms()));

    return cluster::topic_configuration(
      cluster::kafka_namespace,
      model::topic{ss::sstring(topic())},
      partition_count,
      replication_factor,
      compression,
      cleanup_policy,
      compaction_strategy,
      timestamp_type,
      segment_size,
      retention_bytes,
      retention_duration_ms);
}

} // namespace kafka