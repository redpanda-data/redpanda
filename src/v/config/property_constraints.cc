// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/property_constraints.h"

#include "cluster/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "vassert.h"

#include <chrono>
#include <stdexcept>
#include <string>

namespace config {

std::string_view to_string_view(constraint_type type) {
    switch (type) {
    case constraint_type::none:
        return "none";
    case constraint_type::restrikt:
        return "restrict";
    case constraint_type::clamp:
        return "clamp";
    }
}

template<>
std::optional<constraint_type>
from_string_view<constraint_type>(std::string_view sv) {
    return string_switch<std::optional<constraint_type>>(sv)
      .match("none", constraint_type::none)
      .match("restrict", constraint_type::restrikt)
      .match("clamp", constraint_type::clamp);
}

namespace {
template<typename T>
struct range_values {
    std::optional<T> min;
    std::optional<T> max;

    range_values(
      std::optional<ss::sstring>& min_str,
      std::optional<ss::sstring>& max_str,
      ss::noncopyable_function<T(ss::sstring&)> type_converter)
      : min{min_str ? std::make_optional(type_converter(*min_str)) : std::nullopt}
      , max{
          max_str ? std::make_optional(type_converter(*max_str))
                  : std::nullopt} {}
};

model::shadow_indexing_mode drop_fetch(model::shadow_indexing_mode mode) {
    if (mode == model::shadow_indexing_mode::full) {
        return model::shadow_indexing_mode::archival;
    } else if (mode == model::shadow_indexing_mode::fetch) {
        return model::shadow_indexing_mode::disabled;
    } else {
        return mode;
    }
}

model::shadow_indexing_mode drop_archival(model::shadow_indexing_mode mode) {
    if (mode == model::shadow_indexing_mode::full) {
        return model::shadow_indexing_mode::fetch;
    } else if (mode == model::shadow_indexing_mode::archival) {
        return model::shadow_indexing_mode::disabled;
    } else {
        return mode;
    }
}
} // namespace

// This is where all the type conversions happen
ss::shared_ptr<constraint_validator_t> constraint_validator_map(
  ss::sstring& name,
  std::optional<ss::sstring> min_str_opt,
  std::optional<ss::sstring> max_str_opt,
  constraint_enabled_t enabled) {
    auto& cfg = config::shard_local_cfg();

    if (name == "log_cleanup_policy") {
        return ss::make_shared<
          constraint_validator_enabled<model::cleanup_policy_bitflags>>(
          std::move(enabled),
          cfg.log_cleanup_policy.bind(),
          [](const cluster::topic_configuration& topic_cfg) {
              return topic_cfg.properties.cleanup_policy_bitflags.value_or(
                model::cleanup_policy_bitflags::none);
          },
          [](
            cluster::topic_configuration& topic_cfg,
            const model::cleanup_policy_bitflags& topic_val) {
              topic_cfg.properties.cleanup_policy_bitflags = topic_val;
          });
    } else if (name == "log_retention_ms") {
        auto range = range_values<std::chrono::milliseconds>(
          min_str_opt, max_str_opt, [](ss::sstring& str_val) {
              return std::chrono::milliseconds{std::stol(str_val)};
          });
        vassert(
          cfg.log_retention_ms.default_value().has_value(),
          "{} does not have a default value",
          cfg.log_retention_ms.name());
        auto def_val = *cfg.log_retention_ms.default_value();
        return ss::make_shared<
          constraint_validator_range<std::chrono::milliseconds>>(
          std::move(range.min),
          std::move(range.max),
          [def_val{std::move(def_val)}](
            const cluster::topic_configuration& topic_cfg) {
              return topic_cfg.properties.retention_duration
                         .has_optional_value()
                       ? topic_cfg.properties.retention_duration.value()
                       : def_val;
          },
          [](
            cluster::topic_configuration& topic_cfg,
            const std::chrono::milliseconds& topic_val) {
              topic_cfg.properties.retention_duration
                = tristate<std::chrono::milliseconds>(topic_val);
          });
    } else if (name == "log_segment_ms") {
        auto range = range_values<std::chrono::milliseconds>(
          min_str_opt, max_str_opt, [](ss::sstring& str_val) {
              return std::chrono::milliseconds{std::stol(str_val)};
          });
        vassert(
          cfg.log_segment_ms.default_value().has_value(),
          "{} does not have a default value",
          cfg.log_segment_ms.name());
        auto def_val = *cfg.log_segment_ms.default_value();
        return ss::make_shared<
          constraint_validator_range<std::chrono::milliseconds>>(
          std::move(range.min),
          std::move(range.max),
          [def_val{std::move(def_val)}](
            const cluster::topic_configuration& topic_cfg) {
              return topic_cfg.properties.segment_ms.has_optional_value()
                       ? topic_cfg.properties.segment_ms.value()
                       : def_val;
          },
          [](
            cluster::topic_configuration& topic_cfg,
            const std::chrono::milliseconds& topic_val) {
              topic_cfg.properties.segment_ms
                = tristate<std::chrono::milliseconds>(topic_val);
          });
    } else if (name == "log_segment_size") {
        auto range = range_values<uint64_t>(
          min_str_opt, max_str_opt, [](ss::sstring& str_val) {
              return uint64_t{std::stoul(str_val)};
          });
        auto def_val = cfg.log_segment_size.default_value();
        return ss::make_shared<constraint_validator_range<uint64_t>>(
          std::move(range.min),
          std::move(range.max),
          [def_val{std::move(def_val)}](
            const cluster::topic_configuration& topic_cfg) {
              return topic_cfg.properties.segment_size.value_or(def_val);
          },
          [](
            cluster::topic_configuration& topic_cfg,
            const uint64_t& topic_val) {
              topic_cfg.properties.segment_size = topic_val;
          });
    } else if (name == "retention_bytes") {
        auto range = range_values<size_t>(
          min_str_opt, max_str_opt, [](ss::sstring& str_val) {
              return size_t{std::stoul(str_val)};
          });
        return ss::make_shared<constraint_validator_range<size_t>>(
          std::move(range.min),
          std::move(range.max),
          [](const cluster::topic_configuration& topic_cfg) {
              return topic_cfg.properties.retention_bytes.has_optional_value()
                       ? topic_cfg.properties.retention_bytes.value()
                       : 0;
          },
          [](cluster::topic_configuration& topic_cfg, const size_t& topic_val) {
              topic_cfg.properties.retention_bytes = tristate<size_t>(
                topic_val);
          });
    } else if (name == "log_compression_type") {
        return ss::make_shared<
          constraint_validator_enabled<model::compression>>(
          std::move(enabled),
          cfg.log_compression_type.bind(),
          [](const cluster::topic_configuration& topic_cfg) {
              return topic_cfg.properties.compression.value_or(
                model::compression::none);
          },
          [](
            cluster::topic_configuration& topic_cfg,
            const model::compression& topic_val) {
              topic_cfg.properties.compression = topic_val;
          });
    } else if (name == "kafka_batch_max_bytes") {
        auto range = range_values<uint32_t>(
          min_str_opt, max_str_opt, [](ss::sstring& str_val) {
              return static_cast<uint32_t>(std::stoul(str_val));
          });
        auto def_val = cfg.kafka_batch_max_bytes.default_value();
        return ss::make_shared<constraint_validator_range<uint32_t>>(
          std::move(range.min),
          std::move(range.max),
          [def_val{std::move(def_val)}](
            const cluster::topic_configuration& topic_cfg) {
              return topic_cfg.properties.batch_max_bytes.value_or(def_val);
          },
          [](
            cluster::topic_configuration& topic_cfg,
            const uint32_t& topic_val) {
              topic_cfg.properties.batch_max_bytes = topic_val;
          });
    } else if (name == "log_message_timestamp_type") {
        auto def_val = cfg.log_message_timestamp_type.default_value();
        return ss::make_shared<
          constraint_validator_enabled<model::timestamp_type>>(
          std::move(enabled),
          cfg.log_message_timestamp_type.bind(),
          [def_val{std::move(def_val)}](
            const cluster::topic_configuration& topic_cfg) {
              return topic_cfg.properties.timestamp_type.value_or(def_val);
          },
          [](
            cluster::topic_configuration& topic_cfg,
            const model::timestamp_type& topic_val) {
              topic_cfg.properties.timestamp_type = topic_val;
          });
    } else if (name == "cloud_storage_enable_remote_read") {
        auto enable_remote_write
          = cfg.cloud_storage_enable_remote_write.default_value();
        return ss::make_shared<constraint_validator_enabled<bool>>(
          std::move(enabled),
          cfg.cloud_storage_enable_remote_read.bind(),
          [](const cluster::topic_configuration& topic_cfg) {
              return topic_cfg.properties.shadow_indexing
                       ? model::is_fetch_enabled(
                         *topic_cfg.properties.shadow_indexing)
                       : false;
          },
          [enable_remote_write](
            cluster::topic_configuration& topic_cfg,
            const bool& fetch_enabled) {
              if (fetch_enabled) {
                  if (topic_cfg.properties.shadow_indexing) {
                      topic_cfg.properties.shadow_indexing
                        = model::add_shadow_indexing_flag(
                          *topic_cfg.properties.shadow_indexing,
                          model::shadow_indexing_mode::fetch);
                  } else {
                      topic_cfg.properties.shadow_indexing
                        = enable_remote_write
                            ? model::shadow_indexing_mode::full
                            : model::shadow_indexing_mode::fetch;
                  }
              } else {
                  if (topic_cfg.properties.shadow_indexing) {
                      topic_cfg.properties.shadow_indexing = drop_fetch(
                        *topic_cfg.properties.shadow_indexing);
                  } else {
                      topic_cfg.properties.shadow_indexing
                        = enable_remote_write
                            ? model::shadow_indexing_mode::archival
                            : model::shadow_indexing_mode::disabled;
                  }
              }
          });
    } else if (name == "cloud_storage_enable_remote_write") {
        auto enable_remote_read
          = cfg.cloud_storage_enable_remote_read.default_value();
        return ss::make_shared<constraint_validator_enabled<bool>>(
          std::move(enabled),
          cfg.cloud_storage_enable_remote_write.bind(),
          [](const cluster::topic_configuration& topic_cfg) {
              return topic_cfg.properties.shadow_indexing
                       ? model::is_archival_enabled(
                         *topic_cfg.properties.shadow_indexing)
                       : false;
          },
          [enable_remote_read](
            cluster::topic_configuration& topic_cfg,
            const bool& archival_enabled) {
              if (archival_enabled) {
                  if (topic_cfg.properties.shadow_indexing) {
                      topic_cfg.properties.shadow_indexing
                        = model::add_shadow_indexing_flag(
                          *topic_cfg.properties.shadow_indexing,
                          model::shadow_indexing_mode::archival);
                  } else {
                      topic_cfg.properties.shadow_indexing
                        = enable_remote_read
                            ? model::shadow_indexing_mode::full
                            : model::shadow_indexing_mode::archival;
                  }
              } else {
                  if (topic_cfg.properties.shadow_indexing) {
                      topic_cfg.properties.shadow_indexing = drop_archival(
                        *topic_cfg.properties.shadow_indexing);
                  } else {
                      topic_cfg.properties.shadow_indexing
                        = enable_remote_read
                            ? model::shadow_indexing_mode::fetch
                            : model::shadow_indexing_mode::disabled;
                  }
              }
          });
    } else if (name == "retention_local_target_bytes_default") {
        auto range = range_values<size_t>(
          min_str_opt, max_str_opt, [](ss::sstring& str_val) {
              return uint64_t{std::stoul(str_val)};
          });
        return ss::make_shared<constraint_validator_range<uint64_t>>(
          std::move(range.min),
          std::move(range.max),
          [](const cluster::topic_configuration& topic_cfg) {
              return topic_cfg.properties.retention_local_target_bytes
                         .has_optional_value()
                       ? topic_cfg.properties.retention_local_target_bytes
                           .value()
                       : 0;
          },
          [](
            cluster::topic_configuration& topic_cfg,
            const uint64_t& topic_val) {
              topic_cfg.properties.retention_local_target_bytes
                = tristate<uint64_t>(topic_val);
          });
    } else if (name == "retention_local_target_ms_default") {
        auto range = range_values<std::chrono::milliseconds>(
          min_str_opt, max_str_opt, [](ss::sstring& str_val) {
              return std::chrono::milliseconds{std::stol(str_val)};
          });
        auto def_val = cfg.retention_local_target_ms_default.default_value();
        return ss::make_shared<
          constraint_validator_range<std::chrono::milliseconds>>(
          std::move(range.min),
          std::move(range.max),
          [def_val{std::move(def_val)}](
            const cluster::topic_configuration& topic_cfg) {
              return topic_cfg.properties.retention_local_target_ms
                         .has_optional_value()
                       ? topic_cfg.properties.retention_local_target_ms.value()
                       : def_val;
          },
          [](
            cluster::topic_configuration& topic_cfg,
            const std::chrono::milliseconds& topic_val) {
              topic_cfg.properties.retention_local_target_ms
                = tristate<std::chrono::milliseconds>(topic_val);
          });
    } else if (name == "default_topic_replications") {
        auto range = range_values<int16_t>(
          min_str_opt, max_str_opt, [](ss::sstring& str_val) {
              return static_cast<int16_t>(std::stoi(str_val));
          });
        return ss::make_shared<constraint_validator_range<int16_t>>(
          std::move(range.min),
          std::move(range.max),
          [](const cluster::topic_configuration& topic_cfg) {
              return topic_cfg.replication_factor;
          },
          [](
            cluster::topic_configuration& topic_cfg, const int16_t& topic_val) {
              topic_cfg.replication_factor = topic_val;
          });
    } else if (name == "default_topic_partitions") {
        auto range = range_values<int32_t>(
          min_str_opt, max_str_opt, [](ss::sstring& str_val) {
              return int32_t{std::stoi(str_val)};
          });
        return ss::make_shared<constraint_validator_range<int32_t>>(
          std::move(range.min),
          std::move(range.max),
          [](const cluster::topic_configuration& topic_cfg) {
              return topic_cfg.partition_count;
          },
          [](
            cluster::topic_configuration& topic_cfg, const int32_t& topic_val) {
              topic_cfg.partition_count = topic_val;
          });
    } else {
        return nullptr;
    }
}

constraint_t::constraint_t(
  ss::sstring name,
  constraint_type type,
  ss::shared_ptr<constraint_validator_t> validator)
  : name{name}
  , type{type}
  , validator{validator} {}

bool constraint_t::validate(
  const cluster::topic_configuration& topic_cfg) const {
    if (validator != nullptr) {
        return validator->validate(topic_cfg);
    }
    return true;
}

void constraint_t::clamp(cluster::topic_configuration& topic_cfg) const {
    if (validator != nullptr) {
        validator->clamp(topic_cfg);
    }
}

std::optional<ss::sstring> constraint_t::check() const {
    // TODO(@NyaliaLui): Create a predefined map of supported constraints
    // with their correct type mappings. Then check if the given name is
    // in the map and return the necessary error message.

    if (validator != nullptr) {
        return validator->check(name);
    }

    return std::nullopt;
}

std::vector<constraint_t> predefined_constraints() {
    std::vector<constraint_t> constraints;
    constraints.reserve(2);

    ss::sstring segment_size_name = "log_segment_size";
    ss::sstring segment_ms_name = "log_segment_ms";

    constraints.emplace_back(
      segment_size_name,
      constraint_type::clamp,
      constraint_validator_map(
        segment_size_name, "1048576", std::nullopt, constraint_enabled_t::no));
    constraints.emplace_back(
      segment_ms_name,
      constraint_type::clamp,
      constraint_validator_map(
        segment_ms_name, "60000", "31536000000", constraint_enabled_t::no));

    return constraints;
}

std::ostream& operator<<(std::ostream& os, const constraint_t& constraint) {
    os << fmt::format(
      "name: {}, type: {}", constraint.name, to_string_view(constraint.type));
    if (constraint.validator != nullptr) {
        constraint.validator->print(os);
    }
    return os;
}

} // namespace config

namespace YAML {

Node convert<config::constraint_t>::encode(const type& rhs) {
    Node node;
    node["name"] = rhs.name;
    node["type"] = ss::sstring(config::to_string_view(rhs.type));
    if (rhs.validator != nullptr) {
        rhs.validator->encode_yaml(node);
    }
    return node;
}

bool convert<config::constraint_t>::decode(const Node& node, type& rhs) {
    for (const auto& s : {"name", "type"}) {
        if (!node[s]) {
            return false;
        }
    }

    rhs.name = node["name"].as<ss::sstring>();
    rhs.type = config::from_string_view<config::constraint_type>(
                 node["type"].as<ss::sstring>())
                 .value();

    std::optional<ss::sstring> min_str_opt{std::nullopt},
      max_str_opt{std::nullopt};
    if (node["min"] && !node["min"].IsNull()) {
        min_str_opt = node["min"].as<ss::sstring>();
    }

    if (node["max"] && !node["max"].IsNull()) {
        max_str_opt = node["max"].as<ss::sstring>();
    }

    config::constraint_enabled_t enabled{config::constraint_enabled_t::no};
    if (node["enabled"] && node["enabled"].as<bool>()) {
        enabled = config::constraint_enabled_t::yes;
    }

    auto validator = constraint_validator_map(
      rhs.name,
      std::move(min_str_opt),
      std::move(max_str_opt),
      std::move(enabled));

    // A null validator means Redpanda does not support constraints for that
    // config name
    if (validator == nullptr) {
        return false;
    }

    rhs.validator = std::move(validator);
    return true;
}

} // namespace YAML

namespace json {

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::constraint_t& constraint) {
    w.StartObject();
    w.Key("name");
    w.String(constraint.name);
    w.Key("type");
    auto type_str = ss::sstring(config::to_string_view(constraint.type));
    w.String(type_str);
    if (constraint.validator != nullptr) {
        constraint.validator->to_json(w);
    }
    w.EndObject();
}

} // namespace json
