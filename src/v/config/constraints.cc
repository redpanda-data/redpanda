// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/constraints.h"

#include "cluster/types.h"
#include "config/base_property.h"
#include "config/configuration.h"
#include "kafka/server/handlers/topics/types.h"
#include "model/fundamental.h"
#include "units.h"
#include "vlog.h"

#include <seastar/util/log.hh>

#include <limits>

namespace config {
inline ss::logger constraints_log{"constraints"};

std::string_view to_string_view(constraint_type type) {
    switch (type) {
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
      .match("restrict", constraint_type::restrikt)
      .match("clamp", constraint_type::clamp);
}

std::ostream& operator<<(std::ostream& os, const constraint_t& constraint) {
    os << ssx::sformat(
      "name: {} type: {}",
      constraint.name,
      config::to_string_view(constraint.type));
    ss::visit(
      constraint.flags,
      [&os](const config::constraint_enabled_t enabled) {
          os << ssx::sformat(" enabled: {}", enabled);
      },
      [&os](const auto range) { os << range; });

    return os;
}

namespace {
template<typename T, typename RangeMinT>
bool valid_min(
  const tristate<T>& topic_val, const std::optional<RangeMinT>& min) {
    if (min) {
        // Disabled state means infinite which is always greater than the
        // minimum
        if (topic_val.is_disabled()) {
            return true;
        }

        // An undefined topic value implicity breaks the minimum because
        // "nothing" is not within any range.
        if (!topic_val.has_optional_value()) {
            return false;
        }

        return topic_val.value() >= static_cast<T>(*min);
    }

    // Topic value is valid if minimum is undefined because there is no bound to
    // compare
    return true;
}

template<typename T, typename RangeMaxT>
bool valid_max(
  const tristate<T>& topic_val, const std::optional<RangeMaxT> max) {
    if (max) {
        // Disabled state means infinite which is always greater than the
        // maximum
        if (topic_val.is_disabled()) {
            return false;
        }

        // An undefined topic value implicity breaks the maximum because
        // "nothing" is not within any range. Not to be confused with disabled
        // state.
        if (!topic_val.has_optional_value()) {
            return false;
        }

        return topic_val.value() <= static_cast<T>(*max);
    }

    // Topic value is valid if maximum is undefined because there is no bound to
    // compare
    return true;
}

template<typename T, typename RangeT>
bool within_range(
  const tristate<T>& topic_val,
  const range_values<RangeT>& range,
  const model::topic& topic,
  const std::string_view& topic_property) {
    if (!(valid_min(topic_val, range.min) && valid_max(topic_val, range.max))) {
        vlog(
          constraints_log.error,
          "Constraints failure[value out-of-range]: topic property {}.{}, "
          "value {}",
          topic(),
          topic_property,
          topic_val);
        return false;
    }

    // Otherwise, the topic value is valid
    return true;
}

template<typename T>
bool matches_cluster_property_value(
  const tristate<T>& topic_val,
  const std::optional<T>& cluster_val,
  const constraint_enabled_t& enabled,
  const model::topic& topic,
  const std::string_view& topic_property,
  const std::string_view& cluster_property) {
    if (enabled == constraint_enabled_t::no) {
        // A constraint that is turned off (i.e., no) means that the broker
        // should not compare the topic property to the cluster one. This
        // implies the the topic property automatically satisfies the
        // constraint.
        return true;
    }

    // A constraint with "enabled" flag means that the topic property must match
    // the cluster one. An undefined topic value or cluster value could not
    // match the other, this implies that the constraint is not satisfied.
    if (!topic_val.has_optional_value() || !cluster_val) {
        return false;
    }

    if (topic_val.value() != *cluster_val) {
        vlog(
          constraints_log.error,
          "Constraints failure[does not match the cluster property {}]: "
          "topic property {}.{}, value {}",
          cluster_property,
          topic(),
          topic_property,
          topic_val);
        return false;
    }

    return true;
}

/**
 * Returns true if the topic-level value satisfies the constraint.
 * \param topic_val: the topic value
 * \param constraint: the constraint to evaluate
 * \param property_name: name of the cluster-level property
 * \param cluster_opt: the value from the cluster-level property
 */
template<typename T>
bool validate_value(
  const tristate<T>& topic_val,
  const constraint_t& constraint,
  const model::topic& topic,
  const std::string_view& topic_property,
  const std::string_view& cluster_property,
  const std::optional<T> cluster_opt = std::nullopt) {
    if (constraint.name == cluster_property) {
        return ss::visit(
          constraint.flags,
          [&topic_val,
           &topic,
           &topic_property,
           &cluster_property,
           &cluster_opt](const constraint_enabled_t enabled) {
              return matches_cluster_property_value(
                topic_val,
                cluster_opt,
                enabled,
                topic,
                topic_property,
                cluster_property);
          },
          [&topic_val, &topic, &topic_property](const auto range) {
              return within_range(topic_val, range, topic, topic_property);
          });
    }
    return false;
}

template<typename T>
bool validate_value(
  const std::optional<T>& topic_opt,
  const constraint_t& constraint,
  const model::topic& topic,
  const std::string_view& topic_property,
  const std::string_view& cluster_property,
  const std::optional<T> cluster_opt = std::nullopt) {
    auto tri = tristate<T>{topic_opt};

    return validate_value(
      tri, constraint, topic, topic_property, cluster_property, cluster_opt);
}
} // namespace

bool topic_config_satisfies_constraint(
  const cluster::topic_configuration& topic_cfg,
  const constraint_t& constraint) {
    auto partition_count_tri = tristate(
      std::make_optional(topic_cfg.partition_count));
    auto replication_factor_tri = tristate(
      std::make_optional(topic_cfg.replication_factor));
    bool ret
      = validate_value(
          partition_count_tri,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_partition_count,
          config::shard_local_cfg().default_topic_partitions.name())
        || validate_value(
          replication_factor_tri,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_replication_factor,
          config::shard_local_cfg().default_topic_replication.name())
        || validate_value<model::compression>(
          topic_cfg.properties.compression,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_compression,
          config::shard_local_cfg().log_compression_type.name(),
          config::shard_local_cfg().log_compression_type())
        || validate_value<model::cleanup_policy_bitflags>(
          topic_cfg.properties.cleanup_policy_bitflags,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_cleanup_policy,
          config::shard_local_cfg().log_cleanup_policy.name(),
          config::shard_local_cfg().log_cleanup_policy())
        || validate_value<model::timestamp_type>(
          topic_cfg.properties.timestamp_type,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_timestamp_type,
          config::shard_local_cfg().log_message_timestamp_type.name(),
          config::shard_local_cfg().log_message_timestamp_type())
        || validate_value(
          topic_cfg.properties.segment_size,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_segment_size,
          config::shard_local_cfg().log_segment_size.name())
        || validate_value(
          topic_cfg.properties.retention_bytes,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_retention_bytes,
          config::shard_local_cfg().retention_bytes.name())
        || validate_value(
          topic_cfg.properties.retention_duration,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_retention_duration,
          config::shard_local_cfg().log_retention_ms.name())
        || validate_value(
          topic_cfg.properties.batch_max_bytes,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_max_message_bytes,
          config::shard_local_cfg().kafka_batch_max_bytes.name())
        || validate_value(
          topic_cfg.properties.retention_local_target_bytes,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_retention_local_target_bytes,
          config::shard_local_cfg().retention_local_target_bytes_default.name())
        || validate_value(
          topic_cfg.properties.retention_local_target_ms,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_retention_local_target_ms,
          config::shard_local_cfg().retention_local_target_ms_default.name())
        || validate_value(
          topic_cfg.properties.segment_ms,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_segment_ms,
          config::shard_local_cfg().log_segment_ms.name());

    if (topic_cfg.properties.shadow_indexing) {
        auto fetch_enabled_opt = std::make_optional(
          model::is_fetch_enabled(*topic_cfg.properties.shadow_indexing));
        auto archival_enabled_opt = std::make_optional(
          model::is_archival_enabled(*topic_cfg.properties.shadow_indexing));
        ret
          = ret
            || validate_value<bool>(
              fetch_enabled_opt,
              constraint,
              topic_cfg.tp_ns.tp,
              kafka::topic_property_remote_read,
              config::shard_local_cfg().cloud_storage_enable_remote_read.name(),
              config::shard_local_cfg().cloud_storage_enable_remote_read())
            || validate_value<bool>(
              archival_enabled_opt,
              constraint,
              topic_cfg.tp_ns.tp,
              kafka::topic_property_remote_write,
              config::shard_local_cfg()
                .cloud_storage_enable_remote_write.name(),
              config::shard_local_cfg().cloud_storage_enable_remote_write());
    }

    return ret;
}

namespace {
template<typename T, typename RangeT>
void range_clamp(
  tristate<T>& topic_val,
  const range_values<RangeT>& range,
  const model::topic& topic,
  const std::string_view& topic_property) {
    if (!valid_min(topic_val, range.min)) {
        vlog(
          constraints_log.warn,
          "Overwriting topic property to constraint min: topic property {}.{}, "
          "min {}",
          topic(),
          topic_property,
          range.min);
        // NOTE: valid_min checks if the minimum opt is defined, so it is OK to
        // de-reference it here.
        topic_val = tristate<T>{static_cast<T>(*range.min)};
    }

    if (!valid_max(topic_val, range.max)) {
        vlog(
          constraints_log.warn,
          "Overwriting topic property to constraint max: topic property {}.{}, "
          "max {}",
          topic(),
          topic_property,
          range.max);
        // NOTE: valid_max checks if the maximum opt is defined, so it is OK to
        // de-reference it here.
        topic_val = tristate<T>{static_cast<T>(*range.max)};
    }
}

template<typename T>
void cluster_property_clamp(
  tristate<T>& topic_val,
  const std::optional<T>& cluster_val,
  const constraint_enabled_t& enabled,
  const model::topic& topic,
  const std::string_view& topic_property,
  const std::string_view& cluster_property) {
    if (enabled == constraint_enabled_t::no) {
        // Since the constraint is turned off, there is no need to clamp
        return;
    }

    if (!topic_val.has_optional_value() || !cluster_val) {
        return;
    }

    vlog(
      constraints_log.warn,
      "Overwriting topic property to the cluster property {}: topic property "
      "{}.{}, "
      "value {}",
      cluster_property,
      topic(),
      topic_property,
      topic_val);
    topic_val = tristate<T>{cluster_val};
}

/**
 * Assigns the constraint range or cluster-level value to the topic-level value
 * \param topic_val: the topic value
 * \param constraint: the constraint to evaluate
 * \param property_name: name of the cluster-level property
 * \param cluster_opt: the value from the cluster-level property
 */
template<typename T>
void clamp_value(
  tristate<T>& topic_val,
  const constraint_t& constraint,
  const model::topic& topic,
  const std::string_view& topic_property,
  const std::string_view& cluster_property,
  const std::optional<T> cluster_opt = std::nullopt) {
    if (constraint.name == cluster_property) {
        ss::visit(
          constraint.flags,
          [&topic_val,
           &topic,
           &topic_property,
           &cluster_property,
           &cluster_opt](const constraint_enabled_t enabled) {
              cluster_property_clamp(
                topic_val,
                cluster_opt,
                enabled,
                topic,
                topic_property,
                cluster_property);
          },
          [&topic_val, &topic, &topic_property](const auto range) {
              range_clamp(topic_val, range, topic, topic_property);
          });
    }
}

template<typename T>
void clamp_value(
  std::optional<T>& topic_opt,
  const constraint_t& constraint,
  const model::topic& topic,
  const std::string_view& topic_property,
  const std::string_view& cluster_property,
  const std::optional<T> cluster_opt = std::nullopt) {
    auto tri = tristate<T>{topic_opt};
    clamp_value(
      tri, constraint, topic, topic_property, cluster_property, cluster_opt);
    // The tristate is not in disabled state since it was assigned an optional
    // earlier.
    topic_opt = tri.get_optional();
}
} // namespace

void constraint_clamp_topic_config(
  cluster::topic_configuration& topic_cfg, const constraint_t& constraint) {
    auto partition_count_tri = tristate(
      std::make_optional(topic_cfg.partition_count));
    auto replication_factor_tri = tristate(
      std::make_optional(topic_cfg.replication_factor));
    clamp_value(
      partition_count_tri,
      constraint,
      topic_cfg.tp_ns.tp,
      kafka::topic_property_partition_count,
      config::shard_local_cfg().default_topic_partitions.name());
    topic_cfg.partition_count = partition_count_tri.value();
    clamp_value(
      replication_factor_tri,
      constraint,
      topic_cfg.tp_ns.tp,
      kafka::topic_property_replication_factor,
      config::shard_local_cfg().default_topic_replication.name());
    topic_cfg.replication_factor = replication_factor_tri.value();
    clamp_value<model::compression>(
      topic_cfg.properties.compression,
      constraint,
      topic_cfg.tp_ns.tp,
      kafka::topic_property_compression,
      config::shard_local_cfg().log_compression_type.name(),
      config::shard_local_cfg().log_compression_type());
    clamp_value<model::cleanup_policy_bitflags>(
      topic_cfg.properties.cleanup_policy_bitflags,
      constraint,
      topic_cfg.tp_ns.tp,
      kafka::topic_property_cleanup_policy,
      config::shard_local_cfg().log_cleanup_policy.name(),
      config::shard_local_cfg().log_cleanup_policy());
    clamp_value<model::timestamp_type>(
      topic_cfg.properties.timestamp_type,
      constraint,
      topic_cfg.tp_ns.tp,
      kafka::topic_property_timestamp_type,
      config::shard_local_cfg().log_message_timestamp_type.name(),
      config::shard_local_cfg().log_message_timestamp_type());
    clamp_value(
      topic_cfg.properties.segment_size,
      constraint,
      topic_cfg.tp_ns.tp,
      kafka::topic_property_segment_size,
      config::shard_local_cfg().log_segment_size.name());
    clamp_value(
      topic_cfg.properties.retention_bytes,
      constraint,
      topic_cfg.tp_ns.tp,
      kafka::topic_property_retention_bytes,
      config::shard_local_cfg().retention_bytes.name());
    clamp_value(
      topic_cfg.properties.retention_duration,
      constraint,
      topic_cfg.tp_ns.tp,
      kafka::topic_property_retention_duration,
      config::shard_local_cfg().log_retention_ms.name());
    clamp_value(
      topic_cfg.properties.batch_max_bytes,
      constraint,
      topic_cfg.tp_ns.tp,
      kafka::topic_property_max_message_bytes,
      config::shard_local_cfg().kafka_batch_max_bytes.name());
    clamp_value(
      topic_cfg.properties.retention_local_target_bytes,
      constraint,
      topic_cfg.tp_ns.tp,
      kafka::topic_property_retention_local_target_bytes,
      config::shard_local_cfg().retention_local_target_bytes_default.name());
    clamp_value(
      topic_cfg.properties.retention_local_target_ms,
      constraint,
      topic_cfg.tp_ns.tp,
      kafka::topic_property_retention_local_target_ms,
      config::shard_local_cfg().retention_local_target_ms_default.name());
    clamp_value(
      topic_cfg.properties.segment_ms,
      constraint,
      topic_cfg.tp_ns.tp,
      kafka::topic_property_segment_ms,
      config::shard_local_cfg().log_segment_ms.name());

    if (topic_cfg.properties.shadow_indexing) {
        auto fetch_enabled_opt = std::make_optional(
          model::is_fetch_enabled(*topic_cfg.properties.shadow_indexing));
        auto archival_enabled_opt = std::make_optional(
          model::is_archival_enabled(*topic_cfg.properties.shadow_indexing));
        clamp_value<bool>(
          fetch_enabled_opt,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_remote_read,
          config::shard_local_cfg().cloud_storage_enable_remote_read.name(),
          config::shard_local_cfg().cloud_storage_enable_remote_read());
        clamp_value<bool>(
          archival_enabled_opt,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_remote_write,
          config::shard_local_cfg().cloud_storage_enable_remote_write.name(),
          config::shard_local_cfg().cloud_storage_enable_remote_write());
        topic_cfg.properties.shadow_indexing
          = model::get_shadow_indexing_mode_impl(
            *archival_enabled_opt, *fetch_enabled_opt);
    }
}

std::vector<std::string_view> constraint_supported_properties() {
    std::vector<std::string_view> names;
    names.push_back(config::shard_local_cfg().default_topic_partitions.name());
    names.push_back(config::shard_local_cfg().default_topic_replication.name());
    names.push_back(config::shard_local_cfg().log_compression_type.name());
    names.push_back(config::shard_local_cfg().log_cleanup_policy.name());
    names.push_back(
      config::shard_local_cfg().log_message_timestamp_type.name());
    names.push_back(config::shard_local_cfg().log_segment_size.name());
    names.push_back(config::shard_local_cfg().retention_bytes.name());
    names.push_back(config::shard_local_cfg().log_retention_ms.name());
    names.push_back(
      config::shard_local_cfg().cloud_storage_enable_remote_read.name());
    names.push_back(
      config::shard_local_cfg().cloud_storage_enable_remote_write.name());
    names.push_back(config::shard_local_cfg().kafka_batch_max_bytes.name());
    names.push_back(
      config::shard_local_cfg().retention_local_target_bytes_default.name());
    names.push_back(
      config::shard_local_cfg().retention_local_target_ms_default.name());
    names.push_back(config::shard_local_cfg().log_segment_ms.name());

    return names;
}

std::optional<constraint_t> get_constraint(const constraint_t::key_type name) {
    const auto& constraints = config::shard_local_cfg().constraints();
    if (auto found = constraints.find(name); found != constraints.end()) {
        return std::make_optional(found->second);
    }

    return std::nullopt;
}
} // namespace config

namespace YAML {

template<typename T>
void encode_range(Node& node, config::range_values<T>& range) {
    if (range.min) {
        node["min"] = *range.min;
    }
    if (range.max) {
        node["max"] = *range.max;
    }
}

Node convert<config::constraint_t>::encode(const type& rhs) {
    Node node;
    node["name"] = rhs.name;
    node["type"] = ss::sstring(config::to_string_view(rhs.type));

    ss::visit(
      rhs.flags,
      [&node](config::constraint_enabled_t enabled) {
          node["enabled"] = enabled == config::constraint_enabled_t::yes;
      },
      [&node](auto range) { encode_range(node, range); });

    return node;
}

template<typename T>
config::range_values<T> decode_range(const Node& node) {
    config::range_values<T> range;
    if (node["min"] && !node["min"].IsNull()) {
        range.min = node["min"].as<T>();
    }

    if (node["max"] && !node["max"].IsNull()) {
        range.max = node["max"].as<T>();
    }
    return range;
}

template<typename T>
bool maybe_decode_range(const Node& node, config::constraint_t& constraint) {
    auto range = decode_range<T>(node);
    if (!range.min && !range.max) {
        return false;
    }

    constraint.flags = range;
    return true;
}

// Used to run a condition based on the type of the property.
template<typename SignedFunc, typename UnsignedFunc, typename ElseFunc>
auto ternary_property_op(
  const config::base_property& property,
  SignedFunc&& signed_condition,
  UnsignedFunc&& unsigned_condition,
  ElseFunc&& else_condition) {
    if (property.is_signed() || property.is_milliseconds()) {
        return signed_condition();
    } else if (property.is_unsigned() && !property.is_bool()) {
        return unsigned_condition();
    } else {
        return else_condition();
    }
}

bool convert<config::constraint_t>::decode(const Node& node, type& rhs) {
    for (const auto& s : {"name", "type"}) {
        if (!node[s]) {
            return false;
        }
    }

    rhs.name = node["name"].as<ss::sstring>();

    auto type_opt = config::from_string_view<config::constraint_type>(
      node["type"].as<ss::sstring>());
    if (type_opt) {
        rhs.type = *type_opt;
    } else {
        return false;
    }

    // Here, we decode constraint flags based on the property type
    return ternary_property_op(
      config::shard_local_cfg().get(rhs.name),
      [&node, &rhs] { return maybe_decode_range<int64_t>(node, rhs); },
      [&node, &rhs] { return maybe_decode_range<uint64_t>(node, rhs); },
      [&node, &rhs] {
          // For "enabled" contraints
          if (node["enabled"] && !node["enabled"].IsNull()) {
              rhs.flags = config::constraint_enabled_t(
                node["enabled"].as<bool>());
              return true;
          }

          return false;
      });
}
} // namespace YAML

namespace json {

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::constraint_enabled_t& ep) {
    w.Bool(bool(ep));
}

template<typename T>
void rjson_serialize_range(
  json::Writer<json::StringBuffer>& w, config::range_values<T>& range) {
    if (range.min) {
        w.Key("min");
        rjson_serialize(w, range.min);
    }
    if (range.max) {
        w.Key("max");
        rjson_serialize(w, range.max);
    }
}

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::constraint_t& constraint) {
    w.StartObject();
    w.Key("name");
    w.String(constraint.name);
    w.Key("type");
    w.String(ss::sstring(config::to_string_view(constraint.type)));
    ss::visit(
      constraint.flags,
      [&w](config::constraint_enabled_t enabled) {
          w.Key("enabled");
          rjson_serialize(w, enabled);
      },
      [&w](auto range) { rjson_serialize_range(w, range); });
    w.EndObject();
}

} // namespace json
