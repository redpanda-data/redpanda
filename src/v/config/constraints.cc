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
bool is_retention_property(const std::string_view& topic_property) {
    return topic_property == kafka::topic_property_retention_duration
           || topic_property == kafka::topic_property_retention_local_target_ms
           || topic_property == kafka::topic_property_retention_bytes
           || topic_property
                == kafka::topic_property_retention_local_target_bytes;
}

template<typename T>
bool is_infinite(
  const tristate<T>& tri, const std::string_view& topic_property) {
    return tri.is_disabled() && is_retention_property(topic_property);
}

template<typename T>
bool is_turned_off(
  const tristate<T>& tri, const std::string_view& topic_property) {
    return tri.is_disabled() && !is_retention_property(topic_property);
}

template<typename T, typename RangeMinT>
bool valid_min(
  const tristate<T>& topic_val,
  const std::optional<RangeMinT>& min,
  const std::string_view& topic_property) {
    if (min) {
        // Disabled state could mean infinite which is always greater than the
        // minimum or it could mean turned-off which is never within a range.
        if (is_infinite(topic_val, topic_property)) {
            return true;
        } else if (is_turned_off(topic_val, topic_property)) {
            return false;
        }

        // An undefined topic value implicity breaks the minimum because
        // "nothing" is not within any range. Not to be confused with disabled
        // state.
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
        // Disabled state could mean infinite which is always greater than the
        // maximum or it could mean turned-off which is never within a range.
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
    if (!(valid_min(topic_val, range.min, topic_property)
          && valid_max(topic_val, range.max))) {
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
    // the cluster one. A disabled/undefined topic value or cluster value could
    // not match the other, this implies that the constraint is not satisfied.
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
  const std::optional<T> cluster_opt = std::nullopt) {
    return ss::visit(
      constraint.flags,
      [&topic_val,
       &topic,
       &topic_property,
       &cluster_opt,
       cluster_property{constraint.name}](const constraint_enabled_t enabled) {
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

template<typename T, typename RangeT>
void range_clamp(
  tristate<T>& topic_val,
  const range_values<RangeT>& range,
  const model::topic& topic,
  const std::string_view& topic_property) {
    if (!valid_min(topic_val, range.min, topic_property)) {
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

    // A constraint with "enabled" flag means that the topic property gets the
    // cluster value, but an undefined cluster value cannot be assigned so
    // ignore.
    if (!cluster_val) {
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
  const std::optional<T> cluster_opt = std::nullopt) {
    ss::visit(
      constraint.flags,
      [&topic_val,
       &topic,
       &topic_property,
       &cluster_opt,
       cluster_property{constraint.name}](const constraint_enabled_t enabled) {
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

template<typename T>
bool do_apply_constraint_tristate(
  tristate<T>& topic_val,
  const constraint_t& constraint,
  const model::topic& topic,
  const std::string_view& topic_property,
  const std::optional<T> cluster_opt = std::nullopt) {
    if (constraint.type == config::constraint_type::restrikt) {
        return validate_value(
          topic_val, constraint, topic, topic_property, cluster_opt);
    } else if (constraint.type == config::constraint_type::clamp) {
        clamp_value(topic_val, constraint, topic, topic_property, cluster_opt);
        return true;
    }
    vlog(constraints_log.error, "Unknown constraint type");
    return false;
}

template<typename T>
bool do_apply_constraint_optional(
  std::optional<T>& topic_val,
  const constraint_t& constraint,
  const model::topic& topic,
  const std::string_view& topic_property,
  const std::optional<T> cluster_opt = std::nullopt) {
    auto tri = tristate<T>{topic_val};
    auto ret = do_apply_constraint_tristate(
      tri, constraint, topic, topic_property, cluster_opt);
    // The tristate is not in disabled state since it was assigned an optional
    // earlier.
    topic_val = tri.get_optional();
    return ret;
}

template<typename T>
bool do_apply_constraint(
  T& topic_val,
  const constraint_t& constraint,
  const model::topic& topic,
  const std::string_view& topic_property,
  const std::optional<T> cluster_opt = std::nullopt) {
    auto tri = tristate<T>{topic_val};
    auto ret = do_apply_constraint_tristate(
      tri, constraint, topic, topic_property, cluster_opt);
    // The tristate is not in disabled state since it was assigned an optional
    // earlier.
    topic_val = tri.value();
    return ret;
}
} // namespace

bool apply_constraint(
  cluster::topic_configuration& topic_cfg, const constraint_t& constraint) {
    if (
      constraint.name
      == config::shard_local_cfg().default_topic_partitions.name()) {
        return do_apply_constraint(
          topic_cfg.partition_count,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_partition_count);
    }

    if (
      constraint.name
      == config::shard_local_cfg().default_topic_replication.name()) {
        return do_apply_constraint(
          topic_cfg.replication_factor,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_replication_factor);
    }

    if (
      constraint.name
      == config::shard_local_cfg().log_compression_type.name()) {
        return do_apply_constraint_optional<model::compression>(
          topic_cfg.properties.compression,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_compression,
          config::shard_local_cfg().log_compression_type());
    }

    if (
      constraint.name == config::shard_local_cfg().log_cleanup_policy.name()) {
        return do_apply_constraint_optional<model::cleanup_policy_bitflags>(
          topic_cfg.properties.cleanup_policy_bitflags,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_cleanup_policy,
          config::shard_local_cfg().log_cleanup_policy());
    }

    if (
      constraint.name
      == config::shard_local_cfg().log_message_timestamp_type.name()) {
        return do_apply_constraint_optional<model::timestamp_type>(
          topic_cfg.properties.timestamp_type,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_timestamp_type,
          config::shard_local_cfg().log_message_timestamp_type());
    }

    if (constraint.name == config::shard_local_cfg().log_segment_size.name()) {
        return do_apply_constraint_optional(
          topic_cfg.properties.segment_size,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_segment_size);
    }

    if (constraint.name == config::shard_local_cfg().retention_bytes.name()) {
        return do_apply_constraint_tristate(
          topic_cfg.properties.retention_bytes,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_retention_bytes);
    }

    if (constraint.name == config::shard_local_cfg().log_retention_ms.name()) {
        return do_apply_constraint_tristate(
          topic_cfg.properties.retention_duration,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_retention_duration);
    }

    if (
      constraint.name
      == config::shard_local_cfg().kafka_batch_max_bytes.name()) {
        return do_apply_constraint_optional(
          topic_cfg.properties.batch_max_bytes,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_max_message_bytes);
    }

    if (
      constraint.name
      == config::shard_local_cfg().retention_local_target_ms_default.name()) {
        return do_apply_constraint_tristate(
          topic_cfg.properties.retention_local_target_bytes,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_retention_local_target_bytes);
    }

    if (
      constraint.name
      == config::shard_local_cfg().retention_local_target_ms_default.name()) {
        return do_apply_constraint_tristate(
          topic_cfg.properties.retention_local_target_ms,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_retention_local_target_ms);
    }

    if (constraint.name == config::shard_local_cfg().log_segment_ms.name()) {
        return do_apply_constraint_tristate(
          topic_cfg.properties.segment_ms,
          constraint,
          topic_cfg.tp_ns.tp,
          kafka::topic_property_segment_ms);
    }

    auto is_cloud_storage_config = [](const ss::sstring& name) {
        return name
                 == config::shard_local_cfg()
                      .cloud_storage_enable_remote_read.name()
               || name
                    == config::shard_local_cfg()
                         .cloud_storage_enable_remote_write.name();
    };
    if (
      is_cloud_storage_config(constraint.name)
      && topic_cfg.properties.shadow_indexing) {
        auto fetch_enabled = model::is_fetch_enabled(
          *topic_cfg.properties.shadow_indexing);
        auto archival_enabled = model::is_archival_enabled(
          *topic_cfg.properties.shadow_indexing);
        auto ret
          = do_apply_constraint<bool>(
              fetch_enabled,
              constraint,
              topic_cfg.tp_ns.tp,
              kafka::topic_property_remote_read,
              config::shard_local_cfg().cloud_storage_enable_remote_read())
            || do_apply_constraint<bool>(
              archival_enabled,
              constraint,
              topic_cfg.tp_ns.tp,
              kafka::topic_property_remote_write,
              config::shard_local_cfg().cloud_storage_enable_remote_write());
        topic_cfg.properties.shadow_indexing
          = model::get_shadow_indexing_mode_impl(
            archival_enabled, fetch_enabled);
        return ret;
    }

    vlog(constraints_log.error, "Unknown constraint {}", constraint);
    return false;
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
