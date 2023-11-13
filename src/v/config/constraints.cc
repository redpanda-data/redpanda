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

namespace config {
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
