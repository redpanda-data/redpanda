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

#include <seastar/util/log.hh>

#include <chrono>
#include <stdexcept>
#include <string>

namespace config {

std::string_view to_string_view(constraint_type type) {
    switch (type) {
    case constraint_type::none:
        return "none";
    case constraint_type::_restrict:
        return "restrict";
    case constraint_type::clamp:
        return "clamp";
    }

    return "invalid";
}

template<>
std::optional<constraint_type>
from_string_view<constraint_type>(std::string_view sv) {
    return string_switch<std::optional<constraint_type>>(sv)
      .match("none", constraint_type::none)
      .match("restrict", constraint_type::_restrict)
      .match("clamp", constraint_type::clamp)
      .default_match(constraint_type::none);
}

// This is where all the type conversions happen
std::shared_ptr<constraint_validator_t> constraint_validator_map(
  ss::sstring& name,
  std::optional<ss::sstring> min_str_opt,
  std::optional<ss::sstring> max_str_opt,
  constraint_enabled_t enabled) {
    auto& cfg = config::shard_local_cfg();

    if (name == "log_cleanup_policy") {
        auto def_val = cfg.log_cleanup_policy.default_value();
        return std::make_shared<
          constraint_validator_enabled<model::cleanup_policy_bitflags>>(
          std::move(enabled),
          cfg.log_cleanup_policy.bind(),
          [def_val{std::move(def_val)}](
            const cluster::topic_configuration& topic_cfg) {
              if (topic_cfg.properties.cleanup_policy_bitflags) {
                  return *topic_cfg.properties.cleanup_policy_bitflags;
              } else {
                  return def_val;
              }
          });
    } else if (name == "log_retention_ms") {
        std::optional<std::chrono::milliseconds> min_opt{std::nullopt},
          max_opt{std::nullopt};
        if (min_str_opt) {
            min_opt = std::chrono::milliseconds{std::stol(*min_str_opt)};
        }
        if (max_str_opt) {
            max_opt = std::chrono::milliseconds{std::stol(*max_str_opt)};
        }
        auto def_val = *cfg.log_retention_ms.default_value();
        return std::make_shared<
          constraint_validator_range<std::chrono::milliseconds>>(
          std::move(min_opt),
          std::move(max_opt),
          [def_val{std::move(def_val)}](
            const cluster::topic_configuration& topic_cfg)
            -> std::chrono::milliseconds {
              if (topic_cfg.properties.retention_duration
                    .has_optional_value()) {
                  return topic_cfg.properties.retention_duration.value();
              } else {
                  return def_val;
              }
          });
    } else {
        return nullptr;
    }
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
    if (node["min"]) {
        min_str_opt = node["min"].as<ss::sstring>();
    }

    if (node["max"]) {
        max_str_opt = node["max"].as<ss::sstring>();
    }

    config::constraint_enabled_t enabled{config::constraint_enabled_t::no};
    if (node["enabled"] && node["enabled"].as<bool>()) {
        enabled = config::constraint_enabled_t::yes;
    }

    rhs.validator = constraint_validator_map(
      rhs.name,
      std::move(min_str_opt),
      std::move(max_str_opt),
      std::move(enabled));
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
        constraint.validator->rjson_serialize(w);
    }
    w.EndObject();
}

} // namespace json
