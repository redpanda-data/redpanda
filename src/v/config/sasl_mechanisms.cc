// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/sasl_mechanisms.h"

#include "config/configuration.h"

#include <algorithm>

namespace config {

bool is_enterprise_sasl_mechanism(const ss::sstring& sasl_mech) {
    return std::ranges::contains(enterprise_sasl_mechanisms, sasl_mech);
}

bool has_sasl_mechanism(const std::string_view sasl_mech) {
    const auto contains_mech =
      [sasl_mech](const std::vector<ss::sstring>& sasl_mechanisms) {
          return std::ranges::contains(sasl_mechanisms, sasl_mech);
      };
    const bool in_config = contains_mech(
      config::shard_local_cfg().sasl_mechanisms());
    const auto overrides = config::shard_local_cfg().sasl_mechanisms_overrides()
                           | std::views::transform(
                             &sasl_mechanisms_override::sasl_mechanisms);
    const bool in_overrides = std::ranges::any_of(overrides, contains_mech);
    return in_config || in_overrides;
}

bool has_sasl_mechanism(
  const std::string_view listener, const std::string_view sasl_mech) {
    return std::ranges::contains(get_sasl_mechanisms(listener), sasl_mech);
}

const std::vector<ss::sstring>&
get_sasl_mechanisms(const std::string_view listener) {
    const auto& overrides
      = config::shard_local_cfg().sasl_mechanisms_overrides();
    const auto it = std::ranges::find(
      overrides, listener, &sasl_mechanisms_override::listener);
    if (it == overrides.end()) {
        return config::shard_local_cfg().sasl_mechanisms();
    }
    return it->sasl_mechanisms;
}

std::ostream&
operator<<(std::ostream& os, const sasl_mechanisms_override& rhs) {
    fmt::print(os, "{{{}:{}}}", rhs.listener, rhs.sasl_mechanisms);
    return os;
}

bool is_enterprise_sasl_mechanisms_override(
  const sasl_mechanisms_override& rhs) {
    return std::ranges::any_of(
      rhs.sasl_mechanisms, is_enterprise_sasl_mechanism);
}

} // namespace config
  //
namespace YAML {

Node convert<config::sasl_mechanisms_override>::encode(const type& rhs) {
    Node node;
    node["listener"] = rhs.listener;
    node["sasl_mechanisms"] = rhs.sasl_mechanisms;
    return node;
}

bool convert<config::sasl_mechanisms_override>::decode(
  const Node& node, type& rhs) {
    for (auto s : {"listener", "sasl_mechanisms"}) {
        if (!node[s]) {
            return false;
        }
    }
    rhs = config::sasl_mechanisms_override{
      .listener = node["listener"].as<ss::sstring>(),
      .sasl_mechanisms
      = node["sasl_mechanisms"].as<std::vector<ss::sstring>>()};
    return true;
}

} // namespace YAML

void json::rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const config::sasl_mechanisms_override& rhs) {
    w.StartObject();
    w.Key("listener");
    w.String(rhs.listener);
    w.Key("sasl_mechanisms");
    rjson_serialize(w, rhs.sasl_mechanisms);
    w.EndObject();
}
