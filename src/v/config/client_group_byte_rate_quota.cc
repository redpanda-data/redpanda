// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/client_group_byte_rate_quota.h"

namespace config {

std::ostream& operator<<(std::ostream& os, const client_group_quota& rq) {
    fmt::print(
      os,
      "{{group_name: {}, client_prefix: {}, quota: {}}}",
      rq.group_name,
      rq.clients_prefix,
      rq.quota);
    return os;
}

} // namespace config

namespace YAML {

Node convert<config::client_group_quota>::encode(const type& rhs) {
    Node node;
    node["group_name"] = rhs.group_name;
    node["clients_prefix"] = rhs.clients_prefix;
    node["quota"] = rhs.quota;
    return node;
}

bool convert<config::client_group_quota>::decode(const Node& node, type& rhs) {
    for (const auto& s : {"group_name", "clients_prefix", "quota"}) {
        if (!node[s]) {
            return false;
        }
    }
    auto group_name = node["group_name"].as<ss::sstring>();
    auto clients_prefix = node["clients_prefix"].as<ss::sstring>();
    auto quota = node["quota"].as<int64_t>();
    rhs = config::client_group_quota{
      .group_name = std::move(group_name),
      .clients_prefix = std::move(clients_prefix),
      .quota = quota};
    return true;
}

} // namespace YAML

namespace json {

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::client_group_quota& ep) {
    w.StartObject();
    w.Key("group_name");
    w.String(ep.group_name);
    w.Key("clients_prefix");
    w.String(ep.clients_prefix);
    w.Key("quota");
    w.Int64(ep.quota);
    w.EndObject();
}

} // namespace json
