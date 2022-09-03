// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/broker_authn_endpoint.h"

#include "kafka/client/exceptions.h"
#include "model/metadata.h"
#include "utils/string_switch.h"

namespace config {

std::string_view to_string_view(broker_authn_method m) {
    switch (m) {
    case broker_authn_method::none:
        return "none";
    case broker_authn_method::sasl:
        return "sasl";
    case broker_authn_method::mtls_identity:
        return "mtls_identity";
    }
}

template<>
std::optional<broker_authn_method>
from_string_view<broker_authn_method>(std::string_view sv) {
    return string_switch<broker_authn_method>(sv)
      .match("none", broker_authn_method::none)
      .match("sasl", broker_authn_method::sasl)
      .match("mtls_identity", broker_authn_method::mtls_identity)
      .default_match(broker_authn_method::none);
}

std::ostream& operator<<(std::ostream& os, const broker_authn_endpoint& ep) {
    fmt::print(os, "{{{}:{}:{}}}", ep.name, ep.address, ep.authn_method);
    return os;
}

} // namespace config

namespace YAML {

Node convert<config::broker_authn_endpoint>::encode(const type& rhs) {
    Node node;
    node["name"] = rhs.name;
    node["address"] = rhs.address.host();
    node["port"] = rhs.address.port();
    if (rhs.authn_method) {
        node["authentication_method"] = ss::sstring(
          to_string_view(*rhs.authn_method));
    }
    return node;
}

bool convert<config::broker_authn_endpoint>::decode(
  const Node& node, type& rhs) {
    for (auto s : {"address", "port"}) {
        if (!node[s]) {
            return false;
        }
    }
    ss::sstring name;
    if (node["name"]) {
        name = node["name"].as<ss::sstring>();
    }
    auto address = node["address"].as<ss::sstring>();
    auto port = node["port"].as<uint16_t>();
    auto addr = net::unresolved_address(std::move(address), port);
    std::optional<config::broker_authn_method> method{};
    if (auto n = node["authentication_method"]; bool(n)) {
        method = config::from_string_view<config::broker_authn_method>(
          n.as<ss::sstring>());
    }
    rhs = config::broker_authn_endpoint{
      .name = std::move(name),
      .address = std::move(addr),
      .authn_method = method};
    return true;
}

} // namespace YAML

void json::rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const config::broker_authn_endpoint& ep) {
    w.StartObject();
    w.Key("name");
    w.String(ep.name);
    w.Key("address");
    w.String(ep.address.host());
    w.Key("port");
    w.Uint(ep.address.port());
    if (ep.authn_method) {
        w.Key("authentication_method");
        auto method = to_string_view(*ep.authn_method);
        w.String(method.begin(), method.length());
    }
    w.EndObject();
}
