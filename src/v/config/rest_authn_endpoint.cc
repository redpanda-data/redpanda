// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/rest_authn_endpoint.h"

#include "kafka/client/exceptions.h"
#include "model/metadata.h"
#include "utils/string_switch.h"

namespace config {

std::string_view to_string_view(rest_authn_type m) {
    switch (m) {
    case rest_authn_type::none:
        return "none";
    case rest_authn_type::mtls:
        return "mtls";
    case rest_authn_type::http_basic:
        return "http_basic";
    }
}

template<>
std::optional<rest_authn_type>
from_string_view<rest_authn_type>(std::string_view sv) {
    return string_switch<rest_authn_type>(sv)
      .match("none", rest_authn_type::none)
      .match("mtls", rest_authn_type::mtls)
      .match("http_basic", rest_authn_type::http_basic)
      .default_match(rest_authn_type::none);
}

std::ostream& operator<<(std::ostream& os, const rest_authn_endpoint& ep) {
    fmt::print(os, "{{{}:{}:{}}}", ep.name, ep.address, ep.authn_type);
    return os;
}

} // namespace config

namespace YAML {

Node convert<config::rest_authn_endpoint>::encode(const type& rhs) {
    Node node;
    node["name"] = rhs.name;
    node["address"] = rhs.address.host();
    node["port"] = rhs.address.port();
    if (rhs.authn_type) {
        node["authentication_method"]["authentication_type"] = ss::sstring(
          to_string_view(*rhs.authn_type));
    }
    return node;
}

bool convert<config::rest_authn_endpoint>::decode(const Node& node, type& rhs) {
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
    std::optional<config::rest_authn_type> authn_type{};
    if (auto n = node["authentication_method"]["authentication_type"];
        bool(n)) {
        authn_type = config::from_string_view<config::rest_authn_type>(
          n.as<ss::sstring>());
    }
    rhs = config::rest_authn_endpoint{
      .name = std::move(name),
      .address = std::move(addr),
      .authn_type = authn_type};
    return true;
}

} // namespace YAML

void json::rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::rest_authn_endpoint& ep) {
    w.StartObject();
    w.Key("name");
    w.String(ep.name);
    w.Key("address");
    w.String(ep.address.host());
    w.Key("port");
    w.Uint(ep.address.port());
    // If Authn type exists, then a
    // nested object is created.
    if (ep.authn_type) {
        w.Key("authentication_method");
        w.StartObject();
        w.Key("authentication_type");
        auto authn_type = to_string_view(*ep.authn_type);
        w.String(authn_type.begin(), authn_type.length());
        w.EndObject();
    }
    w.EndObject();
}
