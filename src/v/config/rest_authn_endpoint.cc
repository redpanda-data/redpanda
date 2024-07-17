// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/rest_authn_endpoint.h"

#include "model/metadata.h"
#include "strings/string_switch.h"

namespace config {

std::string_view to_string_view(rest_authn_method m) {
    switch (m) {
    case rest_authn_method::none:
        return "none";
    case rest_authn_method::http_basic:
        return "http_basic";
    }
    return "invalid";
}

template<>
std::optional<rest_authn_method>
from_string_view<rest_authn_method>(std::string_view sv) {
    return string_switch<std::optional<rest_authn_method>>(sv)
      .match("none", rest_authn_method::none)
      .match("http_basic", rest_authn_method::http_basic)
      .default_match(std::nullopt);
}

std::ostream& operator<<(std::ostream& os, const rest_authn_endpoint& ep) {
    std::string_view authn_method_str{"<nullopt>"};
    if (ep.authn_method) {
        authn_method_str = to_string_view(*ep.authn_method);
    }

    fmt::print(os, "{{{}:{}:{}}}", ep.name, ep.address, authn_method_str);
    return os;
}

rest_authn_method get_authn_method(
  const std::vector<rest_authn_endpoint>& endpoints, size_t listener_idx) {
    if (listener_idx >= endpoints.size()) {
        return rest_authn_method::none;
    }

    return endpoints[listener_idx].authn_method.value_or(
      rest_authn_method::none);
}
} // namespace config

namespace YAML {

Node convert<config::rest_authn_endpoint>::encode(const type& rhs) {
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
    std::optional<config::rest_authn_method> authn_method{};
    if (auto n = node["authentication_method"]; bool(n)) {
        authn_method = config::from_string_view<config::rest_authn_method>(
          n.as<ss::sstring>());
    }
    rhs = config::rest_authn_endpoint{
      .name = std::move(name),
      .address = std::move(addr),
      .authn_method = authn_method};
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
    if (ep.authn_method) {
        w.Key("authentication_method");
        auto authn_method = to_string_view(*ep.authn_method);
        w.String(authn_method.begin(), authn_method.length());
    }
    w.EndObject();
}
