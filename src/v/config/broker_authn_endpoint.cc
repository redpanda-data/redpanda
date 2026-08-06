// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/broker_authn_endpoint.h"

#include "config/configuration.h"
#include "config/node_config.h"
#include "strings/string_switch.h"

#include <algorithm>
#include <filesystem>
#include <string_view>

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
fmt::iterator format_to(broker_authn_method m, fmt::iterator out) {
    return fmt::format_to(out, "{}", to_string_view(m));
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

fmt::iterator broker_authn_endpoint::format_to(fmt::iterator it) const {
    if (unix_path) {
        return fmt::format_to(
          it,
          "{{{}:unix:{}:mode={:#o}:{}}}",
          name,
          *unix_path,
          unix_socket_mode.value_or(0660),
          authn_method ? to_string_view(*authn_method) : "none");
    }
    return fmt::format_to(
      it,
      "{{{}:{}:{}}}",
      name,
      address,
      authn_method ? to_string_view(*authn_method) : "none");
}

std::optional<ss::sstring>
validate_broker_authn_endpoints(const std::vector<broker_authn_endpoint>& v) {
    for (const auto& ep : v) {
        if (auto err = validate_broker_authn_endpoint(ep); err) {
            return fmt::format("kafka_api entry '{}': {}", ep.name, *err);
        }
    }
    // Duplicate unix_path check.
    for (size_t i = 0; i < v.size(); ++i) {
        if (!v[i].unix_path) {
            continue;
        }
        for (size_t j = i + 1; j < v.size(); ++j) {
            if (v[j].unix_path && *v[i].unix_path == *v[j].unix_path) {
                return fmt::format(
                  "duplicate unix_path {} in kafka_api entries '{}' and '{}'",
                  *v[i].unix_path,
                  v[i].name,
                  v[j].name);
            }
        }
    }
    return std::nullopt;
}

std::optional<ss::sstring> validate_kafka_uds_constraints(
  const std::vector<broker_authn_endpoint>& kafka_api,
  const std::vector<endpoint_tls_config>& kafka_api_tls,
  const std::vector<model::broker_endpoint>& advertised_kafka_api) {
    for (const auto& ep : kafka_api) {
        if (!ep.unix_path) {
            continue;
        }
        // TLS on UDS is rejected.
        auto tls_it = std::find_if(
          kafka_api_tls.begin(),
          kafka_api_tls.end(),
          [&ep](const endpoint_tls_config& t) {
              return t.name == ep.name && t.config.is_enabled();
          });
        if (tls_it != kafka_api_tls.end()) {
            return fmt::format(
              "TLS is not supported on UDS kafka_api listener '{}'", ep.name);
        }
        // UDS listeners must not be advertised.
        auto adv_it = std::find_if(
          advertised_kafka_api.begin(),
          advertised_kafka_api.end(),
          [&ep](const model::broker_endpoint& a) { return a.name == ep.name; });
        if (adv_it != advertised_kafka_api.end()) {
            return fmt::format(
              "cannot advertise UDS kafka_api listener '{}' in "
              "advertised_kafka_api",
              ep.name);
        }
    }
    return std::nullopt;
}

std::optional<ss::sstring>
validate_broker_authn_endpoint(const broker_authn_endpoint& ep) {
    const bool has_inet = !ep.address.host().empty() || ep.address.port() != 0;
    const bool has_uds = ep.unix_path.has_value();
    if (has_inet == has_uds) {
        return ss::sstring{
          "exactly one of (address + port) or unix_path must be set"};
    }
    if (has_uds) {
        const auto& path = *ep.unix_path;
        if (path.empty()) {
            return ss::sstring{"unix_path must not be empty"};
        }
        if (path[0] != '/') {
            return ss::sstring{"unix_path must be an absolute path"};
        }
        if (path.size() > max_unix_path_length) {
            return fmt::format(
              "unix_path too long ({} bytes, max {})",
              path.size(),
              max_unix_path_length);
        }
        // Security: reject embedded NUL bytes. sun_path is NUL-terminated at
        // the kernel layer, so a path like "/real/path\0/elsewhere" would
        // pass every length/absolute check here but be silently truncated by
        // bind(2) — the config and the kernel would disagree about which
        // filesystem entry is being created.
        if (path.find('\0') != ss::sstring::npos) {
            return ss::sstring{"unix_path must not contain embedded NUL bytes"};
        }
        // Security: reject lexical path traversal. A legitimate operator
        // never writes ".." in a UDS path. Rejecting up-front gives a clear
        // error and makes any future outer-layer restriction (e.g. an
        // admission policy that confines sockets to /var/run/redpanda) an
        // enforceable invariant rather than a bypass-by-traversal problem.
        {
            std::filesystem::path p{std::string{path}};
            for (const auto& part : p) {
                if (part == "..") {
                    return ss::sstring{
                      "unix_path must not contain '..' components"};
                }
            }
        }
        // Cosmetic + defensive: reject trailing slashes and collapse //
        // runs. A trailing slash is never valid for a socket path; // runs
        // hint at config-generator bugs. lexically_normal() collapses //
        // but does not strip a trailing '/', so it is checked explicitly.
        if (path.size() > 1 && path.back() == '/') {
            return fmt::format(
              "unix_path '{}' must not have a trailing '/'", path);
        }
        {
            std::filesystem::path p{std::string{path}};
            auto normalized = p.lexically_normal().string();
            if (normalized != std::string_view{path}) {
                return fmt::format(
                  "unix_path '{}' is not in canonical form (expected '{}'); "
                  "remove duplicate '/' separators",
                  path,
                  normalized);
            }
        }
        // Mode upper bound is 07777 so that setuid/setgid/sticky bits are
        // addressable. The setgid bit (02000) in particular is the standard
        // pattern for "inherit parent-dir GID on file creation", which is
        // useful for cross-container bind-mount deployments.
        if (ep.unix_socket_mode.has_value() && *ep.unix_socket_mode > 07777u) {
            return fmt::format(
              "unix_socket_mode {:#o} out of range (max 07777)",
              *ep.unix_socket_mode);
        }
    }
    return std::nullopt;
}

bool kafka_authz_enabled() {
    return config::shard_local_cfg().kafka_enable_authorization().value_or(
      config::shard_local_cfg().enable_sasl());
}

broker_authn_method get_authn_method(std::string_view connection_name) {
    // If authn_method is set on the endpoint
    //    Use it
    // Else if kafka_enable_authorization is not set
    //    Use sasl if enable_sasl
    // Else if has mtls mapping rules
    //    Use mtls_identity
    // Else
    //    Disable AuthN

    std::optional<config::broker_authn_method> authn_method;
    const auto& kafka_api = config::node().kafka_api.value();
    auto ep_it = std::ranges::find(
      kafka_api, connection_name, &broker_authn_endpoint::name);
    if (ep_it != kafka_api.end()) {
        authn_method = ep_it->authn_method;
    }
    if (authn_method.has_value()) {
        return *authn_method;
    }
    const auto& config = config::shard_local_cfg();
    // if kafka_enable_authorization is not set, use sasl iff enable_sasl
    if (
      !config.kafka_enable_authorization().has_value()
      && config.enable_sasl()) {
        return config::broker_authn_method::sasl;
    }
    return config::broker_authn_method::none;
}

} // namespace config

namespace YAML {

Node convert<config::broker_authn_endpoint>::encode(const type& rhs) {
    Node node;
    node["name"] = rhs.name;
    if (rhs.unix_path) {
        node["unix_path"] = *rhs.unix_path;
        if (rhs.unix_socket_mode) {
            node["unix_socket_mode"] = *rhs.unix_socket_mode;
        }
    } else {
        node["address"] = rhs.address.host();
        node["port"] = rhs.address.port();
    }
    if (rhs.authn_method) {
        node["authentication_method"] = ss::sstring(
          to_string_view(*rhs.authn_method));
    }
    return node;
}

bool convert<config::broker_authn_endpoint>::decode(
  const Node& node, type& rhs) {
    const bool has_unix_path = bool(node["unix_path"]);
    const bool has_address = bool(node["address"]) || bool(node["port"]);
    // Exactly one of {address+port, unix_path} must be present.
    if (has_unix_path == has_address) {
        return false;
    }
    ss::sstring name;
    if (node["name"]) {
        name = node["name"].as<ss::sstring>();
    }
    std::optional<config::broker_authn_method> method{};
    if (auto n = node["authentication_method"]; bool(n)) {
        method = config::from_string_view<config::broker_authn_method>(
          n.as<ss::sstring>());
    }
    if (has_unix_path) {
        auto path = node["unix_path"].as<ss::sstring>();
        std::optional<uint32_t> mode;
        if (auto n = node["unix_socket_mode"]; bool(n)) {
            mode = n.as<uint32_t>();
        }
        rhs = config::broker_authn_endpoint{
          .name = std::move(name),
          .address = {},
          .authn_method = method,
          .unix_path = std::move(path),
          .unix_socket_mode = mode};
        return true;
    }
    // Inet form requires both address and port.
    if (!node["address"] || !node["port"]) {
        return false;
    }
    auto address = node["address"].as<ss::sstring>();
    auto port = node["port"].as<uint16_t>();
    auto addr = net::unresolved_address(std::move(address), port);
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
    if (ep.unix_path) {
        w.Key("unix_path");
        w.String(*ep.unix_path);
        if (ep.unix_socket_mode) {
            w.Key("unix_socket_mode");
            w.Uint(*ep.unix_socket_mode);
        }
    } else {
        w.Key("address");
        w.String(ep.address.host());
        w.Key("port");
        w.Uint(ep.address.port());
    }
    if (ep.authn_method) {
        w.Key("authentication_method");
        auto method = to_string_view(*ep.authn_method);
        w.String(method.data(), method.length());
    }
    w.EndObject();
}
