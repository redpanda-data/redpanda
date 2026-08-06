// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "base/format_to.h"
#include "config/convert.h"
#include "config/endpoint_tls_config.h"
#include "config/from_string_view.h"
#include "config/property.h"
#include "json/_include_first.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "model/metadata.h"
#include "utils/unresolved_address.h"

#include <seastar/core/sstring.hh>

#include <yaml-cpp/node/node.h>

#include <optional>
#include <string>

namespace config {

enum class broker_authn_method {
    none = 0,
    sasl,
    mtls_identity,
};

std::string_view to_string_view(broker_authn_method m);
fmt::iterator format_to(broker_authn_method m, fmt::iterator out);

template<>
std::optional<broker_authn_method>
from_string_view<broker_authn_method>(std::string_view sv);

struct broker_authn_endpoint {
    ss::sstring name;
    net::unresolved_address address;
    std::optional<broker_authn_method> authn_method;
    /// Absolute filesystem path for an AF_UNIX listener. When set, the
    /// endpoint binds an AF_UNIX socket instead of a TCP socket and
    /// `address`/`port` are ignored. Exactly one of {address+port, unix_path}
    /// must be specified per entry.
    std::optional<ss::sstring> unix_path;
    /// Filesystem mode (chmod) applied to the AF_UNIX socket after listen().
    /// Only meaningful when `unix_path` is set. Defaults to 0660 at bind time.
    std::optional<uint32_t> unix_socket_mode;

    bool is_unix_domain() const { return unix_path.has_value(); }

    friend bool
    operator==(const broker_authn_endpoint&, const broker_authn_endpoint&)
      = default;

    fmt::iterator format_to(fmt::iterator it) const;
};

/// Maximum length of a Unix domain socket path (sockaddr_un::sun_path is 108
/// bytes on Linux; reserve one byte for NUL).
inline constexpr size_t max_unix_path_length = 107;

/// Validate a single broker_authn_endpoint in isolation. Returns std::nullopt
/// on success, or a human-readable error string.
std::optional<ss::sstring>
validate_broker_authn_endpoint(const broker_authn_endpoint& ep);

/// Validate a list of broker_authn_endpoints. Checks each entry individually
/// via validate_broker_authn_endpoint(), then enforces that no two entries
/// share the same unix_path (duplicate TCP addresses are allowed for
/// backward compatibility and are caught elsewhere).
std::optional<ss::sstring>
validate_broker_authn_endpoints(const std::vector<broker_authn_endpoint>& v);

/// Validate cross-list invariants between kafka_api, kafka_api_tls, and
/// advertised_kafka_api. Specifically:
///   - no TLS entry may share a name with a UDS kafka_api entry,
///   - no advertised entry may share a name with a UDS kafka_api entry.
/// Returns std::nullopt on success, or a human-readable error string.
///
/// This is invoked from application.cc after node_config is loaded because
/// it requires access to multiple properties simultaneously and cannot be
/// expressed as a per-property validator.
std::optional<ss::sstring> validate_kafka_uds_constraints(
  const std::vector<broker_authn_endpoint>& kafka_api,
  const std::vector<endpoint_tls_config>& kafka_api_tls,
  const std::vector<model::broker_endpoint>& advertised_kafka_api);

namespace detail {

template<>
consteval std::string_view property_type_name<broker_authn_endpoint>() {
    return "config::broker_auth_endpoint";
}

} // namespace detail

bool kafka_authz_enabled();
broker_authn_method get_authn_method(std::string_view connection_name);

} // namespace config

namespace YAML {

template<>
struct convert<config::broker_authn_endpoint> {
    using type = config::broker_authn_endpoint;
    static Node encode(const type& rhs);
    static bool decode(const Node& node, type& rhs);
};

} // namespace YAML

namespace json {

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::broker_authn_endpoint& ep);

}
