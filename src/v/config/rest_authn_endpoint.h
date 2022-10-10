// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "config/convert.h"
#include "config/from_string_view.h"
#include "config/property.h"
#include "json/_include_first.h"
#include "json/stringbuffer.h"
#include "json/writer.h"
#include "net/unresolved_address.h"

#include <seastar/core/sstring.hh>

#include <yaml-cpp/node/node.h>

#include <iosfwd>
#include <optional>
#include <string>
#include <vector>

namespace config {

enum class rest_authn_method {
    none = 0,
    http_basic,
};

std::string_view to_string_view(rest_authn_method m);

template<>
std::optional<rest_authn_method>
from_string_view<rest_authn_method>(std::string_view sv);

struct rest_authn_endpoint {
    ss::sstring name;
    net::unresolved_address address;
    std::optional<rest_authn_method> authn_method;

    friend bool
    operator==(const rest_authn_endpoint&, const rest_authn_endpoint&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& os, const rest_authn_endpoint& ep);
};

// A helper method that searches for the listener within a vector of
// rest_authn_endpoints. Returns the authn method if the address is found.
// Returns none type otherwise
rest_authn_method get_authn_method(
  const std::vector<rest_authn_endpoint>& endpoints, size_t listener_idx);

struct always_true : public binding<bool> {
    always_true()
      : binding<bool>(true) {}
};

namespace detail {

template<>
consteval std::string_view property_type_name<rest_authn_endpoint>() {
    return "config::rest_authn_endpoint";
}

} // namespace detail

} // namespace config

namespace YAML {

template<>
struct convert<config::rest_authn_endpoint> {
    using type = config::rest_authn_endpoint;
    static Node encode(const type& rhs);
    static bool decode(const Node& node, type& rhs);
};

} // namespace YAML

namespace json {

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::rest_authn_endpoint& ep);

}
