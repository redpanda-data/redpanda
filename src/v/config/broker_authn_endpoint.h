// Copyright 2021 Redpanda Data, Inc.
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

namespace config {

enum class broker_authn_method {
    none = 0,
    sasl,
    mtls_identity,
};

std::string_view to_string_view(broker_authn_method m);

template<>
std::optional<broker_authn_method>
from_string_view<broker_authn_method>(std::string_view sv);

struct broker_authn_endpoint {
    ss::sstring name;
    net::unresolved_address address;
    std::optional<broker_authn_method> authn_method;

    friend bool
    operator==(const broker_authn_endpoint&, const broker_authn_endpoint&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& os, const broker_authn_endpoint& ep);
};

namespace detail {

template<>
consteval std::string_view property_type_name<broker_authn_endpoint>() {
    return "config::broker_auth_endpoint";
}

} // namespace detail

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
