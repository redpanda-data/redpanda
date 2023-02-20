/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "config/convert.h"
#include "config/property.h"
#include "json/_include_first.h"
#include "json/stringbuffer.h"
#include "json/writer.h"

#include <seastar/core/sstring.hh>

#include <yaml-cpp/node/node.h>

#include <iosfwd>
#include <string>

namespace config {

struct client_group_quota {
    using key_type = ss::sstring;

    ss::sstring group_name;
    ss::sstring clients_prefix;
    int64_t quota;

    static ss::sstring key_name() { return "group_name"; }

    const ss::sstring& key() const { return group_name; }

    friend bool operator==(const client_group_quota&, const client_group_quota&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& os, const client_group_quota& ep);
};

namespace detail {

template<>
consteval std::string_view property_type_name<client_group_quota>() {
    return "config::client_group_quota";
}

} // namespace detail

} // namespace config

namespace YAML {
template<>
struct convert<config::client_group_quota> {
    using type = config::client_group_quota;
    static Node encode(const type& rhs);
    static bool decode(const Node& node, type& rhs);
};

} // namespace YAML

namespace json {

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::client_group_quota& ep);

} // namespace json
