/*
 * Copyright 2023 Redpanda Data, Inc.
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
#include "config/from_string_view.h"
#include "config/property.h"
#include "json/json.h"
#include "ssx/sformat.h"

#include <seastar/core/sstring.hh>

#include <yaml-cpp/node/node.h>

#include <optional>
#include <string>

namespace cluster {
struct topic_configuration;
} // namespace cluster

namespace config {
enum class constraint_type {
    restrikt = 0,
    clamp = 1,
};

std::string_view to_string_view(constraint_type type);

template<>
std::optional<constraint_type>
from_string_view<constraint_type>(std::string_view sv);

using constraint_enabled_t = ss::bool_class<struct constraint_enabled_tag>;

namespace {
template<typename T>
struct range_values {
    std::optional<T> min;
    std::optional<T> max;

    range_values() = default;
    range_values(std::optional<T> min_opt, std::optional<T> max_opt)
      : min{std::move(min_opt)}
      , max{std::move(max_opt)} {}

    friend std::ostream&
    operator<<(std::ostream& os, const range_values<T>& range) {
        os << ssx::sformat(" min: {}, max: {}", range.min, range.max);
        return os;
    }

    friend bool operator==(const range_values&, const range_values&) = default;
    friend bool operator!=(const range_values&, const range_values&) = default;
};
} // namespace

// Captures the flags that constraints could hold
struct constraint_t {
    using key_type = ss::sstring;

    ss::sstring name;
    constraint_type type;

    std::variant<
      range_values<int64_t>,
      range_values<uint64_t>,
      constraint_enabled_t>
      flags;

    static ss::sstring key_name() { return "name"; }

    const ss::sstring& key() const { return name; }

    friend bool operator==(const constraint_t&, const constraint_t&) = default;

    friend std::ostream& operator<<(std::ostream& os, const constraint_t& args);
};

namespace detail {

template<>
consteval std::string_view property_type_name<constraint_t>() {
    return "config::constraint_t";
}

} // namespace detail
} // namespace config

namespace YAML {
template<>
struct convert<config::constraint_t> {
    using type = config::constraint_t;
    static Node encode(const type& rhs);
    static bool decode(const Node& node, type& rhs);
};

} // namespace YAML

namespace json {

void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::constraint_enabled_t& ep);
void rjson_serialize(
  json::Writer<json::StringBuffer>& w, const config::constraint_t& ep);

} // namespace json
