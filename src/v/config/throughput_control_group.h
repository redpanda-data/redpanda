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

#include "base/seastarx.h"
#include "config/property.h"
#include "json/_include_first.h"
#include "json/stringbuffer.h"
#include "json/writer.h"

#include <seastar/core/print.hh>
#include <seastar/core/sstring.hh>

#include <yaml-cpp/node/node.h>

#include <algorithm>
#include <cstddef>
#include <iterator>
#include <string_view>

namespace re2 {
class RE2;
}

namespace config {

struct throughput_control_group {
    throughput_control_group();
    throughput_control_group(const throughput_control_group&);
    throughput_control_group& operator=(const throughput_control_group&);
    throughput_control_group(throughput_control_group&&) noexcept;
    throughput_control_group& operator=(throughput_control_group&&) noexcept;
    ~throughput_control_group() noexcept;

    friend bool
    operator==(const throughput_control_group&, const throughput_control_group&)
      = default;

    friend std::ostream&
    operator<<(std::ostream& os, const throughput_control_group& tcg);

    bool match_client_id(std::optional<std::string_view> client_id) const;
    bool is_noname() const noexcept;
    ss::sstring validate() const;

    ss::sstring name;
    std::unique_ptr<struct client_id_matcher_type> client_id_matcher;
    // nuillopt means unlimited:
    std::optional<int64_t> throughput_limit_node_in_bps;
    std::optional<int64_t> throughput_limit_node_out_bps;
};

template<>
consteval std::string_view
detail::property_type_name<throughput_control_group>() {
    return "config::throughput_control_group";
}

template<class InputIt>
InputIt find_throughput_control_group(
  const InputIt first,
  const InputIt last,
  const std::optional<std::string_view> client_id) {
    return std::find_if(
      first, last, [&client_id](const config::throughput_control_group& cg) {
          return cg.match_client_id(client_id);
      });
}

template<class InputIt>
std::optional<ss::sstring>
validate_throughput_control_groups(const InputIt first, const InputIt last) {
    // verify that group names are unique where set
    // o(n^2/2) algo because we don't expect many items
    for (auto i = first; i != last; ++i) {
        if (const auto verr = i->validate(); unlikely(!verr.empty())) {
            return ss::format(
              "Validation failed for throughput control group #{}: {}. cgroup: "
              "{{{}}}",
              std::distance(first, i),
              verr,
              *i);
        }
        if (i->is_noname()) {
            continue;
        }
        if (unlikely(
              std::any_of(first, i, [i](const throughput_control_group& cg) {
                  return cg.name == i->name;
              }))) {
            return ss::format(
              "Duplicate throughput control group name: {}", i->name);
        }
    }
    return std::nullopt;
}

} // namespace config

namespace YAML {
template<>
struct convert<config::throughput_control_group> {
    using type = config::throughput_control_group;
    static Node encode(const type& rhs);
    static bool decode(const Node& node, type& rhs);
};

} // namespace YAML

namespace json {

void rjson_serialize(
  json::Writer<json::StringBuffer>& w,
  const config::throughput_control_group& ep);

} // namespace json
