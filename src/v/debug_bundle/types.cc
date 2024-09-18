/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "types.h"

#include "strings/string_switch.h"

#include <seastar/util/variant_utils.hh>

#include <fmt/chrono.h>
#include <fmt/core.h>

namespace debug_bundle {
std::ostream& operator<<(std::ostream& o, const special_date& d) {
    return o << to_string_view(d);
}

std::istream& operator>>(std::istream& i, special_date& d) {
    ss::sstring s;
    i >> s;
    d = string_switch<special_date>(s)
          .match(
            to_string_view(special_date::yesterday), special_date::yesterday)
          .match(to_string_view(special_date::today), special_date::today)
          .match(to_string_view(special_date::now), special_date::now)
          .match(
            to_string_view(special_date::tomorrow), special_date::tomorrow);
    return i;
}

std::ostream& operator<<(std::ostream& o, const debug_bundle_status& s) {
    return o << to_string_view(s);
}

std::ostream& operator<<(std::ostream& o, const partition_selection& p) {
    fmt::print(o, "{}/{}/{}", p.tn.ns, p.tn.tp, fmt::join(p.partitions, ","));
    return o;
}
} // namespace debug_bundle

auto fmt::formatter<debug_bundle::special_date>::format(
  debug_bundle::special_date d,
  format_context& ctx) const -> format_context::iterator {
    return formatter<string_view>::format(debug_bundle::to_string_view(d), ctx);
}

auto fmt::formatter<debug_bundle::time_variant>::format(
  const debug_bundle::time_variant& t,
  format_context& ctx) const -> format_context::iterator {
    return ss::visit(
      t,
      [&ctx](const debug_bundle::clock::time_point& t) {
          auto tt = debug_bundle::clock::to_time_t(t);
          std::tm tm = *std::localtime(&tt);
          return fmt::format_to(ctx.out(), "{:%FT%T}", tm);
      },
      [&ctx](const debug_bundle::special_date& d) {
          return fmt::format_to(ctx.out(), "{}", d);
      });
}

auto fmt::formatter<debug_bundle::partition_selection>::format(
  const debug_bundle::partition_selection& p,
  format_context& ctx) const -> format_context::iterator {
    return fmt::format_to(
      ctx.out(), "{}/{}/{}", p.tn.ns, p.tn.tp, fmt::join(p.partitions, ","));
}
