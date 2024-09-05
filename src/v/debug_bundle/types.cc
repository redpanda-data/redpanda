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

#include <seastar/util/variant_utils.hh>

auto fmt::formatter<debug_bundle::special_date>::format(
  debug_bundle::special_date d, format_context& ctx) const
  -> format_context::iterator {
    return formatter<string_view>::format(debug_bundle::to_string_view(d), ctx);
}

fmt::format_context::iterator
fmt::formatter<debug_bundle::time_variant>::format(
  const debug_bundle::time_variant& t, format_context& ctx) const {
    return ss::visit(
      t,
      [&ctx](const debug_bundle::debug_bundle_timepoint& t) {
          auto tt = debug_bundle::debug_bundle_clock::to_time_t(t);
          std::stringstream ss;
          ss << std::put_time(std::localtime(&tt), "%FT%T");
          return fmt::format_to(ctx.out(), "{}", ss.str());
      },
      [&ctx](const debug_bundle::special_date& d) {
          return fmt::format_to(ctx.out(), "{}", d);
      });
}

fmt::format_context::iterator
fmt::formatter<debug_bundle::partition_selection>::format(
  const debug_bundle::partition_selection& p, format_context& ctx) const {
    return fmt::format_to(
      ctx.out(), "{}/{}/{}", p.first.ns, p.first.tp, fmt::join(p.second, ","));
}
