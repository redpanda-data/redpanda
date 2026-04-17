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

#include "serde/parquet/value.h"

#include "absl/strings/escaping.h"
#include "bytes/hash.h"

#include <seastar/util/variant_utils.hh>

namespace serde::parquet {

value copy(const value& val) {
    return ss::visit(
      val,
      [](const byte_array_value& v) {
          return value(byte_array_value(v.val.copy()));
      },
      [](const fixed_byte_array_value& v) {
          return value(fixed_byte_array_value(v.val.copy()));
      },
      [](const group_value& v) {
          group_value clone;
          clone.reserve(v.size());
          for (const auto& member : v) {
              clone.emplace_back(copy(member.field));
          }
          return value(std::move(clone));
      },
      [](const repeated_value& v) {
          repeated_value clone;
          clone.reserve(v.size());
          for (const auto& member : v) {
              clone.emplace_back(copy(member.element));
          }
          return value(std::move(clone));
      },
      [](const auto& v) { return value(v); });
}

size_t value_hash::operator()(const value& v) const {
    return ss::visit(
      v,
      [](const null_value&) -> size_t { return 0; },
      [](const boolean_value& x) -> size_t { return std::hash<bool>{}(x.val); },
      [](const int32_value& x) -> size_t {
          return std::hash<int32_t>{}(x.val);
      },
      [](const int64_value& x) -> size_t {
          return std::hash<int64_t>{}(x.val);
      },
      [](const float32_value& x) -> size_t {
          return std::hash<float>{}(x.val);
      },
      [](const float64_value& x) -> size_t {
          return std::hash<double>{}(x.val);
      },
      [](const byte_array_value& x) -> size_t {
          return std::hash<iobuf>{}(x.val);
      },
      [](const fixed_byte_array_value& x) -> size_t {
          return std::hash<iobuf>{}(x.val);
      },
      [](const group_value& x) -> size_t { return group_value_hash{}(x); },
      [](const repeated_value&) -> size_t { return 0; });
}

size_t group_value_hash::operator()(const group_value& gv) const {
    size_t h = 0;
    value_hash vh;
    for (const auto& m : gv) {
        // Boost-style hash combine for better distribution.
        h ^= vh(m.field) + 0x9e3779b9 + (h << 6) + (h >> 2);
    }
    return h;
}

} // namespace serde::parquet

auto fmt::formatter<serde::parquet::value>::format(
  const serde::parquet::value& val, fmt::format_context& ctx) const
  -> decltype(ctx.out()) {
    using namespace serde::parquet;
    return ss::visit(
      val,
      [&ctx](const null_value&) { return fmt::format_to(ctx.out(), "NULL"); },
      [&ctx](const boolean_value& v) {
          return fmt::format_to(ctx.out(), "{}", v.val);
      },
      [&ctx](const int32_value& v) {
          return fmt::format_to(ctx.out(), "{}", v.val);
      },
      [&ctx](const int64_value& v) {
          return fmt::format_to(ctx.out(), "{}", v.val);
      },
      [&ctx](const float32_value& v) {
          return fmt::format_to(ctx.out(), "{}", v.val);
      },
      [&ctx](const float64_value& v) {
          return fmt::format_to(ctx.out(), "{}", v.val);
      },
      [&ctx](const byte_array_value& v) {
          auto out = ctx.out();
          for (const auto& e : v.val) {
              std::string_view s{e.get(), e.size()};
              out = fmt::format_to(out, "{}", absl::CEscape(s));
          }
          return out;
      },
      [&ctx](const fixed_byte_array_value& v) {
          auto out = ctx.out();
          for (const auto& e : v.val) {
              std::string_view s{e.get(), e.size()};
              out = fmt::format_to(out, "{}", absl::CEscape(s));
          }
          return out;
      },
      [&ctx](const group_value& v) {
          return fmt::format_to(ctx.out(), "({})", fmt::join(v, ", "));
      },
      [&ctx](const repeated_value& v) {
          return fmt::format_to(ctx.out(), "[{}]", fmt::join(v, ", "));
      });
}

auto fmt::formatter<serde::parquet::group_member>::format(
  const serde::parquet::group_member& f, fmt::format_context& ctx) const
  -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", f.field);
}

auto fmt::formatter<serde::parquet::repeated_element>::format(
  const serde::parquet::repeated_element& e, fmt::format_context& ctx) const
  -> decltype(ctx.out()) {
    return fmt::format_to(ctx.out(), "{}", e.element);
}
