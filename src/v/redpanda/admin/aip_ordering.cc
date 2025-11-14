/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "redpanda/admin/aip_ordering.h"

#include "absl/strings/match.h"
#include "absl/strings/str_split.h"
#include "serde/protobuf/base.h"
#include "serde/protobuf/rpc.h"

#include <seastar/util/variant_utils.hh>

#include <fmt/format.h>

#include <compare>
#include <limits>
#include <string_view>
#include <type_traits>

namespace admin {

namespace {
std::optional<sort_order::component::order>
parse_order_token(std::string_view tok) {
    if (tok.empty()) {
        return sort_order::component::order::ascending;
    }

    if (absl::EqualsIgnoreCase(tok, "asc")) {
        return sort_order::component::order::ascending;
    }

    if (absl::EqualsIgnoreCase(tok, "desc")) {
        return sort_order::component::order::descending;
    }

    return std::nullopt;
}

template<typename T>
concept ComparableValue
  = std::is_same_v<T, bool> || std::is_same_v<T, int32_t>
    || std::is_same_v<T, int64_t> || std::is_same_v<T, uint32_t>
    || std::is_same_v<T, uint64_t> || std::is_same_v<T, float>
    || std::is_same_v<T, double> || std::is_same_v<T, ss::sstring>
    || std::is_same_v<T, serde::pb::raw_enum_value>
    || std::is_same_v<T, absl::Time> || std::is_same_v<T, absl::Duration>
    || std::is_same_v<T, std::monostate>;

bool is_type_supported(const serde::pb::field::value_t& val) {
    return ss::visit(
      val,
      [](const ComparableValue auto&) { return true; },
      [](const auto&) { return false; });
}

auto compare_field_variant(
  const serde::pb::field::value_t& va,
  const serde::pb::field::value_t& vb,
  const std::vector<int32_t>& field_path) {
    return std::visit(
      [&field_path](const auto& a, const auto& b) -> std::strong_ordering {
          using A = std::remove_cvref_t<decltype(a)>;
          using B = std::remove_cvref_t<decltype(b)>;

          if constexpr (!ComparableValue<A> || !ComparableValue<B>) {
              throw serde::pb::rpc::internal_exception(
                fmt::format(
                  "Unsupported field type during sorting for field path: {}",
                  field_path));
          } else if constexpr (
            std::is_same_v<A, std::monostate>
            || std::is_same_v<B, std::monostate>) {
              // Handle monostate (unset fields)
              if constexpr (std::is_same_v<A, B>) {
                  return std::strong_ordering::equal;
              } else {
                  // A=null -> A < B; B=null -> A > B
                  return std::is_same_v<A, std::monostate>
                           ? std::strong_ordering::less
                           : std::strong_ordering::greater;
              }
          } else if constexpr (!std::is_same_v<A, B>) {
              throw serde::pb::rpc::internal_exception(
                fmt::format("Type mismatch for field path: {}", field_path));
          } else if constexpr (std::is_same_v<A, serde::pb::raw_enum_value>) {
              // Compare enums by their variant number
              return a.number <=> b.number;
          } else if constexpr (std::is_floating_point_v<A>) {
              if (a < b) {
                  return std::strong_ordering::less;
              } else if (a > b) {
                  return std::strong_ordering::greater;
              }
              using integral_type = std::
                conditional_t<sizeof(A) == sizeof(int32_t), int32_t, int64_t>;
              // Normalize nans so they compare equal
              auto abits = std::bit_cast<integral_type, A>(
                std::isnan(a) ? std::numeric_limits<A>::quiet_NaN() : a);
              auto bbits = std::bit_cast<integral_type, B>(
                std::isnan(b) ? std::numeric_limits<B>::quiet_NaN() : b);
              // Comparing the same bits covers NaN, and also -0 vs 0
              return abits <=> bbits;
          } else {
              return a <=> b;
          }
      },
      va,
      vb);
}

} // namespace

sort_order::sort_order(std::vector<component> components)
  : _components(std::move(components)) {}

sort_order sort_order::parse(const aip_ordering_config& config) {
    auto result = std::vector<component>{};

    auto parts = absl::StrSplit(std::string_view{config.ordering_expr}, ',');
    for (auto part : parts) {
        part = absl::StripAsciiWhitespace(part);

        // Split by first space: field [order]
        auto space_pos = part.find_first_of(' ');
        auto field = part.substr(0, space_pos);
        auto ord_tok = (space_pos != std::string_view::npos)
                         ? absl::StripAsciiWhitespace(part.substr(space_pos))
                         : std::string_view{};

        if (field.empty()) {
            throw serde::pb::rpc::invalid_argument_exception(
              fmt::format(
                "Invalid (empty) field in ordering expression: '{}'", part));
        }

        std::vector<std::string_view> path_components = absl::StrSplit(
          field, '.');
        auto field_nums = config.field_path_converter(path_components);
        if (!field_nums) {
            throw serde::pb::rpc::invalid_argument_exception(
              fmt::format(
                "Invalid field path in ordering expression: '{}'", field));
        }

        auto field_type = config.field_type_getter(field_nums.value());
        if (!is_type_supported(field_type)) {
            throw serde::pb::rpc::invalid_argument_exception(
              fmt::format("Unsupported field type for field: '{}'", field));
        }

        auto ord = parse_order_token(ord_tok);
        if (!ord) {
            throw serde::pb::rpc::invalid_argument_exception(
              fmt::format(
                "Unknown ordering specifier '{}', must be 'asc' or 'desc'",
                ord_tok));
        }

        result.emplace_back(
          component{.field_numbers = field_nums.value(), .ord = ord.value()});
    }

    if (result.empty()) {
        throw serde::pb::rpc::invalid_argument_exception(
          "Ordering expression did not specify any fields");
    }

    return sort_order(std::move(result));
}

bool sort_order::operator()(
  serde::pb::base_message& a, serde::pb::base_message& b) const {
    for (const auto& comp : _components) {
        auto fa = a.lookup_field(comp.field_numbers);
        auto fb = b.lookup_field(comp.field_numbers);

        if (!fa || !fb) {
            throw serde::pb::rpc::invalid_argument_exception(
              fmt::format(
                "Failed field lookup while sorting for field path {}",
                comp.field_numbers));
        }

        auto cmp = compare_field_variant(
          fa->value, fb->value, comp.field_numbers);

        if (cmp != std::strong_ordering::equal) {
            // Ascending: cmp as is. Descending: invert
            return comp.ord == component::order::ascending
                     ? cmp == std::partial_ordering::less
                     : cmp == std::partial_ordering::greater;
        }
    }

    // All compared equal, a < b should return false
    return false;
}

} // namespace admin
