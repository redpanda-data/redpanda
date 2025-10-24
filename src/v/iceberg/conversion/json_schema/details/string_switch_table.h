// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#pragma once

#include "strings/string_switch.h"

#include <array>
#include <string_view>
#include <utility>

namespace iceberg::conversion::json_schema::details {

struct string_switch_table_no_default_t {};

inline constexpr string_switch_table_no_default_t
  string_switch_table_no_default{};

template<auto Default = string_switch_table_no_default, typename T, size_t Size>
constexpr auto string_switch_table(
  std::string_view schema_id, const std::array<T, Size>& array) {
    using default_t = decltype(Default);

    using val_t = decltype(array[0].second);
    using ret_t = std::conditional_t<
      std::is_same_v<default_t, string_switch_table_no_default_t>,
      val_t,
      std::conditional_t<
        std::is_same_v<default_t, std::nullopt_t>,
        std::optional<val_t>,
        default_t>>;

    auto matcher = string_switch<ret_t>(schema_id);

    [&]<size_t... Is>(std::index_sequence<Is...>) {
        ((matcher.match(array[Is].first, array[Is].second)), ...);
    }(std::make_index_sequence<Size>{});

    if constexpr (!std::
                    is_same_v<default_t, string_switch_table_no_default_t>) {
        return matcher.default_match(Default);
    }

    return ret_t{std::move(matcher)};
}

}; // namespace iceberg::conversion::json_schema::details
