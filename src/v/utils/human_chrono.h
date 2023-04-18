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

#include <fmt/chrono.h>
#include <fmt/core.h>

#include <chrono>
#include <concepts>

// Formatting tools for chrono types

namespace human {

namespace detail {

template<class Rep, class Per>
struct seconds_decimal_view {
    const std::chrono::duration<Rep, Per>& d;
};

} // namespace detail

/// \brief Compile-time calculate number of digits required to represent the
/// number \p N in the specified \p Base, excluding the minus sign
template<std::integral auto N, int Base>
constexpr int num_digits = N<Base && N> - Base ? 1
                                               : num_digits<N / Base, Base> + 1;

} // namespace human

template<std::integral Rep, class Per>
struct fmt::formatter<human::detail::seconds_decimal_view<Rep, Per>> {
    static_assert(
      Per::num == 1 && Per::den > 1,
      "Only durations at sub-second scale are supported");

    constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) {
        const auto it = ctx.begin(), end = ctx.end();
        if (it != end && *it != '}') {
            throw format_error("invalid format");
        }
        return it;
    }

    template<typename FormatContext>
    auto format(
      const ::human::detail::seconds_decimal_view<Rep, Per>& v,
      FormatContext& ctx) const -> decltype(ctx.out()) {
        const bool neg = v.d.count() < 0;
        const auto d = std::chrono::abs(v.d);
        const auto s = std::chrono::duration_cast<std::chrono::seconds>(d);
        return fmt::format_to(
          ctx.out(),
          FMT_STRING("{3}{0:%Q}.{1:0>{2}%Q}s"),
          s,
          d - s,
          ::human::num_digits<Per::den - 1, 10>,
          neg ? "-" : "");
    }
};

namespace human {

/// \brief Returns a view that formats a duration as seconds with decimal
/// fraction.
/// Number of fractional digits is exactly to support the duration scale:
/// 3 for milliseconds, 6 for microseconds, etc. Only subsecond scale is
/// supported. No format presentation types supported.
///
/// \example fmt::format("{}", human::seconds_decimal(356789ms));
template<class Rep, class Per>
auto seconds_decimal(const std::chrono::duration<Rep, Per>& d)
  -> detail::seconds_decimal_view<Rep, Per> {
    return detail::seconds_decimal_view<Rep, Per>{d};
}

} // namespace human
