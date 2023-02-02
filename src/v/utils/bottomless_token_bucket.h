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
#include "seastar/core/lowres_clock.hh"
#include "seastarx.h"

#include <fmt/core.h>

#include <algorithm>
#include <chrono>
#include <limits>

/// muldiv calculates \p x * \p mul / \p div in integer domain avoiding both
/// integer overflow and loss of precision because of the intermediate result
/// being the same integer type as the arguments.
///
/// There are still limits for the values where overflow can kick in:
/// if \p x % \p div * \p mul fails to stay within type limits, the behaviour is
/// undefined. That may happen when
/// abs( \p mul * \p div ) >= numeric_limits<int64_t>::max()
inline int64_t
muldiv(const int64_t x, const int64_t mul, const int64_t div) noexcept {
    const auto qr = std::div(x, div);
    return qr.quot * mul + qr.rem * mul / div;
}

/// Variation of token bucket algorithm to allow bursty traffic.
/// Tokens represent resource units (e.g. bytes of traffic) whose usage is
/// controlled. Number of tokens in the bucket is capped by \ref quota *
/// \ref width at the top, and can go negative without a bound.
class bottomless_token_bucket {
public:
    /// Quota is measured in [tokens/s] and cannot be negative despite the
    /// signed type
    using quota_t = int64_t;  // [tokens/s]
    using tokens_t = int64_t; // [tokens]
    using clock = ss::lowres_clock;
    using time_res_t = std::chrono::milliseconds;

    // Algoritms rely on time_res_t to be subsecond
    static_assert(time_res_t::period::num == 1);

    // Maximum supported width of a bucket ~ 25 days
    static constexpr time_res_t max_width{std::numeric_limits<int32_t>::max()};

    // To avoid dynamic checking of acceptable width and quota maximums,
    // quota is limited in the way to avoid integer overflows in any condition.
    // This effectively maxes it to ~ 2 Ttokens/s. If that is ever found not
    // enough, this maximum should be raised to full int64_t, and both
    // set_quota() and set_width() should dynamically check that the calculated
    // burst does not overflow.
    static constexpr quota_t max_quota{
      static_cast<quota_t>(std::numeric_limits<int32_t>::max())
      * time_res_t::period::den};

    static constexpr quota_t min_quota{1};

    /// Token bucket is initialized full of tokens
    bottomless_token_bucket(
      const quota_t quota, const time_res_t width) noexcept {
        // do boundary checks on params and calculate _burst
        set_quota_impl(quota);
        set_width_impl(width);
        update_burst_tokens();
        refill(clock::time_point{});
    }

    void set_quota(const quota_t quota) noexcept {
        if (quota != _quota) {
            set_quota_impl(quota);
            update_burst_tokens();
        }
    }

    /// @post 0 < return value <= max_quota
    quota_t quota() const noexcept { return _quota; }

    void set_width(const time_res_t width) noexcept {
        if (width != _width) {
            set_width_impl(width);
            update_burst_tokens();
        }
    }

    /// Advance internal time and record usage of tokens
    /// @param n Number of tokens used
    /// @param now Time to record at
    void use(tokens_t n, clock::time_point now) noexcept;

    /// Advance internal time
    void refill(clock::time_point now) noexcept;

    /// Amount of tokens after the last use()/refill()
    tokens_t tokens() const noexcept { return _tokens; }

    /// Amount of tokens after the last use()/refill() expressed in [tokens/s],
    /// i.e. how much of the quota value is available after the last use.
    /// @post return value <= quota()
    quota_t get_current_rate() const noexcept;

private:
    void set_quota_impl(const quota_t quota) noexcept;
    void set_width_impl(const time_res_t width) noexcept;
    void update_burst_tokens() noexcept;

private:
    quota_t _quota{0};
    time_res_t _width;
    tokens_t _tokens{0};
    tokens_t _burst{0}; // capacity of the bucket
    clock::time_point _last_check{-max_width};

    friend struct fmt::formatter<bottomless_token_bucket>;
};

template<>
struct fmt::formatter<bottomless_token_bucket> {
    constexpr auto parse(fmt::format_parse_context& ctx) { return ctx.begin(); }
    template<typename Ctx>
    typename Ctx::iterator format(const bottomless_token_bucket&, Ctx&) const;
};
