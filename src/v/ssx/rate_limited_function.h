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

#pragma once

#include <seastar/core/lowres_clock.hh>

#include <functional>
#include <optional>
#include <type_traits>

namespace ssx {

/// A rate-limited function that caches the result of the function call
/// for a specified duration.
template<
  typename Signature,
  typename Clock = seastar::lowres_clock,
  typename Rate = Clock::duration>
class rate_limited_function;

template<typename Ret, bool Noexcept, typename Clock, typename Rate>
class rate_limited_function<Ret() noexcept(Noexcept), Clock, Rate> {
public:
    template<class Fn>
    rate_limited_function(Fn&& func, Rate rate)
      : _func(std::forward<Fn>(func))
      , _result(std::nullopt)
      , _rate(rate)
      , _last_call(Clock::now()) {}

    Ret operator()() const noexcept(Noexcept) {
        auto now = Clock::now();
        if (!is_ready(now)) {
            // Recompute if there is no cached value or the cached value
            // has expired.
            _last_call = now;
            _result = _func();
        }
        return *_result;
    }

private:
    // Returns true if the cached value is ready to be used.
    bool is_ready(Clock::time_point now = Clock::now()) const {
        auto rate = [](
                      this auto&& self,
                      auto&& rate) -> std::optional<typename Clock::duration> {
            if constexpr (std::is_invocable_v<decltype(rate)>) {
                return self(rate());
            } else {
                return rate;
            }
        }(_rate);

        if (!rate.has_value()) {
            // If rate is unset, use originally computed value.
            return _result.has_value();
        }

        return _result.has_value() && now <= (_last_call + rate.value());
    }

    std::function<Ret() noexcept(Noexcept)> _func;
    mutable std::optional<Ret> _result;
    Rate _rate;
    mutable Clock::time_point _last_call;
};

} // namespace ssx
