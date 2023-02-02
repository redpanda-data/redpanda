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
#include "utils/bottomless_token_bucket.h"

#include "likely.h"
#include "vassert.h"

#include <fmt/chrono.h>

void bottomless_token_bucket::use(
  const tokens_t n, const clock::time_point now) noexcept {
    vassert(n >= 0, "Cannot use negative number of tokens");
    refill(now);
    _tokens -= n;
}

void bottomless_token_bucket::set_quota_impl(const quota_t quota) noexcept {
    vassert(
      quota >= min_quota, "Token bucket quota must be positive ({})", quota);
    vassert(
      quota <= max_quota,
      "Token bucket quota too large ({} > {})",
      quota,
      max_quota);
    _quota = quota;
}

void bottomless_token_bucket::set_width_impl(const time_res_t width) noexcept {
    vassert(
      width.count() > 0,
      "Token bucket width must be positive ({})",
      width.count());
    vassert(
      width <= max_width,
      "Token bucket width too large ({} > {})",
      width.count(),
      max_width.count());
    _width = width;
}

void bottomless_token_bucket::update_burst_tokens() noexcept {
    _burst = muldiv(_quota, _width.count(), time_res_t::period::den);
    _tokens = std::min(_burst, _tokens);
}

bottomless_token_bucket::quota_t
bottomless_token_bucket::get_current_rate() const noexcept {
    return muldiv(_tokens, time_res_t::period::den, _width.count());
}

void bottomless_token_bucket::refill(const clock::time_point now) noexcept {
    const auto delta = std::chrono::duration_cast<time_res_t>(
      now - _last_check);
    _last_check = now;
    if (unlikely(delta >= max_width)) {
        // avoid overflow on multiplication - the product will be greater
        // than _burst anyway
        _tokens = _burst;
    } else {
        const tokens_t add_tokens = muldiv(
          _quota, delta.count(), time_res_t::period::den);
        if (add_tokens >= _burst) {
            _tokens = _burst;
        } else {
            _tokens = std::min<tokens_t>(_burst, _tokens + add_tokens);
        }
    }
}

template<>
typename fmt::basic_format_context<fmt::appender, char>::iterator
fmt::formatter<bottomless_token_bucket, char, void>::format<
  fmt::basic_format_context<fmt::appender, char>>(
  const bottomless_token_bucket& v,
  fmt::basic_format_context<fmt::appender, char>& ctx) const {
    return fmt::format_to(
      ctx.out(),
      "{{quota:{}, width:{}, tokens:{}, burst:{}, last_check:{:%S}}}",
      v._quota,
      v._width.count(),
      v._tokens,
      v._burst,
      // time_since_epoch() here has no meaning on its own,
      // we just care about seeing this value change
      v._last_check.time_since_epoch());
}
