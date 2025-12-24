// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/retry_loop.h"

#include "ssx/context_sleep.h"

#include <random>

namespace utils {

retry_loop_state::retry_loop_state(retry_loop_config cfg)
  : cfg_(cfg)
  , current_(cfg.initial_delay) {}

seastar::future<std::optional<uint16_t>>
retry_loop_state::next(context_ref ctx) {
    if (attempt_ >= cfg_.max_attempts || ctx.is_cancelled()) {
        return seastar::make_ready_future<std::optional<uint16_t>>(
          std::nullopt);
    }

    if (attempt_ == 0) {
        return seastar::make_ready_future<std::optional<uint16_t>>(attempt_++);
    }

    auto delay = apply_jitter(current_);
    current_ = std::min(
      context::duration{static_cast<context::duration::rep>(
        current_.count() * cfg_.multiplier)},
      cfg_.max_delay);

    if (delay > ctx.time_left()) {
        return seastar::make_ready_future<std::optional<uint16_t>>(
          std::nullopt);
    }

    return ssx::sleep(ctx, delay)
      .then([this] { return std::make_optional(attempt_++); })
      .handle_exception_type([](const ssx::context_sleep_aborted&) {
          return std::optional<uint16_t>{std::nullopt};
      });
}

context::duration retry_loop_state::apply_jitter(context::duration d) const {
    if (cfg_.jitter <= 0.0) {
        return d;
    }
    static thread_local std::minstd_rand rng{std::random_device{}()};
    std::uniform_real_distribution<double> dist(
      1.0 - cfg_.jitter, 1.0 + cfg_.jitter);
    return context::duration{
      static_cast<context::duration::rep>(d.count() * dist(rng))};
}

} // namespace utils
