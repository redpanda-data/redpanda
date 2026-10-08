/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/format_to.h"
#include "base/seastarx.h"
#include "base/vassert.h"
#include "ssx/future-util.h"
#include "utils/hdr_hist.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sleep.hh>

#include <algorithm>
#include <chrono>
#include <cstdint>

namespace tests {

/// Summary of the reactor-stall distribution observed by a
/// \ref reactor_stall_probe run.
struct reactor_stall_stats {
    // Number of stalls recorded into the histogram, i.e. those in the 1ms..1s
    // range; the percentiles are computed over this population. Stalls outside
    // that range are reflected only in `max`, the exact worst case over all
    // samples, which can therefore be non-zero even when `stall_samples` is 0.
    uint64_t stall_samples{0};
    std::chrono::microseconds p50{0};
    std::chrono::microseconds p90{0};
    std::chrono::microseconds p99{0};
    std::chrono::microseconds max{0};

    fmt::iterator format_to(fmt::iterator out) const {
        return fmt::format_to(
          out,
          "{{stall_samples: {}, p50: {}us, p90: {}us, p99: {}us, max: {}us}}",
          stall_samples,
          p50.count(),
          p90.count(),
          p99.count(),
          max.count());
    }
};

/// Measures reactor stalls on this shard for benchmarks (a "hiccup meter").
///
/// A background fiber sleeps for 1ms at a time and records how much longer than
/// that it took to wake; a busy reactor can't service the timer on time, so the
/// excess is the stall. \ref stop reports p50/p90/p99 (>= 1ms only) and the
/// exact max. Stalls under 1ms may be missed and caught ones may be
/// underestimated by up to 1ms; both are within the chosen resolution.
///
/// Cheap (~1000 wakeups/s) but keeps the shard awake. Single use, one per
/// shard. \ref stop must be awaited before destruction.
///
/// \code
///     tests::reactor_stall_probe probe;
///     probe.start();
///     // ... run the workload ...
///     vlog(logger.info, "reactor stalls: {}", co_await probe.stop());
/// \endcode
class reactor_stall_probe {
public:
    using clock = std::chrono::steady_clock;

    // Percentiles come from a histogram over the 1ms..1s range (microseconds)
    // with ~1% precision. Stalls outside that range are not recorded (see
    // run()). The exact worst case is tracked separately in `worst_` and
    // reported as `max`, so it is never clamped.
    reactor_stall_probe()
      : hist_(
          /*max_value=*/1'000'000, /*min=*/1'000, /*significant_figures=*/2) {}
    reactor_stall_probe(const reactor_stall_probe&) = delete;
    reactor_stall_probe& operator=(const reactor_stall_probe&) = delete;
    reactor_stall_probe(reactor_stall_probe&&) = delete;
    reactor_stall_probe& operator=(reactor_stall_probe&&) = delete;
    ~reactor_stall_probe() {
        vassert(
          state_ != state::running,
          "reactor_stall_probe destroyed while running; stop() must be awaited "
          "first");
    }

    /// Start sampling. May be called at most once.
    void start() {
        vassert(
          state_ == state::idle,
          "reactor_stall_probe::start() may only be called once, before "
          "stop()");
        state_ = state::running;
        // The first measurement window opens synchronously here:
        // spawn_with_gate invokes the lambda eagerly (futurize_invoke + eager
        // Seastar coroutines), so run() reaches its first `co_await ss::sleep`
        // before start() returns.
        ssx::spawn_with_gate(gate_, [this] { return run(); });
    }

    /// Stop sampling and return the observed stall distribution.
    ss::future<reactor_stall_stats> stop() {
        vassert(
          state_ == state::running,
          "reactor_stall_probe::stop() is only valid while running");
        state_ = state::stopped;
        co_await gate_.close();
        co_return snapshot();
    }

private:
    enum class state : uint8_t { idle, running, stopped };

    // Sleep interval between samples, and thus the smallest stall reliably
    // caught. Kept equal to the histogram's lower bound.
    static constexpr auto sample_period = std::chrono::milliseconds{1};

    // Histogram upper bound. hdr drops values above its max, so stalls larger
    // than this are not recorded; they surface only in `max`.
    static constexpr auto histogram_ceiling = std::chrono::seconds{1};

    reactor_stall_stats snapshot() const {
        auto max_us = std::chrono::duration_cast<std::chrono::microseconds>(
          worst_);
        if (stall_samples_ == 0) {
            // Nothing reached the 1ms floor, so get_value_at() would return the
            // bottom-bucket artifact; report only the exact worst case.
            return reactor_stall_stats{.max = max_us};
        }
        return reactor_stall_stats{
          .stall_samples = stall_samples_,
          .p50 = to_us(hist_.get_value_at(50.0)),
          .p90 = to_us(hist_.get_value_at(90.0)),
          .p99 = to_us(hist_.get_value_at(99.0)),
          .max = max_us,
        };
    }

    static std::chrono::microseconds to_us(int64_t v) {
        return std::chrono::microseconds{v};
    }

    ss::future<> run() {
        while (state_ == state::running) {
            auto before = clock::now();
            co_await ss::sleep(sample_period);
            auto elapsed = clock::now() - before;
            auto stall = elapsed > sample_period ? elapsed - sample_period
                                                 : clock::duration::zero();
            worst_ = std::max(worst_, stall);
            // Record (and count) only stalls within the histogram range
            // [sample_period, histogram_ceiling] == [1ms, 1s]. hdr drops values
            // above its max, so counting those would leave stall_samples_ out
            // of sync with the histogram and taint the percentiles; smaller
            // ones are below the resolution we asked for. Either way `worst_`
            // above still captures the exact value.
            if (stall >= sample_period && stall <= histogram_ceiling) {
                auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                            stall)
                            .count();
                hist_.record(static_cast<uint64_t>(us));
                ++stall_samples_;
            }
        }
    }

    ss::gate gate_;
    state state_{state::idle};
    clock::duration worst_{clock::duration::zero()};
    uint64_t stall_samples_{0};
    hdr_hist hist_;
};

} // namespace tests
