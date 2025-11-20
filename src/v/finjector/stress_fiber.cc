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

#include "finjector/stress_fiber.h"

#include "random/generators.h"
#include "ssx/future-util.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/timer.hh>

class stress_payload {
public:
    // Starts the stress payload with the given configuration.
    static std::unique_ptr<stress_payload> start(stress_config cfg) {
        return std::unique_ptr<stress_payload>(new stress_payload(cfg));
    }

    // Stops any existing stress fibers.
    ss::future<> stop();

private:
    explicit stress_payload(stress_config);

    // Runs a single fiber that spins the given number of times per scheduling
    // point, until an abort is requested.
    ss::future<> run_count_fiber(int depth, int min_count, int max_count);
    ss::future<> run_delay_fiber(int min_ms, int max_ms);

    ss::gate _gate;
    ss::abort_source _as;
};

stress_payload::stress_payload(stress_config cfg) {
    if (
      cfg.max_spins_per_scheduling_point.has_value()
      && cfg.min_spins_per_scheduling_point.has_value()) {
        for (size_t i = 0; i < cfg.num_fibers; i++) {
            ssx::spawn_with_gate(_gate, [cfg, this] {
                return run_count_fiber(
                  cfg.stack_depth.value_or(0),
                  cfg.min_spins_per_scheduling_point.value(),
                  cfg.max_spins_per_scheduling_point.value());
            });
        }
    }
    if (
      cfg.max_ms_per_scheduling_point.has_value()
      && cfg.min_ms_per_scheduling_point.has_value()) {
        for (size_t i = 0; i < cfg.num_fibers; i++) {
            ssx::spawn_with_gate(_gate, [cfg, this] {
                return run_delay_fiber(
                  cfg.min_ms_per_scheduling_point.value(),
                  cfg.max_ms_per_scheduling_point.value());
            });
        }
    }
}

/**
 * @brief Spins a given number of times at a given stack depth.
 *
 * The spinning function will have this many extra frames
 * added to its stack depth using recursive calls. Each frame is one
 * of two variants (corresponding to 0 or 1 for the template parameter).
 * The exact series of frames is set by the seed parameter: each unique seed has
 * a unique series of calls. This can help stress the CPU profiler by creating a
 * large number of unique stacks.
 * @param stack_depth
 * @param spins_per_scheduling_point number of spins before returning
 * @param seed the pattern of stack traces will be unique to the seed
 */
template<int VALUE>
[[clang::noinline]] void spinner(
  int stack_depth, int spins_per_scheduling_point, uint64_t seed) { // NOLINT
    volatile int spins = 0;
    if (stack_depth > 0) {
        auto to_call = seed & 1u ? spinner<1> : spinner<2>;
        to_call(stack_depth - 1, spins_per_scheduling_point, seed >> 1u);
        // the line below is prevent tailcall optimization would would otherwise
        // defeat our attempt to create many stack frames (we would make the
        // required number of calls but each call would simply replace the
        // current stack frame, not add a new one)
        spins = stack_depth;
    } else {
        while (true) {
            if (spins == spins_per_scheduling_point) {
                break;
            }
            spins = spins + 1;
        }
    }
}

ss::future<>
// NOLINTNEXTLINE
stress_payload::run_count_fiber(int depth, int min_count, int max_count) {
    uint64_t seed = 0;
    while (!_as.abort_requested()) {
        int spins_per_scheduling_point = min_count == max_count
                                           ? min_count
                                           : random_generators::get_int(
                                               min_count, max_count);
        co_await ss::maybe_yield();
        spinner<0>(depth, spins_per_scheduling_point, seed++);
    }
}

ss::future<> stress_payload::run_delay_fiber(int min_ms, int max_ms) {
    while (!_as.abort_requested()) {
        int ms_per_scheduling_point = min_ms == max_ms
                                        ? min_ms
                                        : random_generators::get_int(
                                            min_ms, max_ms);
        co_await ss::maybe_yield();
        const auto stop_time = ss::steady_clock_type::now()
                               + std::chrono::milliseconds(
                                 ms_per_scheduling_point);
        while (true) {
            if (ss::steady_clock_type::now() >= stop_time) {
                break;
            }
        }
    }
}

ss::future<> stress_payload::stop() {
    _as.request_abort();
    co_await _gate.close();
}

stress_fiber_manager::stress_fiber_manager()
  : _stress(nullptr) {}

stress_fiber_manager::~stress_fiber_manager() {}

bool stress_fiber_manager::start(stress_config cfg) {
    if (_stress) {
        return false;
    }
    _stress = stress_payload::start(cfg);
    return true;
}

ss::future<> stress_fiber_manager::stop() {
    if (_stress) {
        co_await _stress->stop();
        _stress.reset();
    }
}
