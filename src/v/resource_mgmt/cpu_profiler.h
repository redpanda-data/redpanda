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

#include "base/seastarx.h"
#include "config/property.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/internal/cpu_profiler.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/timer.hh>
#include <seastar/util/backtrace.hh>

#include <absl/container/node_hash_map.h>

#include <chrono>
#include <cstdint>
#include <deque>

namespace resources {

/**
 * The `cpu_profiler` service polls traces from seastar at fixed intervals.
 * It then aggregates the traces and provides methods for other services
 * to view them.
 */
class cpu_profiler : public ss::peering_sharded_service<cpu_profiler> {
    // The number of results buffers to retain. Each of these buffers
    // are `ss::max_number_of_traces` * 1568bytes in size (196KiB).
    // Each buffer has samples from a window of
    // `_sample_rate` * `ss::max_number_of_traces` milliseconds.
    //
    // At 10 results buffers with a 100ms sample rate this service
    // retains samples representative of the last 2 minutes of CPU
    // activity.
    static constexpr size_t number_of_results_buffers{10};

public:
    struct sample {
        ss::sstring user_backtrace;
        size_t occurrences;

        sample(ss::sstring ub, size_t o)
          : user_backtrace(std::move(ub))
          , occurrences(o) {}
    };

    struct shard_samples {
        ss::shard_id shard;
        size_t dropped_samples;
        std::vector<sample> samples;
    };

    cpu_profiler(
      config::binding<bool>&&, config::binding<std::chrono::milliseconds>&&);

    ss::future<> start();
    ss::future<> stop();

    // Collects `shard_results()` for each shard in a node and returns
    // them as a vector.
    ss::future<std::vector<shard_samples>> results(
      std::optional<ss::shard_id> shard_id,
      std::optional<ss::lowres_clock::time_point> filter_before = std::nullopt);

    // Returns the samples and dropped samples from the shard this function
    // is called on.
    //
    // `filter_before` will filter out any samples taken before a specified
    // time_point before from the returned samples.
    shard_samples shard_results(
      std::optional<ss::lowres_clock::time_point> filter_before
      = std::nullopt) const;

    // Enables the profiler for `timeout` milliseconds, then returns samples
    // collected during that time period.
    ss::future<std::vector<shard_samples>> collect_results_for_period(
      std::chrono::milliseconds timeout, std::optional<ss::shard_id> shard_id);

private:
    // impl for the above
    ss::future<>
    collect_results_for_period_impl(std::chrono::milliseconds timeout);

    // Used to poll seastar at set intervals to capture all samples
    ss::timer<ss::lowres_clock> _query_timer;
    ss::gate _gate;

    // Counts active overrides
    unsigned _override_enabled{0};
    // Used to abort sleeping `collect_results_for_period` tasks when stopping.
    ss::abort_source _as;

    // Configuration for seastar's cpu profiler
    config::binding<bool> _enabled;
    config::binding<std::chrono::milliseconds> _sample_period;

    struct profiler_result {
        size_t dropped_samples;
        std::vector<seastar::cpu_profiler_trace> samples;
        ss::lowres_clock::time_point polled_time;
    };

    // Buffers to copy sampled traces from seastar into so
    // we're not reallocating every call.
    // The oldest results buffer is overwritten when polling
    // for new samples from seastar.
    std::deque<profiler_result> _results_buffers;

    void poll_samples();

    void on_enabled_change();
    void on_sample_period_change();

    bool is_enabled() const;
};

} // namespace resources
