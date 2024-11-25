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

#include "resource_mgmt/cpu_profiler.h"

#include "random/generators.h"
#include "resource_mgmt/logger.h"
#include "ssx/future-util.h"
#include "ssx/sformat.h"

#include <seastar/core/future.hh>
#include <seastar/core/internal/cpu_profiler.hh>
#include <seastar/core/reactor.hh>
#include <seastar/util/later.hh>

#include <iterator>

namespace resources {

cpu_profiler::cpu_profiler(
  config::binding<bool>&& enabled,
  config::binding<std::chrono::milliseconds>&& sample_period)
  : _query_timer([this] { poll_samples(); })
  , _enabled(std::move(enabled))
  , _sample_period(std::move(sample_period)) {
    _enabled.watch([this] { on_enabled_change(); });
    _sample_period.watch([this] { on_sample_period_change(); });

    for (size_t i = 0; i < number_of_results_buffers; i++) {
        _results_buffers.emplace_back(0, std::vector<ss::cpu_profiler_trace>{});
        _results_buffers.back().samples.reserve(ss::max_number_of_traces);
    }
}

ss::future<> cpu_profiler::start() {
    seastar::engine().set_cpu_profiler_period(_sample_period());
    seastar::engine().set_cpu_profiler_enabled(_enabled());

    if (_enabled()) {
        // Arm the timer to fire whenever the profiler collects
        // the maximum number of traces it can retain.
        _query_timer.arm_periodic(ss::max_number_of_traces * _sample_period());
    }

    return ss::now();
}

ss::future<> cpu_profiler::stop() {
    _query_timer.cancel();
    co_await _gate.close();
}

ss::future<std::vector<cpu_profiler::shard_samples>>
cpu_profiler::results(std::optional<ss::shard_id> shard_id) {
    if (_gate.is_closed()) {
        co_return std::vector<shard_samples>{};
    }
    auto holder = _gate.hold();

    std::vector<shard_samples> results{};

    if (shard_id) {
        auto shard_result = co_await container().invoke_on(
          shard_id.value(), [](auto& s) { return s.shard_results(); });

        results.emplace_back(
          shard_result.shard,
          shard_result.dropped_samples,
          std::move(shard_result.samples));
    } else {
        results = co_await container().map_reduce0(
          [](auto& s) { return s.shard_results(); },
          std::vector<shard_samples>{},
          [](std::vector<shard_samples> results, shard_samples shard_result) {
              results.emplace_back(
                shard_result.shard,
                shard_result.dropped_samples,
                std::move(shard_result.samples));
              return results;
          });
    }

    co_return results;
}

cpu_profiler::shard_samples cpu_profiler::shard_results() const {
    size_t dropped_samples = 0;
    absl::node_hash_map<ss::simple_backtrace, size_t> backtraces;
    for (auto& results_buffer : _results_buffers) {
        dropped_samples += results_buffer.dropped_samples;
        for (auto& result : results_buffer.samples) {
            backtraces[result.user_backtrace]++;
        }
    }

    std::vector<sample> results{};
    results.reserve(backtraces.size());

    for (auto& backtrace : backtraces) {
        results.emplace_back(
          ssx::sformat("{}", backtrace.first), backtrace.second);
    }

    return {ss::this_shard_id(), dropped_samples, results};
}

void cpu_profiler::poll_samples() {
    std::vector<ss::cpu_profiler_trace> results_buffer;
    if (_results_buffers.size() < number_of_results_buffers) {
        results_buffer = std::vector<ss::cpu_profiler_trace>{
          ss::max_number_of_traces};
    } else {
        results_buffer = std::move(_results_buffers.back().samples);
        _results_buffers.pop_back();
    }

    auto dropped_samples = ss::engine().profiler_results(results_buffer);

    resourceslog.trace(
      "Polled {} samples from the CPU profiler", results_buffer.size());

    _results_buffers.emplace_front(dropped_samples, std::move(results_buffer));
}

void cpu_profiler::on_enabled_change() {
    if (_gate.is_closed()) {
        return;
    }

    ss::engine().set_cpu_profiler_enabled(_enabled());
    _query_timer.cancel();

    if (_enabled()) {
        _query_timer.arm_periodic(ss::max_number_of_traces * _sample_period());
    }
}

void cpu_profiler::on_sample_period_change() {
    if (_gate.is_closed()) {
        return;
    }

    ss::engine().set_cpu_profiler_period(_sample_period());
}

} // namespace resources
