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
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/later.hh>

#include <chrono>
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
        _results_buffers.emplace_back(
          0,
          std::vector<ss::cpu_profiler_trace>{},
          ss::lowres_clock::time_point::min());
        _results_buffers.back().samples.reserve(ss::max_number_of_traces);
    }
}

ss::future<> cpu_profiler::start() {
    on_sample_period_change();
    on_enabled_change();

    return ss::now();
}

ss::future<> cpu_profiler::stop() {
    _as.request_abort();
    _query_timer.cancel();
    co_await _gate.close();
    ss::engine().set_cpu_profiler_enabled(false);
}

ss::future<std::vector<cpu_profiler::shard_samples>> cpu_profiler::results(
  std::optional<ss::shard_id> shard_id,
  std::optional<ss::lowres_clock::time_point> filter_before) {
    if (_gate.is_closed()) {
        co_return std::vector<shard_samples>{};
    }
    auto holder = _gate.hold();

    std::vector<shard_samples> results{};

    if (shard_id) {
        auto shard_result = co_await container().invoke_on(
          shard_id.value(),
          [filter_before](auto& s) { return s.shard_results(filter_before); });

        results.emplace_back(
          shard_result.shard,
          shard_result.dropped_samples,
          std::move(shard_result.samples));
    } else {
        results = co_await container().map_reduce0(
          [filter_before](auto& s) { return s.shard_results(filter_before); },
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

cpu_profiler::shard_samples cpu_profiler::shard_results(
  std::optional<ss::lowres_clock::time_point> filter_before) const {
    size_t dropped_samples = 0;
    absl::node_hash_map<ss::simple_backtrace, size_t> backtraces;
    for (auto& results_buffer : _results_buffers) {
        if (filter_before && results_buffer.polled_time < *filter_before) {
            continue;
        }

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

    _results_buffers.emplace_front(
      dropped_samples, std::move(results_buffer), ss::lowres_clock::now());
}

bool cpu_profiler::is_enabled() const {
    auto currently_overriden = _override_enabled > 0;
    return _enabled() || currently_overriden;
}

void cpu_profiler::on_enabled_change() {
    if (_gate.is_closed()) {
        return;
    }

    if (ss::engine().get_cpu_profiler_enabled() == is_enabled()) {
        return;
    }

    ss::engine().set_cpu_profiler_enabled(is_enabled());
    _query_timer.cancel();

    if (is_enabled()) {
        // Arm the timer to fire whenever the profiler collects
        // the maximum number of traces it can retain.
        _query_timer.arm_periodic(ss::max_number_of_traces * _sample_period());
    }
}

void cpu_profiler::on_sample_period_change() {
    if (_gate.is_closed()) {
        return;
    }

    if (ss::engine().get_cpu_profiler_period() == _sample_period()) {
        return;
    }

    ss::engine().set_cpu_profiler_period(_sample_period());

    if (is_enabled()) {
        // Arm the timer to fire whenever the profiler collects
        // the maximum number of traces it can retain.
        _query_timer.rearm_periodic(
          ss::max_number_of_traces * _sample_period());
    }
}

ss::future<> cpu_profiler::collect_results_for_period_impl(
  std::chrono::milliseconds timeout) {
    _override_enabled++;
    // enable the profiler if disabled pre-override.
    on_enabled_change();

    try {
        co_await ss::sleep_abortable(timeout, _as);
    } catch (const ss::sleep_aborted&) {
        resourceslog.debug("Sleep aborted while collecting CPU profile");
    }

    _override_enabled--;
    // check if profiler should be disabled post-override.
    on_enabled_change();

    // The timer to reap events from seastar only fires ~128 * sample period so
    // by default we would only collect samples every 13 seconds. To make sure
    // we get samples for something like wait_ms=5s we reap explicitly.
    poll_samples();
}

ss::future<std::vector<cpu_profiler::shard_samples>>
cpu_profiler::collect_results_for_period(
  std::chrono::milliseconds timeout, std::optional<ss::shard_id> shard_id) {
    if (_gate.is_closed()) {
        co_return std::vector<shard_samples>{};
    }
    auto holder = _gate.hold();

    auto polling_start_time = ss::lowres_clock::now();

    co_await container().invoke_on_all(
      [timeout](cpu_profiler& prof) -> ss::future<> {
          return prof.collect_results_for_period_impl(timeout);
      });

    co_return co_await results(shard_id, polling_start_time);
}

} // namespace resources
