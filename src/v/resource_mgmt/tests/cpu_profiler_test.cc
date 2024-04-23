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

#include "config/configuration.h"
#include "config/property.h"
#include "resource_mgmt/cpu_profiler.h"

#include <seastar/core/future.hh>
#include <seastar/core/internal/cpu_profiler.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/thread.hh>
#include <seastar/core/timer.hh>
#include <seastar/coroutine/all.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <exception>
#include <optional>

namespace {
ss::future<> busy_loop(std::chrono::milliseconds duration) {
    auto end_time = ss::lowres_clock::now() + duration;
    while (ss::lowres_clock::now() < end_time) {
        // yield to allow timer to trigger and lowres_clock to update
        co_await ss::coroutine::maybe_yield();
    }
}
} // namespace

using namespace std::literals;

SEASTAR_THREAD_TEST_CASE(test_cpu_profiler) {
    resources::cpu_profiler cp(
      config::mock_binding(true), config::mock_binding(2ms));
    cp.start().get();

    // The profiler service will request samples from seastar every
    // 256ms since the sample rate is 2ms. So we need to be running
    // for at least that long to ensure the service pulls in samples.
    busy_loop(256ms + 10ms).get();

    auto results = cp.shard_results();
    BOOST_TEST(results.samples.size() >= 1);
}

SEASTAR_TEST_CASE(test_cpu_profiler_enable_override) {
    // Ensure that overrides to the profiler will enable it and collect samples
    // for the specified period of time.

    ss::sharded<resources::cpu_profiler> cp;
    co_await cp.start(config::mock_binding(false), config::mock_binding(2ms));
    co_await cp.invoke_on_all(&resources::cpu_profiler::start);

    auto wait_ms = 256ms + 10ms;
    auto [results] = co_await ss::coroutine::all(
      [&]() {
          return cp.local().collect_results_for_period(wait_ms, std::nullopt);
      },
      [&]() {
          return ss::smp::invoke_on_all([&]() { return busy_loop(wait_ms); });
      });

    for (auto& shard_results : results) {
        BOOST_TEST(shard_results.samples.size() >= 1);
    }

    // CPU profiler should be disabled so if we wait some time and collect the
    // samples again there should be no new samples.
    co_await busy_loop(wait_ms);

    auto new_results = cp.local().shard_results();
    auto old_results = results[ss::this_shard_id()];

    BOOST_REQUIRE(new_results.samples.size() == old_results.samples.size());

    for (int i = 0; i < old_results.samples.size(); i++) {
        BOOST_REQUIRE(
          new_results.samples[i].occurrences
          == old_results.samples[i].occurrences);
        BOOST_REQUIRE(
          new_results.samples[i].user_backtrace
          == old_results.samples[i].user_backtrace);
    }

    co_await cp.stop();
}

SEASTAR_TEST_CASE(test_cpu_profiler_enable_override_nested) {
    // Ensure that in cases with multiple on-going overrides that the shortest
    // override doesn't prematurely disable the profiler.

    ss::sharded<resources::cpu_profiler> cp;
    co_await cp.start(config::mock_binding(false), config::mock_binding(10ms));
    co_await cp.invoke_on_all(&resources::cpu_profiler::start);

    auto wait_ms = 1s;
    auto [results_p, results_10p] = co_await ss::coroutine::all(
      [&]() {
          return cp.local().collect_results_for_period(wait_ms, std::nullopt);
      },

      [&]() {
          return cp.local().collect_results_for_period(
            10 * wait_ms, std::nullopt);
      },
      [&]() { return busy_loop(10 * wait_ms); });

    auto samples_p = std::accumulate(
      results_p[ss::this_shard_id()].samples.begin(),
      results_p[ss::this_shard_id()].samples.end(),
      0,
      [](auto acc, auto& s) { return acc + s.occurrences; });
    auto samples_10p = std::accumulate(
      results_10p[ss::this_shard_id()].samples.begin(),
      results_10p[ss::this_shard_id()].samples.end(),
      0,
      [](auto acc, auto& s) { return acc + s.occurrences; });

    BOOST_REQUIRE_LT(samples_p, samples_10p);

    co_await cp.stop();
}

SEASTAR_TEST_CASE(test_cpu_profiler_enable_override_abort) {
    // Ensure that in cases that the profiler is stopped with on-going overrides
    // that those overrides do not delay the stop.

    ss::sharded<resources::cpu_profiler> cp;
    co_await cp.start(config::mock_binding(false), config::mock_binding(2ms));
    co_await cp.invoke_on_all(&resources::cpu_profiler::start);

    std::chrono::milliseconds wait_ms = 100min;
    auto res_fut = cp.local().collect_results_for_period(wait_ms, std::nullopt);

    auto start = ss::steady_clock_type::now();
    co_await cp.stop();
    auto end = ss::steady_clock_type::now();

    BOOST_REQUIRE(end - start < 1min);
}

SEASTAR_TEST_CASE(test_cpu_profiler_enable_override_filter_old_samples) {
    // Ensures that cpu_profiler::override_and_get_results doesn't return
    // samples collected before the function is called.

    std::chrono::milliseconds sample_rate = 1ms;
    auto one_poll_dur = ss::max_number_of_traces * sample_rate;

    ss::sharded<resources::cpu_profiler> cp;
    co_await cp.start(
      config::mock_binding(false),
      config::mock_binding<std::chrono::milliseconds>(sample_rate));
    co_await cp.invoke_on_all(&resources::cpu_profiler::start);

    auto wait_ms = 2 * one_poll_dur;
    auto [results] = co_await ss::coroutine::all(
      [&]() {
          return cp.local().collect_results_for_period(wait_ms, std::nullopt);
      },
      [&]() { return busy_loop(wait_ms); });

    BOOST_TEST(results[ss::this_shard_id()].samples.size() >= 1);

    ss::engine().set_cpu_profiler_period(std::chrono::seconds(10));

    co_await busy_loop(2 * one_poll_dur);

    wait_ms = 1ms;
    auto [override_results] = co_await ss::coroutine::all(
      [&]() {
          return cp.local().collect_results_for_period(wait_ms, std::nullopt);
      },
      [&]() { return busy_loop(wait_ms); });

    // Since we waited less then one poll duration no samples should of been
    // returned. If samples were returned then they must've come from the
    // previous override.
    BOOST_TEST(override_results[ss::this_shard_id()].samples.size() == 0);
}

SEASTAR_TEST_CASE(test_cpu_profiler_enable_override_sub_collection_period) {
    // Ensures that cpu_profiler::override_and_get_results returns results even
    // for periods shorter than `ss::max_number_of_traces * sample_rate`

    std::chrono::milliseconds sample_rate = 100ms;

    ss::sharded<resources::cpu_profiler> cp;
    co_await cp.start(
      config::mock_binding(false),
      config::mock_binding<std::chrono::milliseconds>(sample_rate));
    co_await cp.invoke_on_all(&resources::cpu_profiler::start);

    auto wait_ms = std::chrono::seconds(3);
    auto [results] = co_await ss::coroutine::all(
      [&]() {
          return cp.local().collect_results_for_period(wait_ms, std::nullopt);
      },
      [&]() { return busy_loop(wait_ms); });

    BOOST_TEST(results[ss::this_shard_id()].samples.size() >= 1);
}
