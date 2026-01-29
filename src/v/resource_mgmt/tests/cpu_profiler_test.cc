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

#include "config/property.h"
#include "resource_mgmt/cpu_profiler.h"
#include "resource_mgmt/cpu_scheduling.h"

#include <seastar/core/future.hh>
#include <seastar/core/internal/cpu_profiler.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/timer.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/coroutine/switch_to.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>

using shard_samples = resources::cpu_profiler::shard_samples;
using sharded_profiler = ss::sharded<resources::cpu_profiler>;

namespace {
ss::future<> busy_loop(
  std::chrono::milliseconds duration,
  std::optional<ss::scheduling_group> sg = {}) {
    if (sg) {
        co_await ss::coroutine::switch_to(*sg);
    }

    auto end_time = ss::lowres_clock::now() + duration;
    while (ss::lowres_clock::now() < end_time) {
        // yield to allow timer to trigger and lowres_clock to update
        co_await ss::coroutine::maybe_yield();
    }
}

// Creates a sharded_profiler, and destroys it when this object
// is destructed (must be run in ss::thread).
struct cp_holder {
    std::unique_ptr<sharded_profiler> cp;

    cp_holder(bool start_now, std::chrono::milliseconds interval)
      : cp{std::make_unique<sharded_profiler>()} {
        cp->start(
            config::mock_binding(std::move(start_now)),
            config::mock_binding(std::move(interval)))
          .get();
        cp->invoke_on_all(&resources::cpu_profiler::start).get();
    }

    ~cp_holder() {
        if (cp) {
            cp->stop().get();
        }
    }
};

} // namespace

using namespace std::literals;

SEASTAR_THREAD_TEST_CASE(test_cpu_profiler) {
    resources::cpu_profiler cp(
      config::mock_binding(true), config::mock_binding(2ms));
    cp.start().get();
    auto stop = ss::defer([&] { cp.stop().get(); });

    // The profiler service will request samples from seastar every
    // 256ms since the sample rate is 2ms. So we need to be running
    // for at least that long to ensure the service pulls in samples.
    busy_loop(256ms + 10ms).get();

    auto results = cp.shard_results();
    BOOST_TEST(results.samples.size() >= 1);
}

SEASTAR_THREAD_TEST_CASE(test_cpu_scheduler_groups) {
    scheduling_groups& sg = scheduling_groups::instance();

    resources::cpu_profiler cp(
      config::mock_binding(true), config::mock_binding(2ms));
    cp.start().get();
    auto stop = ss::defer([&] { cp.stop().get(); });

    busy_loop(256ms + 10ms).get();

    auto results = cp.shard_results();
    BOOST_TEST(results.samples.size() >= 1);
    for (auto& r : results.samples) {
        BOOST_REQUIRE_EQUAL(r.sg, "main");
    }

    busy_loop(256ms + 10ms, sg.kafka_sg()).get();

    results = cp.shard_results();
    BOOST_TEST(results.samples.size() >= 1);
    int found_kafka = 0;
    for (auto& r : results.samples) {
        // we accept both main and kafka as some internal reactor work
        // will be recorded as main group
        BOOST_REQUIRE_MESSAGE(
          r.sg == "main" || r.sg == "kafka", "unexpected group: " << r.sg);
        found_kafka += r.sg == "kafka";
    }
    // should get at least some kafka!
    BOOST_REQUIRE(found_kafka > 0);
}

SEASTAR_THREAD_TEST_CASE(test_cpu_profiler_enable_override) {
    // Ensure that overrides to the profiler will enable it and collect samples
    // for the specified period of time.

    cp_holder holder(false, 2ms);
    ss::sharded<resources::cpu_profiler>& cp = *holder.cp;

    auto wait_ms = 256ms + 10ms;
    auto [results] = ss::when_all_succeed(
                       [&]() {
                           return cp.local().collect_results_for_period(
                             wait_ms, std::nullopt);
                       },
                       [&]() {
                           return ss::smp::invoke_on_all(
                             [&]() { return busy_loop(wait_ms); });
                       })
                       .get();

    BOOST_REQUIRE_EQUAL(ss::smp::count, results.size());

    for (ss::shard_id shard_id = 0; shard_id < ss::smp::count; ++shard_id) {
        auto& shard_results = results[shard_id];
        BOOST_TEST(shard_results.samples.size() >= 1);
        BOOST_REQUIRE_EQUAL(shard_id, shard_results.shard);
    }

    // CPU profiler should be disabled so if we wait some time and collect the
    // samples again there should be no new samples.
    busy_loop(wait_ms).get();

    auto new_results = cp.local().shard_results();
    auto old_results = results[ss::this_shard_id()];

    BOOST_REQUIRE_EQUAL(new_results.samples.size(), old_results.samples.size());

    for (size_t i = 0; i < old_results.samples.size(); i++) {
        BOOST_REQUIRE(
          new_results.samples[i].occurrences
          == old_results.samples[i].occurrences);
        BOOST_REQUIRE(
          new_results.samples[i].user_backtrace
          == old_results.samples[i].user_backtrace);
    }
}

SEASTAR_THREAD_TEST_CASE(test_cpu_profiler_enable_override_nested) {
    // Ensure that in cases with multiple on-going overrides that the shortest
    // override doesn't prematurely disable the profiler.

    cp_holder holder(false, 10ms);
    ss::sharded<resources::cpu_profiler>& cp = *holder.cp;

    auto wait_ms = 1s;
    auto [results_p, results_10p]
      = ss::when_all_succeed(
          [&]() {
              return cp.local().collect_results_for_period(
                wait_ms, std::nullopt);
          },
          [&]() {
              return cp.local().collect_results_for_period(
                10 * wait_ms, std::nullopt);
          },
          [&]() { return busy_loop(10 * wait_ms); })
          .get();

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
}

SEASTAR_THREAD_TEST_CASE(test_cpu_profiler_enable_override_abort) {
    // Ensure that in cases that the profiler is stopped with on-going overrides
    // that those overrides do not delay the stop.

    cp_holder holder(false, 2ms);
    ss::sharded<resources::cpu_profiler>& cp = *holder.cp;

    std::chrono::milliseconds wait_ms = 100min;
    auto res_fut = cp.local().collect_results_for_period(wait_ms, std::nullopt);

    auto start = ss::steady_clock_type::now();
    cp.stop().get();
    auto end = ss::steady_clock_type::now();

    BOOST_REQUIRE(end - start < 1min);
    res_fut.get();
}

SEASTAR_THREAD_TEST_CASE(test_cpu_profiler_enable_override_filter_old_samples) {
    // Ensures that cpu_profiler::override_and_get_results doesn't return
    // samples collected before the function is called.

    std::chrono::milliseconds sample_rate = 1ms;
    auto one_poll_dur = ss::max_number_of_traces * sample_rate;

    cp_holder holder(false, sample_rate);
    ss::sharded<resources::cpu_profiler>& cp = *holder.cp;

    auto wait_ms = 2 * one_poll_dur;
    auto [results] = ss::when_all_succeed(
                       [&]() {
                           return cp.local().collect_results_for_period(
                             wait_ms, std::nullopt);
                       },
                       [&]() { return busy_loop(wait_ms); })
                       .get();

    BOOST_TEST(results[ss::this_shard_id()].samples.size() >= 1);

    ss::engine().set_cpu_profiler_period(std::chrono::seconds(10));

    busy_loop(2 * one_poll_dur).get();

    wait_ms = 1ms;
    auto [override_results]
      = ss::when_all_succeed(
          [&]() {
              return cp.local().collect_results_for_period(
                wait_ms, std::nullopt);
          },
          [&]() { return busy_loop(wait_ms); })
          .get();

    // Since we waited less then one poll duration no samples should of been
    // returned. If samples were returned then they must've come from the
    // previous override.
    BOOST_TEST(override_results[ss::this_shard_id()].samples.size() == 0);
}

SEASTAR_THREAD_TEST_CASE(
  test_cpu_profiler_enable_override_sub_collection_period) {
    // Ensures that cpu_profiler::override_and_get_results returns results even
    // for periods shorter than `ss::max_number_of_traces * sample_rate`

    cp_holder holder(false, 100ms);
    ss::sharded<resources::cpu_profiler>& cp = *holder.cp;

    auto wait_ms = std::chrono::seconds(3);
    auto [results] = ss::when_all_succeed(
                       [&]() {
                           return cp.local().collect_results_for_period(
                             wait_ms, std::nullopt);
                       },
                       [&]() { return busy_loop(wait_ms); })
                       .get();

    BOOST_TEST(results[ss::this_shard_id()].samples.size() >= 1);
}
