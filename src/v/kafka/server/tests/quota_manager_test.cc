// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"
#include "kafka/server/quota_manager.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <random>

using namespace std::chrono_literals;

const static auto client_id = "franz-go";

template<typename F>
ss::future<> run_quota_manager_test(F test_core) {
    kafka::quota_manager::client_quotas_t buckets_map;
    ss::sharded<kafka::quota_manager> sqm;
    co_await sqm.start(std::ref(buckets_map));
    co_await sqm.invoke_on_all(&kafka::quota_manager::start);

    // Run the core of the test now that everything's set up
    co_await ss::futurize_invoke(test_core, sqm);

    co_await sqm.stop();
}

template<typename F>
ss::future<> set_config(F update) {
    co_await ss::smp::invoke_on_all(
      [update{std::move(update)}]() { update(config::shard_local_cfg()); });
}

SEASTAR_THREAD_TEST_CASE(quota_manager_fetch_no_throttling) {
    set_config([](auto& conf) {
        conf.target_fetch_quota_byte_rate.set_value(std::nullopt);
    }).get();

    run_quota_manager_test(
      ss::coroutine::lambda(
        [](ss::sharded<kafka::quota_manager>& sqm) -> ss::future<> {
            auto& qm = sqm.local();

            // Test that if fetch throttling is disabled, we don't throttle
            co_await qm.record_fetch_tp(client_id, 10000000000000);
            auto delay = co_await qm.throttle_fetch_tp(client_id);

            BOOST_CHECK_EQUAL(0ms, delay);
        }))
      .get();
}

SEASTAR_THREAD_TEST_CASE(quota_manager_fetch_throttling) {
    set_config([](auto& conf) {
        conf.target_fetch_quota_byte_rate.set_value(100);
        conf.max_kafka_throttle_delay_ms.set_value(
          std::chrono::milliseconds::max());
    }).get();

    run_quota_manager_test(
      ss::coroutine::lambda(
        [](ss::sharded<kafka::quota_manager>& sqm) -> ss::future<> {
            auto& qm = sqm.local();

            // Test that below the fetch quota we don't throttle
            co_await qm.record_fetch_tp(client_id, 99);
            auto delay = co_await qm.throttle_fetch_tp(client_id);

            BOOST_CHECK_EQUAL(delay, 0ms);

            // Test that above the fetch quota we throttle
            co_await qm.record_fetch_tp(client_id, 10);
            delay = co_await qm.throttle_fetch_tp(client_id);

            BOOST_CHECK_GT(delay, 0ms);

            // Test that once we wait out the throttling delay, we don't
            // throttle again (as long as we stay under the limit)
            co_await seastar::sleep_abortable(delay + 1s);
            co_await qm.record_fetch_tp(client_id, 10);
            delay = co_await qm.throttle_fetch_tp(client_id);

            BOOST_CHECK_EQUAL(delay, 0ms);
        }))
      .get();
}

SEASTAR_THREAD_TEST_CASE(quota_manager_fetch_stress_test) {
    set_config([](config::configuration& conf) {
        conf.target_fetch_quota_byte_rate.set_value(100);
        conf.max_kafka_throttle_delay_ms.set_value(
          std::chrono::milliseconds::max());
    }).get();

    run_quota_manager_test(
      ss::coroutine::lambda(
        [](ss::sharded<kafka::quota_manager>& sqm) -> ss::future<> {
            // Exercise the quota manager from multiple cores to attempt to
            // discover segfaults caused by data races/use-after-free
            co_await sqm.invoke_on_all(ss::coroutine::lambda(
              [](kafka::quota_manager& qm) -> ss::future<> {
                  for (size_t i = 0; i < 1000; ++i) {
                      co_await qm.record_fetch_tp(client_id, 1);
                      auto delay [[maybe_unused]]
                      = co_await qm.throttle_fetch_tp(client_id);
                      co_await ss::maybe_yield();
                  }
              }));
        }))
      .get();
}
