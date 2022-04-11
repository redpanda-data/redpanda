// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/api_versions.h"
#include "kafka/server/quota_manager.h"
#include "random/generators.h"
#include "redpanda/tests/fixture.h"
#include "resource_mgmt/rate.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>

using namespace std::chrono_literals;

void set_quota_manager_test_defaults(
  int16_t num_windows,
  std::chrono::milliseconds window_size,
  uint32_t quota_rate,
  std::chrono::milliseconds max_delay) {
    /// Configure quota manager with known defaults otherwise if assumptions are
    /// made on tests defaults values obtained from config settings, test would
    /// fail if these defaults are later changed
    auto& cfg = config::shard_local_cfg();
    cfg.get("default_num_windows").set_value(num_windows);
    cfg.get("default_window_sec").set_value(window_size);
    cfg.get("target_quota_time_rate")
      .set_value(std::optional<uint32_t>(quota_rate));
    cfg.get("max_kafka_throttle_delay_ms").set_value(max_delay);
}

/// This test emmulates a single client making frequent requests to
/// redpanda. Each request is given a random time interval (between 1-10ms),
/// logged in the quota manager and tracked by a local instance of
/// rate_tracker
SEASTAR_THREAD_TEST_CASE(quota_manager_delay_rate_test) {
    const auto num_windows = 10;
    const auto window_size = 1000ms;
    const auto quota_rate = 10;
    const auto max_delay = 200ms;
    set_quota_manager_test_defaults(
      num_windows, window_size, quota_rate, max_delay);
    const auto cid = "client_id_foo_1";
    const auto target_rate = window_size * quota_rate * 0.01;
    auto window_start = ss::lowres_clock::now();

    /// If the local rate tracker determines the rate is higher then the static
    /// preconfigured rate, check the value against the quota managers
    rate_tracker rt(num_windows, window_size);
    kafka::quota_manager qmgr;
    for (auto i = 0; i < 100; ++i) {
        for (auto j = 0; j < 100; ++j) {
            /// Every iteration of j will take more time slices from the quota
            /// each time taking another 1-10ms
            const auto used = std::chrono::milliseconds(
              random_generators::get_int(1, 10));
            const auto next = window_start + used;
            qmgr.record_time_quota(cid, window_start, next);
            rt.record(used.count(), next);
            auto rate = std::chrono::milliseconds(
              static_cast<uint64_t>(rt.measure(next)));
            /// Pass 0 to the bytes parameter to avoid the quota manager
            /// calculating and returning a throttle by byte amount
            auto observed_rate = qmgr.record_tp_and_throttle(cid, 0, next);
            if (rate > target_rate) {
                rate = std::chrono::duration_cast<std::chrono::milliseconds>(
                  rate - target_rate);
                BOOST_CHECK_EQUAL(
                  std::min(rate, max_delay), observed_rate.duration);
            } else {
                BOOST_CHECK_EQUAL(0ms, observed_rate.duration);
            }
            /// Incrementing this parameter may push the rate tracker to open a
            /// new window
            window_start += used;
        }
    }
}

FIXTURE_TEST(quota_manager_e2e_delay_rate, redpanda_thread_fixture) {
    /// This test has very low quota limits to try to trigger the quota by rate
    /// mechanism. Local testing has shown that the mechanism can be encountered
    /// with these parameters.
    const auto num_windows = 1;
    const auto window_size = 10ms;
    const auto quota_rate = 1;
    /// Should never be encountered because the maximum throttle by rate is 1
    /// window side, or 10ms in this test
    const auto max_delay = 200ms;
    set_quota_manager_test_defaults(
      num_windows, window_size, quota_rate, max_delay);

    /// Make 100 concurrent api_versions requests
    auto r = boost::irange<int>(0, 100);
    ss::parallel_for_each(r, [&](int) -> ss::future<> {
        auto client = co_await make_kafka_client();
        co_await client.connect();
        auto response = co_await client.dispatch(
          kafka::api_versions_request{.data{
            .client_software_name = "redpanda",
            .client_software_version = "x.x"}},
          kafka::api_version(3));
        /// Check that if throttling did occur, it was less than or equal to one
        /// window length
        BOOST_CHECK_LE(response.data.throttle_time_ms, window_size);
        if (response.data.throttle_time_ms > 0ms) {
            std::cout << "Throttle time ms: " << response.data.throttle_time_ms
                      << std::endl;
        }
        co_await client.stop();
    }).get();
}
