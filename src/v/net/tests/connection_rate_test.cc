/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/seastarx.h"
#include "net/connection_rate.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <algorithm>
#include <chrono>
#include <unordered_map>

SEASTAR_THREAD_TEST_CASE(general_rate_test) {
    ss::gate gate;
    net::server_probe probe;

    const std::vector<int64_t> max_rates = {5, 10, 2};
    const int64_t max_threads = 50;

    net::connection_rate_info info{.max_connection_rate = 1};
    net::connection_rate<ss::manual_clock> connection_rate(
      info, gate, probe, 1000000ms);

    for (auto max_rate : max_rates) {
        connection_rate.update_general_rate(max_rate);
        ss::manual_clock::advance(std::chrono::seconds(2));
        ss::sleep(50ms).get();

        const int64_t one_connection_time_ms = std::max(1000 / max_rate, 1l);
        // In first second max_rate fibers can go in, so we should added it to
        // expected time
        const int64_t max_wait_s = ((max_threads - max_rate) / max_rate) * 1000;

        int64_t wait_time = 0;

        std::vector<ss::future<>> futures;

        ss::net::inet_address addr("127.0.0.1");

        for (auto i = 0; i < max_threads; ++i) {
            auto f = connection_rate.maybe_wait(addr).handle_exception(
              [](std::exception_ptr) {
                  // do notinhg
              });
            futures.emplace_back(std::move(f));
        }

        ss::sleep(50ms).get();

        while (
          !std::all_of(futures.begin(), futures.end(), [](ss::future<>& f) {
              return f.available();
          })) {
            ss::manual_clock::advance(
              std::chrono::milliseconds(one_connection_time_ms));
            ss::sleep(50ms).get();
            wait_time += one_connection_time_ms;
        }

        BOOST_CHECK(wait_time == max_wait_s);

        ss::manual_clock::advance(std::chrono::seconds(2));
        ss::sleep(50ms).get();
    }

    gate.close().get();
    connection_rate.stop();
}

SEASTAR_THREAD_TEST_CASE(overrides_rate_test) {
    ss::gate gate;
    net::server_probe probe;

    int64_t max_rate = 10;
    const int64_t max_threads = 50;

    std::vector<ss::sstring> overrides = {"127.0.0.2:5", "127.0.0.3:2"};

    struct override {
        int64_t max_rate;
        ss::net::inet_address addr;
        std::vector<ss::future<>> futures;
        int64_t wait_time_ms{0};
    };

    std::unordered_map<ss::sstring, override> overrides_map;
    overrides_map["127.0.0.1"] = {
      .max_rate = 10,
      .addr = ss::net::inet_address("127.0.0.1")}; // General rate;
    overrides_map["127.0.0.2"] = {
      .max_rate = 5, .addr = ss::net::inet_address("127.0.0.2")};
    overrides_map["127.0.0.3"] = {
      .max_rate = 2, .addr = ss::net::inet_address("127.0.0.3")};

    const int64_t min_one_connection_time_ms = std::max(1000 / max_rate, 1l);

    net::connection_rate_info info{
      .max_connection_rate = max_rate, .overrides = std::move(overrides)};
    net::connection_rate<ss::manual_clock> connection_rate(
      info, gate, probe, 1000000ms);

    for (auto i = 0; i < max_threads; ++i) {
        for (auto& override : overrides_map) {
            auto f = connection_rate.maybe_wait(override.second.addr)
                       .handle_exception([](std::exception_ptr) {
                           // do notinhg
                       });
            override.second.futures.emplace_back(std::move(f));
        }
    }

    ss::sleep(50ms).get();

    bool need_stop = false;
    while (!need_stop) {
        need_stop = true;
        for (auto& override : overrides_map) {
            if (std::all_of(
                  override.second.futures.begin(),
                  override.second.futures.end(),
                  [](ss::future<>& f) { return f.available(); })) {
                int64_t expected_time = ((max_threads
                                          - override.second.max_rate)
                                         / override.second.max_rate)
                                        * 1000;
                BOOST_CHECK(expected_time == override.second.wait_time_ms);
            } else {
                need_stop = false;
                override.second.wait_time_ms += min_one_connection_time_ms;
            }
        }

        ss::manual_clock::advance(
          std::chrono::milliseconds(min_one_connection_time_ms));
        ss::sleep(50ms).get();
    }

    ss::manual_clock::advance(std::chrono::seconds(2));
    ss::sleep(50ms).get();

    gate.close().get();
    connection_rate.stop();
}
