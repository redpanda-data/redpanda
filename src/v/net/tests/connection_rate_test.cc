/*
 * Copyright 2022 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "net/connection_rate.h"
#include "seastarx.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/when_all.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <chrono>
#include <unordered_map>

namespace {

// In test we can get situation when one fiber succesfull go in
// semaphore.wait(), but increase counter for next_second
void check_rate(int64_t rate, int64_t prev_diff, int64_t max_rate) {
    BOOST_CHECK(rate <= max_rate + prev_diff);
}

} // namespace

SEASTAR_THREAD_TEST_CASE(rate_test) {
    ss::gate gate;

    const int64_t max_rate = 5;
    const int64_t max_wait_time_sec = 20;

    net::connection_rate_info info{.max_connection_rate = max_rate};
    net::connection_rate connection_rate(info, gate);

    std::vector<ss::future<>> futures;
    std::unordered_map<int64_t, int64_t> rate_counter;

    auto start = ss::lowres_clock::now();
    while (true) {
        auto current_time = ss::lowres_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(
          current_time - start);

        if (duration.count() > max_wait_time_sec) {
            break;
        }

        ss::net::inet_address addr("127.0.0.1");
        auto f = connection_rate.maybe_wait(addr)
                   .then([&rate_counter, start] {
                       auto current_time = ss::lowres_clock::now();
                       auto duration
                         = std::chrono::duration_cast<std::chrono::seconds>(
                           current_time - start);
                       rate_counter[duration.count()]++;
                   })
                   .handle_exception([](std::exception_ptr const&) {});

        futures.push_back(std::move(f));

        // We should move execution to seastar.
        if (futures.size() % 30 == 0) {
            ss::sleep(1ms).get();
        }
    }

    connection_rate.stop();
    gate.close().get();

    ss::when_all(futures.begin(), futures.end()).get();

    auto diff = 0;
    for (const auto& [second, rate] : rate_counter) {
        // For zero second we have max_rate tokens, and will add new each
        // 1000ms/max_rate, so for seconds with max_rate tokens we can expect
        // max_rate * 2 connections
        if (second == 0) {
            check_rate(rate, diff, 2 * max_rate);
            diff = 2 * max_rate - rate;
        } else {
            check_rate(rate, diff, max_rate);
            diff = max_rate - rate;
        }
    }
}

SEASTAR_THREAD_TEST_CASE(update_rate_test) {
    ss::gate gate;
    net::connection_rate_info info{.max_connection_rate = 1};
    net::connection_rate connection_rate(info, gate);
    int64_t max_wait_time_sec = 10;

    std::vector<int64_t> rate_counter(40, 0);

    struct rate_test_t {
        int64_t max_rate;
        std::vector<ss::future<>> futures;
        ss::lowres_clock::time_point updating_rate_time;
    };

    std::vector<rate_test_t> rates(3);
    rates[0].max_rate = 10;
    rates[1].max_rate = 20;
    rates[2].max_rate = 5;

    auto start = ss::lowres_clock::now();

    for (auto& rate : rates) {
        connection_rate.update_general_rate(rate.max_rate);

        rate.updating_rate_time = ss::lowres_clock::now();
        while (true) {
            auto current_time = ss::lowres_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::seconds>(
              current_time - rate.updating_rate_time);

            if (duration.count() >= max_wait_time_sec) {
                break;
            }

            ss::net::inet_address addr("127.0.0.1");
            auto f = connection_rate.maybe_wait(addr)
                       .then([&rate_counter, start] {
                           auto current_time = ss::lowres_clock::now();
                           auto duration
                             = std::chrono::duration_cast<std::chrono::seconds>(
                               current_time - start);
                           rate_counter[duration.count()]++;
                       })
                       .handle_exception([](std::exception_ptr const&) {});

            rate.futures.push_back(std::move(f));

            // We should move execution to seastar.
            if (rate.futures.size() % 30 == 0) {
                ss::sleep(1ms).get();
            }
        }
    }

    connection_rate.stop();
    gate.close().get();

    for (auto& rate_info : rates) {
        ss::when_all(rate_info.futures.begin(), rate_info.futures.end()).get();
    }

    size_t current_index = 0;
    auto max_rate = rates[0].max_rate;
    auto change_rate_time = rates[0].updating_rate_time;
    int64_t diff = 0;
    for (auto second = 0; second < rate_counter.size(); ++second) {
        auto rate = rate_counter[second];
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(
          rates[current_index].updating_rate_time - start);
        if (second >= duration.count() + max_wait_time_sec) {
            current_index = std::min(++current_index, rates.size() - 1);
            max_rate = rates[current_index].max_rate;
            change_rate_time = rates[current_index].updating_rate_time;
        }
        if (
          std::chrono::duration_cast<std::chrono::seconds>(
            change_rate_time - start)
            .count()
          == second) {
            check_rate(rate, diff, max_rate * 2);
            diff = max_rate * 2 - rate;
        } else {
            check_rate(rate, diff, max_rate);
            diff = max_rate - rate;
        }
    }
}
