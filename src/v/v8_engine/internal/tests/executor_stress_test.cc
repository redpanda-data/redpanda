/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "seastarx.h"
#include "v8_engine/internal/executor.h"

#include <seastar/core/app-template.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/when_all.hh>
#include <seastar/testing/thread_test_case.hh>

#include <algorithm>
#include <chrono>
#include <unordered_map>

SEASTAR_THREAD_TEST_CASE(stress_test) {
    struct task_for_test {
        explicit task_for_test(char& is_finish)
          : _is_finish(is_finish) {}

        void operator()() { _is_finish = 1; }

        void cancel() {}

        void on_timeout() {}

        char& _is_finish;
    };

    std::vector<size_t> queue_size = {1, 10, 1000};

    for (auto size : queue_size) {
        v8_engine::executor test_executor(ss::engine().alien(), 1, size);

        std::unordered_map<size_t, char> finish_flags;
        std::vector<ss::future<>> futures;

        auto finish_time = ss::lowres_clock::now() + std::chrono::seconds(10);

        size_t it_count = 0;

        while (ss::lowres_clock::now() < finish_time) {
            if (it_count % 5 == 0) {
                char is_finish = 0;
                auto fut = test_executor.submit(
                  task_for_test(is_finish), std::chrono::milliseconds(100));
                fut.get();
                BOOST_REQUIRE_EQUAL(is_finish, 1);
            } else {
                finish_flags[it_count] = 0;
                futures.emplace_back(test_executor.submit(
                  task_for_test(finish_flags[it_count]),
                  std::chrono::milliseconds(5000)));
            }

            ++it_count;
        }

        auto futures_res = ss::when_all(futures.begin(), futures.end()).get();
        for (auto res : finish_flags) {
            BOOST_REQUIRE_EQUAL(res.second, 1);
        }

        test_executor.stop().get();
    }
}
