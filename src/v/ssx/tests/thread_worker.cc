// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/thread_worker.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/smp.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/later.hh>

#include <absl/algorithm/container.h>
#include <boost/test/unit_test_log.hpp>

struct move_only {
    explicit move_only(size_t v)
      : value(v) {}
    ~move_only() = default;
    move_only(move_only&&) = default;
    move_only(move_only const&) = delete;
    move_only& operator=(move_only&&) = default;
    move_only& operator=(move_only const&) = delete;
    size_t value;
};

template<size_t tries, size_t stop_at>
auto thread_worker_test() {
    BOOST_REQUIRE_GT(ss::smp::count, 1);

    auto w = ssx::thread_worker{};
    w.start().get();

    std::vector<std::vector<ss::future<move_only>>> all_results(ss::smp::count);

    ss::smp::invoke_on_all([&w, &all_results]() {
        auto& results = all_results[ss::this_shard_id()];
        results.reserve(tries);
        for (size_t i = 0; i < tries; ++i) {
            results.emplace_back(w.submit([i]() { return move_only(i); }));
        }
    }).get();

    BOOST_REQUIRE(absl::c_all_of(
      all_results, [](const auto& c) { return c.size() == tries; }));

    ss::smp::invoke_on_all([&w, &all_results]() {
        return [](auto& w, auto& all_results) -> ss::future<> {
            bool is_worker_shard = w.shard_id == ss::this_shard_id();
            auto& results = all_results[ss::this_shard_id()];
            for (size_t i = 0; i < tries; ++i) {
                if (i == stop_at && is_worker_shard) {
                    co_await w.stop();
                }
                auto result = co_await std::move(results[i])
                                .handle_exception_type(
                                  [i](const seastar::gate_closed_exception&) {
                                      return move_only(i);
                                  });
                BOOST_REQUIRE_EQUAL(result.value, i);
            }
        }(w, all_results);
    }).get();

    if (stop_at >= tries) {
        w.stop().get();
    }
}

SEASTAR_THREAD_TEST_CASE(thread_worker_single_cancel_after) {
    // cancel thread worker at the after all of the gets
    thread_worker_test<1, 1>();
}

SEASTAR_THREAD_TEST_CASE(thread_worker_many_cancel_after) {
    // cancel thread worker at the after all of the gets
    thread_worker_test<128, 128>();
}

SEASTAR_THREAD_TEST_CASE(thread_worker_many_cancel_middle) {
    // cancel thread worker half way through the gets
    thread_worker_test<128, 64>();
}
