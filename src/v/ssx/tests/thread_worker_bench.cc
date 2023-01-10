// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "seastarx.h"
#include "ssx/thread_worker.h"

#include <seastar/testing/perf_tests.hh>

ss::future<> run_test(size_t data_size) {
    auto w = ssx::thread_worker{};
    co_await w.start();

    std::vector<ss::future<size_t>> vec;
    vec.reserve(data_size);

    perf_tests::start_measuring_time();
    for (size_t i = 0; i < data_size; ++i) {
        vec.push_back(w.submit([i]() { return i; }));
    }

    for (size_t i = 0; i < data_size; ++i) {
        auto val = co_await std::move(vec[i]);
        vassert(val == i, "Failed");
        perf_tests::do_not_optimize(val);
    }
    perf_tests::stop_measuring_time();
    co_await w.stop();
}

struct thread_worker_test {};
PERF_TEST_C(thread_worker_test, 1) { co_return co_await run_test(1); }
PERF_TEST_C(thread_worker_test, 10) { co_return co_await run_test(10); }
PERF_TEST_C(thread_worker_test, 100) { co_return co_await run_test(100); }
PERF_TEST_C(thread_worker_test, 1000) { co_return co_await run_test(1000); }
