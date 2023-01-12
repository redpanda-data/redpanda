// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/this_fiber_id.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/perf_tests.hh>

PERF_TEST(fiber_local, test_this_fiber_id) {
    return ss::yield().then([]() -> ss::future<> {
        co_await ss::yield();
        co_await ss::yield();
        co_await ss::yield();
        perf_tests::start_measuring_time();
        auto id = ssx::this_fiber_id();
        perf_tests::stop_measuring_time();
        perf_tests::do_not_optimize(id);
    });
}

PERF_TEST(fiber_local, test_this_fiber_id_deep) {
    return ss::yield().then([]() -> ss::future<> {
        co_await ss::yield();
        co_await ss::yield();
        co_await ss::yield();
        co_await ss::yield();
        co_await ss::yield();
        co_await ss::yield();
        co_await ss::yield();
        co_await ss::yield();
        co_await ss::yield();
        co_await ss::yield();
        co_await ss::yield();
        co_await ss::yield();
        perf_tests::start_measuring_time();
        auto id = ssx::this_fiber_id();
        perf_tests::stop_measuring_time();
        perf_tests::do_not_optimize(id);
    });
}
