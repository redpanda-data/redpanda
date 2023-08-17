// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "seastarx.h"
#include "test_utils/test.h"
#include "wasm/work_queue.h"

#include <seastar/core/future.hh>
#include <seastar/core/when_all.hh>

#include <exception>
#include <stdexcept>
#include <vector>

using namespace std::chrono_literals;

// NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)

TEST(ThreadedWorkQueue, CanEnqueueTasks) {
    wasm::threaded_work_queue q;
    q.start().get();
    int a = q.enqueue<int>([]() { return 1; }).get();
    ASSERT_EQ(a, 1);
    auto [b, c, d, e] = ss::when_all_succeed(
                          q.enqueue<int>([]() { return 1; }),
                          q.enqueue<int>([]() { return 2; }),
                          q.enqueue<int>([]() { return 3; }),
                          q.enqueue<int>([]() { return 4; }))
                          .get();
    ASSERT_EQ(b, 1);
    ASSERT_EQ(c, 2);
    ASSERT_EQ(d, 3);
    ASSERT_EQ(e, 4);
    q.stop().get();
}

TEST(ThreadedWorkQueue, CanBeRestarted) {
    wasm::threaded_work_queue q;
    for (int i = 0; i < 128; ++i) {
        q.start().get();
        auto [a, stop, b] = ss::when_all(
                              q.enqueue<int>([]() { return 1; }),
                              q.stop(),
                              q.enqueue<int>([]() { return 1; }))
                              .get();
        // These are valid to race with stopping the queue are OK to fail
        a.ignore_ready_future();
        b.ignore_ready_future();
        ASSERT_FALSE(stop.failed());
        stop.get();
    }
}

// NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
