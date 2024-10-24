// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "base/seastarx.h"
#include "gmock/gmock.h"
#include "ssx/work_queue.h"

#include <seastar/core/future.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/later.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <exception>
#include <stdexcept>
#include <vector>

namespace ssx {

using namespace std::chrono_literals;
using ::testing::ElementsAre;
using ::testing::IsEmpty;

namespace {
ss::future<> submit_delayed(
  work_queue* q,
  ss::manual_clock::duration d,
  ss::noncopyable_function<void()> fn) {
    ss::promise<> p;
    q->submit_delayed<ss::manual_clock>(d, [&] {
        fn();
        p.set_value();
        return ss::now();
    });
    co_await p.get_future();
}
ss::future<> submit(work_queue* q, ss::noncopyable_function<void()> fn) {
    ss::promise<> p;
    q->submit([&] {
        fn();
        p.set_value();
        return ss::now();
    });
    co_await p.get_future();
}
} // namespace

TEST(WorkQueue, Order) {
    work_queue queue([](auto ex) { std::rethrow_exception(ex); });
    // Add a task that isn't ready to the queue, so that in release mode,
    // seastar doesn't run it inline without adding it to the scheduler. These
    // tests rely on the callbacks not executing immedately.
    queue.submit([] { return ss::yield(); });

    std::vector<int> values;

    queue.submit([&] {
        values.push_back(1);
        return ss::now();
    });
    queue.submit([&] {
        values.push_back(2);
        return ss::now();
    });
    auto done = submit(&queue, [&] { values.push_back(3); });

    EXPECT_THAT(values, IsEmpty());
    done.get();
    EXPECT_THAT(values, ElementsAre(1, 2, 3));
    queue.shutdown().get();
}

TEST(WorkQueue, FailuresDoNotStopTheQueue) {
    int error_count = 0;
    work_queue queue([&error_count](auto) { ++error_count; });
    queue.submit([] { return ss::yield(); });

    std::vector<int> values;

    queue.submit([&] {
        values.push_back(1);
        return ss::now();
    });
    queue.submit(
      [&]() -> ss::future<> { throw std::runtime_error("oh noes!"); });
    auto done = submit(&queue, [&] { values.push_back(3); });

    EXPECT_THAT(values, IsEmpty());
    EXPECT_EQ(error_count, 0);
    done.get();
    EXPECT_EQ(error_count, 1);
    EXPECT_THAT(values, ElementsAre(1, 3));
    queue.shutdown().get();
}

TEST(WorkQueue, ShutdownStopsTheQueueImmediately) {
    work_queue queue([](auto ex) { std::rethrow_exception(ex); });
    queue.submit([] { return ss::yield(); });
    int a = 1;
    queue.submit([&] {
        a = 2;
        return ss::now();
    });
    queue.shutdown().get();
    EXPECT_EQ(a, 1);
}

TEST(WorkQueue, CanSubmitDelayedTasks) {
    work_queue queue([](auto ex) { std::rethrow_exception(ex); });
    int a = 1;
    ss::promise<> p1;
    auto f1 = submit_delayed(&queue, 1s, [&] { a = 2; });
    auto f2 = submit(&queue, [&] { a = 3; });
    f2.get();
    EXPECT_FALSE(f1.available());
    EXPECT_EQ(a, 3);
    ss::manual_clock::advance(1s);
    f1.get();
    EXPECT_EQ(a, 2);
    queue.shutdown().get();
}

TEST(WorkQueue, CanShutdownWithDelayedTasks) {
    work_queue queue([](auto ex) { std::rethrow_exception(ex); });
    auto f1 = submit_delayed(&queue, 1s, [&] {});
    bool a2 = false;
    queue.submit_delayed<ss::manual_clock>(2s, [&] {
        a2 = true;
        return ss::now();
    });
    EXPECT_FALSE(f1.available());
    EXPECT_FALSE(a2);
    ss::manual_clock::advance(1s);
    f1.get();
    EXPECT_FALSE(a2);
    queue.shutdown().get();
    ss::manual_clock::advance(3s);
    ss::now().get();
    EXPECT_FALSE(a2);
}

TEST(WorkQueue, RecursiveSubmit) {
    work_queue queue([](auto ex) { std::rethrow_exception(ex); });
    ss::promise<> p;
    queue.submit([&] {
        queue.submit([&] {
            queue.submit([&] {
                p.set_value();
                return ss::now();
            });
            return ss::now();
        });
        return ss::now();
    });
    p.get_future().get();
    queue.shutdown().get();
}

} // namespace ssx
