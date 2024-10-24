/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "test_utils/async.h"
#include "transform/memory_limiter.h"

#include <seastar/core/abort_source.hh>

#include <gtest/gtest.h>

namespace transform {
namespace {

// NOLINTBEGIN(*-magic-*)

TEST(MemoryLimiter, Works) {
    ss::abort_source as;
    memory_limiter limiter(10);
    limiter.acquire(3, &as).get();
    EXPECT_EQ(limiter.used_memory(), 3);
    EXPECT_EQ(limiter.available_memory(), 7);
    limiter.acquire(3, &as).get();
    EXPECT_EQ(limiter.used_memory(), 6);
    EXPECT_EQ(limiter.available_memory(), 4);
    limiter.acquire(3, &as).get();
    EXPECT_EQ(limiter.used_memory(), 9);
    EXPECT_EQ(limiter.available_memory(), 1);
    auto fut = limiter.acquire(3, &as);
    tests::drain_task_queue().get();
    EXPECT_FALSE(fut.available());
    limiter.release(1);
    tests::drain_task_queue().get();
    EXPECT_FALSE(fut.available());
    limiter.release(1);
    fut.get();
}

TEST(MemoryLimiter, IsFair) {
    ss::abort_source as;
    memory_limiter limiter(10);
    limiter.acquire(7, &as).get();
    auto fut1 = limiter.acquire(4, &as);
    tests::drain_task_queue().get();
    EXPECT_FALSE(fut1.available());
    auto fut2 = limiter.acquire(1, &as);
    tests::drain_task_queue().get();
    EXPECT_FALSE(fut1.available());
    EXPECT_FALSE(fut2.available());
    limiter.release(2);
    fut1.get();
    fut2.get();
}

TEST(MemoryLimiter, IsFairWithMany) {
    ss::abort_source as;
    memory_limiter limiter(10);
    std::vector<ss::future<>> acquired;
    acquired.reserve(100);
    for (int i = 0; i < 100; ++i) {
        acquired.push_back(limiter.acquire(10, &as));
    }
    for (int i = 0; i < 100; ++i) {
        tests::drain_task_queue().get();
        EXPECT_TRUE(acquired[i].available());
        for (int j = i + 1; j < 100; ++j) {
            EXPECT_FALSE(acquired[j].available());
        }
        acquired[i].get();
        limiter.release(10);
    }
}

TEST(MemoryLimiter, CanAbortWaiting) {
    ss::abort_source as;
    memory_limiter limiter(10);
    limiter.acquire(7, &as).get();
    auto fut = limiter.acquire(4, &as);
    as.request_abort();
    EXPECT_THROW(fut.get(), ss::abort_requested_exception);
    // Works with custom exceptions properly
    as = {};
    fut = limiter.acquire(4, &as);
    struct custom_exception : public std::exception {};
    as.request_abort_ex(custom_exception());
    EXPECT_THROW(fut.get(), custom_exception);
}

TEST(MemoryLimiter, CanAbortManyWaiting) {
    ss::abort_source as;
    memory_limiter limiter(10);
    limiter.acquire(10, &as).get();
    std::vector<ss::future<>> waiting;
    waiting.reserve(100);
    for (int i = 0; i < 100; ++i) {
        waiting.push_back(limiter.acquire(10, &as));
    }
    as.request_abort();
    for (auto& fut : waiting) {
        EXPECT_THROW(fut.get(), ss::abort_requested_exception);
    }
}

TEST(MemoryLimiter, AbortDoesNotRaceWithAcquire) {
    ss::abort_source as;
    memory_limiter limiter(10);
    limiter.acquire(10, &as).get();
    // Release then abort is successful.
    auto fut = limiter.acquire(1, &as);
    tests::drain_task_queue().get();
    EXPECT_FALSE(fut.available());
    limiter.release(1);
    as.request_abort();
    EXPECT_FALSE(fut.available());
    tests::drain_task_queue().get();
    EXPECT_NO_THROW(fut.get());
    EXPECT_EQ(limiter.used_memory(), 10);
    // Abort then release fails.
    as = {};
    fut = limiter.acquire(1, &as);
    tests::drain_task_queue().get();
    EXPECT_FALSE(fut.available());
    as.request_abort();
    limiter.release(1);
    EXPECT_THROW(fut.get(), ss::abort_requested_exception);
    EXPECT_EQ(limiter.used_memory(), 9);
}

TEST(MemoryLimiter, CannotReleaseExtra) {
    memory_limiter limiter(10);
    EXPECT_EQ(limiter.available_memory(), 10);
    limiter.release(10);
    EXPECT_EQ(limiter.available_memory(), 10);
}

TEST(MemoryLimiter, AlreadyAborted) {
    memory_limiter limiter(10);
    ss::abort_source as;
    as.request_abort();
    // The fast path doesn't check, which is fine
    EXPECT_NO_THROW(limiter.acquire(5, &as).get());
    // But a waiter always throws immediately
    EXPECT_THROW(limiter.acquire(6, &as).get(), ss::abort_requested_exception);
}

// NOLINTEND(*-magic-*)

} // namespace
} // namespace transform
