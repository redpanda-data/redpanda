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

#include "base/units.h"
#include "test_utils/async.h"
#include "transform/memory_limiter.h"
#include "transform/transfer_queue.h"

#include <seastar/core/abort_source.hh>

#include <gtest/gtest.h>

#include <optional>

namespace transform {
namespace {

struct entry {
    size_t mem;

    explicit entry(size_t mem)
      : mem(mem) {}
    entry(const entry&) = delete;
    entry& operator=(const entry&) = delete;
    entry(entry&&) = default;
    entry& operator=(entry&&) = default;

    size_t memory_usage() const { return mem; }
};

TEST(TransferQueue, IsFifo) {
    ss::abort_source as;
    memory_limiter ml(32_KiB);
    transfer_queue<entry> q(&ml);
    for (int i = 1; i <= 3; ++i) {
        q.push(entry{i * 1_KiB}, &as).get();
    }
    for (int i = 1; i <= 3; ++i) {
        auto entry = q.pop_one(&as).get();
        ASSERT_NE(entry, std::nullopt);
        EXPECT_EQ(entry->mem, i * 1_KiB);
    }
    auto fut = q.pop_one(&as);
    tests::drain_task_queue().get();
    EXPECT_FALSE(fut.available());
    as.request_abort();
    EXPECT_EQ(fut.get(), std::nullopt);
}

TEST(TransferQueue, IsBatchFifo) {
    ss::abort_source as;
    memory_limiter ml(32_KiB);
    transfer_queue<entry> q(&ml);
    for (int i = 1; i <= 3; ++i) {
        q.push(entry{i * 1_KiB}, &as).get();
    }
    auto entries = q.pop_all(&as).get();
    ASSERT_EQ(entries.size(), 3);
    for (int i = 1; i <= 3; ++i) {
        EXPECT_EQ(entries.front().mem, i * 1_KiB);
        entries.pop_front();
    }
}

TEST(TransferQueue, PopCanBeAborted) {
    ss::abort_source as;
    memory_limiter ml(2_KiB);
    transfer_queue<entry> q(&ml);
    auto fut = q.pop_all(&as);
    EXPECT_FALSE(fut.available());
    tests::drain_task_queue().get();
    EXPECT_FALSE(fut.available());
    as.request_abort();
    tests::drain_task_queue().get();
    EXPECT_TRUE(fut.available());
}

TEST(TransferQueue, LimitsMemoryUsage) {
    ss::abort_source as;
    memory_limiter ml(8);
    transfer_queue<entry> q(&ml);
    q.push(entry{4}, &as).get();
    q.push(entry{2}, &as).get();
    q.push(entry{1}, &as).get();
    q.push(entry{1}, &as).get();
    auto fut = q.push(entry{2}, &as);
    tests::drain_task_queue().get();
    EXPECT_FALSE(fut.available());
    q.pop_all(&as).get();
    fut.get();
    q.push(entry{6}, &as).get();
    fut = q.push(entry{1}, &as);
    tests::drain_task_queue().get();
    EXPECT_FALSE(fut.available());
    EXPECT_NE(q.pop_one(&as).get(), std::nullopt);
    fut.get();
}

TEST(TransferQueue, CanAlwaysPushAtLeastOneEntry) {
    ss::abort_source as;
    memory_limiter ml(1);
    transfer_queue<entry> q(&ml);
    q.push(entry{10}, &as).get();
    auto fut = q.push(entry{20}, &as);
    tests::drain_task_queue().get();
    EXPECT_FALSE(fut.available());
    q.pop_one(&as).get();
    fut.get();
}

TEST(TransferQueue, PushCanBeAborted) {
    ss::abort_source as;
    memory_limiter ml(1);
    transfer_queue<entry> q(&ml);
    q.push(entry{10}, &as).get();
    auto fut = q.push(entry{20}, &as);
    tests::drain_task_queue().get();
    EXPECT_FALSE(fut.available());
    as.request_abort();
    fut.get();
}

} // namespace
} // namespace transform
