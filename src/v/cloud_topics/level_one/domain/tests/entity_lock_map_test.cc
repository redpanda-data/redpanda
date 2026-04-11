/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_one/domain/entity_lock_map.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>

#include <gtest/gtest.h>

using namespace cloud_topics::l1;

// Two acquires on the same key: the second blocks until the first releases.
// The map entry is created on first acquire and cleaned up after both release.
TEST_CORO(EntityLockMapTest, ContentionAndCleanup) {
    entity_lock_map<int> map{"test"};
    EXPECT_EQ(map.size(), 0);

    auto units1 = co_await map.acquire(42);
    EXPECT_EQ(map.size(), 1);

    auto fut2 = map.acquire(42);
    EXPECT_FALSE(fut2.available());
    EXPECT_EQ(map.size(), 1);

    // Release the first — unblocks the second. Entry persists (waiter
    // still references it).
    {
        [[maybe_unused]] auto released = std::move(units1);
    }
    EXPECT_EQ(map.size(), 1);

    auto units2 = co_await std::move(fut2);
    EXPECT_EQ(map.size(), 1);

    // Release the second — entry is cleaned up. Verify by re-acquiring
    // (creates a fresh entry) and checking size returns to 0.
    {
        [[maybe_unused]] auto released = std::move(units2);
    }
    auto units3 = co_await map.acquire(42);
    EXPECT_EQ(map.size(), 1);
    {
        [[maybe_unused]] auto released = std::move(units3);
    }
    EXPECT_EQ(map.size(), 0);
}

// If a mutex is broken while a waiter is pending, the waiter throws and
// the active refcount is cleaned up (not leaked).
TEST_CORO(EntityLockMapTest, BrokenMutexCleansUpActiveCount) {
    entity_lock_map<int> map{"test"};

    auto units = co_await map.acquire(7);
    auto fut = map.acquire(7);
    EXPECT_FALSE(fut.available());
    EXPECT_EQ(map.size(), 1);

    map.broken();

    EXPECT_THROW(co_await std::move(fut), ss::broken_semaphore);

    // The broken waiter's active count was rolled back. Only the original
    // holder remains.
    EXPECT_EQ(map.size(), 1);

    units = {};
    EXPECT_EQ(map.size(), 0);
}

// Two read locks on the same key succeed without deadlock.
TEST_CORO(EntityRwlockMapTest, ReadReadDoesNotBlock) {
    entity_rwlock_map<int> map;

    absl::btree_set<int> keys{1};
    auto readers1 = co_await map.acquire_read(keys);
    EXPECT_EQ(map.size(), 1);

    auto readers2 = co_await map.acquire_read(keys);
    EXPECT_EQ(map.size(), 1);

    readers1.clear();
    readers2.clear();
    EXPECT_EQ(map.size(), 0);
}

// A write lock blocks while a reader holds the same key.
TEST_CORO(EntityRwlockMapTest, ReadWriteBlocks) {
    entity_rwlock_map<int> map;

    absl::btree_set<int> keys{1};
    auto readers = co_await map.acquire_read(keys);
    EXPECT_EQ(map.size(), 1);

    auto write_fut = map.acquire_write(keys);
    EXPECT_FALSE(write_fut.available());

    readers.clear();

    auto writers = co_await std::move(write_fut);
    EXPECT_EQ(map.size(), 1);

    writers.clear();
    EXPECT_EQ(map.size(), 0);
}
