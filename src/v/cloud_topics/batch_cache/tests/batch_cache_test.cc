/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/batch_cache/batch_cache.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "redpanda/tests/fixture.h"

#include <seastar/core/sstring.hh>

#include <gtest/gtest.h>

namespace cloud_topics {
struct batch_cache_accessor {
    static void evict_offset(
      batch_cache& c, const model::topic_id_partition& tidp, model::offset o) {
        c._entries[tidp].index->testing_evict_from_cache(o);
    }
    static void reclaim(
      batch_cache& c, const model::topic_id_partition& tidp, size_t size) {
        // it doesn't really matter what tidp is used, all the indices point to
        // the same cache.
        c._entries[tidp].index->testing_reclaim_from_cache(size);
    }
    static bool
    contains_tidp(const batch_cache& c, const model::topic_id_partition& tidp) {
        auto it = c._entries.find(tidp);
        return it != c._entries.end() && it->second.index != nullptr;
    }
};

} // namespace cloud_topics

constexpr auto cache_check_interval = 100ms;

class batch_cache_test_fixture
  : public redpanda_thread_fixture
  , public ::testing::Test {
public:
    batch_cache_test_fixture()
      : redpanda_thread_fixture()
      , _cache(&app.storage.local().log_mgr(), cache_check_interval) {}

    cloud_topics::batch_cache _cache;

    bool contains_tidp(const model::topic_id_partition& tidp) {
        return cloud_topics::batch_cache_accessor::contains_tidp(_cache, tidp);
    }

    void evict_offset(const model::topic_id_partition& tidp, model::offset o) {
        cloud_topics::batch_cache_accessor::evict_offset(_cache, tidp, o);
    }

    void reclaim(const model::topic_id_partition& tidp, size_t size) {
        cloud_topics::batch_cache_accessor::reclaim(_cache, tidp, size);
    }
};

TEST_F(batch_cache_test_fixture, test_batch_cache_put_get) {
    auto tidp = model::topic_id_partition{
      model::topic_id::create(), model::partition_id(0)};
    auto batch = model::test::make_random_batch(model::offset(0), 10, false);

    // Put batch in cache
    _cache.put(tidp, batch);

    // Get batch
    auto retrieved = _cache.get(tidp, model::offset(0));
    ASSERT_TRUE(retrieved.has_value());
    ASSERT_EQ(retrieved->base_offset(), batch.base_offset());
    ASSERT_EQ(retrieved->header().record_count, batch.header().record_count);
}

TEST_F(batch_cache_test_fixture, test_batch_cache_get_nonexistent) {
    auto tidp = model::topic_id_partition{
      model::topic_id::create(), model::partition_id(0)};

    // Try to get batch that doesn't exist
    auto retrieved = _cache.get(tidp, model::offset(0));
    ASSERT_TRUE(!retrieved.has_value());
}

TEST_F(batch_cache_test_fixture, test_batch_cache_multiple_tidps) {
    auto tidp1 = model::topic_id_partition{
      model::topic_id::create(), model::partition_id(0)};
    auto tidp2 = model::topic_id_partition{
      model::topic_id::create(), model::partition_id(1)};

    auto batch1 = model::test::make_random_batch(model::offset(0), 5, false);
    auto batch2 = model::test::make_random_batch(model::offset(10), 8, false);

    // Put batches in cache
    _cache.put(tidp1, batch1);
    _cache.put(tidp2, batch2);

    // Get batches
    auto retrieved1 = _cache.get(tidp1, model::offset(0));
    auto retrieved2 = _cache.get(tidp2, model::offset(10));

    ASSERT_TRUE(retrieved1.has_value());
    ASSERT_TRUE(retrieved2.has_value());

    ASSERT_EQ(retrieved1->base_offset(), batch1.base_offset());
    ASSERT_EQ(retrieved2->base_offset(), batch2.base_offset());

    // Try to get batch with wrong offset
    auto retrieved = _cache.get(tidp2, model::offset(0));
    ASSERT_TRUE(!retrieved.has_value());
}

TEST_F(batch_cache_test_fixture, test_batch_cache_eviction) {
    auto tidp = model::topic_id_partition{
      model::topic_id::create(), model::partition_id(0)};
    auto batch = model::test::make_random_batch(model::offset(42), 10, false);

    // The cleanup will start in 100ms
    _cache.start().get();

    // Put batch in cache
    _cache.put(tidp, batch);
    auto retrieved = _cache.get(tidp, model::offset(42));
    ASSERT_TRUE(retrieved.has_value());

    ASSERT_TRUE(contains_tidp(tidp));

    reclaim(tidp, 1);

    ASSERT_TRUE(contains_tidp(tidp));

    // This should evict the topic_id_partition
    ss::sleep(cache_check_interval * 2).get();

    ASSERT_FALSE(contains_tidp(tidp));

    _cache.stop().get();
}

// Verify that put_ordered inserts batches into the cache and notifies
// waiters blocked in wait_for_offset.
TEST_F(batch_cache_test_fixture, test_put_ordered_sequential) {
    auto tidp = model::topic_id_partition{
      model::topic_id::create(), model::partition_id(0)};

    // Create 3 batches: [0,9], [10,19], [20,29]
    chunked_vector<model::record_batch> batches;
    batches.push_back(
      model::test::make_random_batch(model::offset(0), 10, false));
    batches.push_back(
      model::test::make_random_batch(model::offset(10), 10, false));
    batches.push_back(
      model::test::make_random_batch(model::offset(20), 10, false));

    _cache.start().get();

    // Seed the offset monitor so put_ordered can check alignment.
    _cache
      .wait_for_offset(
        tidp,
        model::offset{},
        model::offset{},
        model::timeout_clock::now(),
        std::nullopt)
      .get();

    _cache.put_ordered(tidp, std::move(batches));

    // All batches should be retrievable immediately.
    auto b0 = _cache.get(tidp, model::offset(0));
    auto b1 = _cache.get(tidp, model::offset(10));
    auto b2 = _cache.get(tidp, model::offset(20));
    ASSERT_TRUE(b0.has_value());
    ASSERT_TRUE(b1.has_value());
    ASSERT_TRUE(b2.has_value());
    ASSERT_EQ(b0->base_offset(), model::offset(0));
    ASSERT_EQ(b1->base_offset(), model::offset(10));
    ASSERT_EQ(b2->base_offset(), model::offset(20));

    // The monitor should have been notified up to offset 29.
    auto wait_fut = _cache.wait_for_offset(
      tidp,
      model::offset(29),
      model::offset{},
      model::timeout_clock::now(),
      std::nullopt);
    ASSERT_TRUE(wait_fut.available());
    ASSERT_NO_THROW(wait_fut.get());

    _cache.stop().get();
}

// Verify that put_ordered wakes a concurrent wait_for_offset.
TEST_F(batch_cache_test_fixture, test_put_ordered_wakes_waiter) {
    auto tidp = model::topic_id_partition{
      model::topic_id::create(), model::partition_id(0)};

    _cache.start().get();

    // Start waiting for offset 9 (last offset of a 10-record batch at 0).
    auto wait_fut = _cache.wait_for_offset(
      tidp,
      model::offset(9),
      model::offset{},
      model::timeout_clock::now() + 5s,
      std::nullopt);

    ASSERT_FALSE(wait_fut.available())
      << "wait_for_offset should block before put_ordered";

    // Insert the batch that covers offset 9.
    chunked_vector<model::record_batch> batches;
    batches.push_back(
      model::test::make_random_batch(model::offset(0), 10, false));
    _cache.put_ordered(tidp, std::move(batches));

    // The waiter should now be resolved.
    ASSERT_NO_THROW(wait_fut.get());

    _cache.stop().get();
}

// Verify that out-of-order put_ordered calls notify correctly:
// inserting [10,19] first puts the data in cache but doesn't notify the
// monitor.  When [0,9] arrives the index has contiguous coverage and
// the monitor is notified for both batches.
TEST_F(batch_cache_test_fixture, test_put_ordered_out_of_order) {
    auto tidp = model::topic_id_partition{
      model::topic_id::create(), model::partition_id(0)};

    _cache.start().get();

    // Set up a waiter for offset 19 so the monitor exists.
    auto wait_fut = _cache.wait_for_offset(
      tidp,
      model::offset(19),
      model::offset{},
      model::timeout_clock::now() + 5s,
      std::nullopt);
    ASSERT_FALSE(wait_fut.available());

    // Insert batch [10,19] first — it goes into cache but the monitor
    // is not notified because there is no contiguous coverage from
    // the monitor's position.
    chunked_vector<model::record_batch> second_batch;
    second_batch.push_back(
      model::test::make_random_batch(model::offset(10), 10, false));
    _cache.put_ordered(tidp, std::move(second_batch));

    // The batch is in cache but the monitor hasn't been notified.
    ASSERT_TRUE(_cache.get(tidp, model::offset(10)).has_value());
    ASSERT_FALSE(wait_fut.available());

    // Now insert batch [0,9] — the index now has contiguous coverage
    // from 0 through 19, which fills the gap. put_ordered scans forward
    // past the inserted range and discovers [10,19] is already cached,
    // so the monitor is notified with offset 19.
    chunked_vector<model::record_batch> first_batch;
    first_batch.push_back(
      model::test::make_random_batch(model::offset(0), 10, false));
    _cache.put_ordered(tidp, std::move(first_batch));

    // Both batches are in cache and the waiter for offset 19 is resolved
    // because the gap-closing put discovers the full contiguous range.
    auto b0 = _cache.get(tidp, model::offset(0));
    auto b1 = _cache.get(tidp, model::offset(10));
    ASSERT_TRUE(b0.has_value());
    ASSERT_TRUE(b1.has_value());
    ASSERT_TRUE(wait_fut.available());
    wait_fut.get();

    _cache.stop().get();
}

// Verify that put_ordered inserts the batch even when the predecessor
// never arrives — the data is still cached, just no monitor notification.
TEST_F(
  batch_cache_test_fixture, test_put_ordered_no_predecessor_still_inserts) {
    auto tidp = model::topic_id_partition{
      model::topic_id::create(), model::partition_id(0)};

    _cache.start().get();

    // Seed the monitor.
    _cache
      .wait_for_offset(
        tidp,
        model::offset{},
        model::offset{},
        model::timeout_clock::now(),
        std::nullopt)
      .get();

    // Insert batch [10,19] without [0,9] — no contiguous coverage so
    // the monitor is not notified, but the batch is still in cache.
    chunked_vector<model::record_batch> batches;
    batches.push_back(
      model::test::make_random_batch(model::offset(10), 10, false));
    _cache.put_ordered(tidp, std::move(batches));

    auto b = _cache.get(tidp, model::offset(10));
    ASSERT_TRUE(b.has_value());
    ASSERT_EQ(b->base_offset(), model::offset(10));

    _cache.stop().get();
}

TEST_F(batch_cache_test_fixture, test_batch_cache_topic_recreation) {
    // Test that recreating a topic with the same name doesn't resurrect batches
    auto topic_id_1 = model::topic_id::create();
    auto topic_id_2 = model::topic_id::create();
    auto tidp1 = model::topic_id_partition{topic_id_1, model::partition_id(0)};
    auto tidp2 = model::topic_id_partition{topic_id_2, model::partition_id(0)};

    auto batch1 = model::test::make_random_batch(model::offset(0), 5, false);
    auto batch2 = model::test::make_random_batch(model::offset(0), 8, false);

    // Put batch for first topic
    _cache.put(tidp1, batch1);

    // Verify we can get it back
    auto retrieved1 = _cache.get(tidp1, model::offset(0));
    ASSERT_TRUE(retrieved1.has_value());
    ASSERT_EQ(retrieved1->base_offset(), batch1.base_offset());

    // Put batch for second topic (simulating topic recreation)
    _cache.put(tidp2, batch2);

    // Verify we get the correct batch for each topic
    auto retrieved1_again = _cache.get(tidp1, model::offset(0));
    auto retrieved2 = _cache.get(tidp2, model::offset(0));

    ASSERT_TRUE(retrieved1_again.has_value());
    ASSERT_TRUE(retrieved2.has_value());

    // The batches should be different
    ASSERT_EQ(retrieved1_again->base_offset(), batch1.base_offset());
    ASSERT_EQ(retrieved2->base_offset(), batch2.base_offset());
    ASSERT_NE(
      retrieved1_again->header().record_count,
      retrieved2->header().record_count);
}
