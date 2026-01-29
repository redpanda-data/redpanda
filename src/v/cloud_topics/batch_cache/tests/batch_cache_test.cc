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
        c._index[tidp]->testing_evict_from_cache(o);
    }
    static void reclaim(
      batch_cache& c, const model::topic_id_partition& tidp, size_t size) {
        // it doesn't really matter what tidp is used, all the indices point to
        // the same cache.
        c._index[tidp]->testing_reclaim_from_cache(size);
    }
    static bool
    contains_tidp(const batch_cache& c, const model::topic_id_partition& tidp) {
        return c._index.contains(tidp);
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
