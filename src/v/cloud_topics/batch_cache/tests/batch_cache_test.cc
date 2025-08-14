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
#include "redpanda/tests/fixture.h"

#include <seastar/core/sstring.hh>

#include <gtest/gtest.h>

using namespace experimental;

namespace experimental::cloud_topics {
struct batch_cache_accessor {
    static void
    evict_offset(batch_cache& c, const model::ntp& ntp, model::offset o) {
        c._index[ntp]->testing_evict_from_cache(o);
    }
    static bool contains_ntp(const batch_cache& c, const model::ntp& ntp) {
        return c._index.contains(ntp);
    }
};

} // namespace experimental::cloud_topics

constexpr auto cache_check_interval = 100ms;

class batch_cache_test_fixture
  : public redpanda_thread_fixture
  , public ::testing::Test {
public:
    batch_cache_test_fixture()
      : redpanda_thread_fixture()
      , _cache(&app.storage.local().log_mgr(), cache_check_interval) {}

    cloud_topics::batch_cache _cache;

    bool contains_ntp(const model::ntp& ntp) {
        return cloud_topics::batch_cache_accessor::contains_ntp(_cache, ntp);
    }

    void evict_offset(const model::ntp& ntp, model::offset o) {
        cloud_topics::batch_cache_accessor::evict_offset(_cache, ntp, o);
    }
};

TEST_F(batch_cache_test_fixture, test_batch_cache_put_get) {
    model::ntp test_ntp("ns", "topic", 0);
    auto batch = model::test::make_random_batch(model::offset(0), 10, false);

    // Put batch in cache
    _cache.put(test_ntp, batch);

    // Get batch
    auto retrieved = _cache.get(test_ntp, model::offset(0));
    ASSERT_TRUE(retrieved.has_value());
    ASSERT_EQ(retrieved->base_offset(), batch.base_offset());
    ASSERT_EQ(retrieved->header().record_count, batch.header().record_count);
}

TEST_F(batch_cache_test_fixture, test_batch_cache_get_nonexistent) {
    model::ntp test_ntp("ns", "topic", 0);

    // Try to get batch that doesn't exist
    auto retrieved = _cache.get(test_ntp, model::offset(0));
    ASSERT_TRUE(!retrieved.has_value());
}

TEST_F(batch_cache_test_fixture, test_batch_cache_multiple_ntps) {
    model::ntp ntp1("ns1", "topic1", 0);
    model::ntp ntp2("ns2", "topic2", 1);

    auto batch1 = model::test::make_random_batch(model::offset(0), 5, false);
    auto batch2 = model::test::make_random_batch(model::offset(10), 8, false);

    // Put batches in cache
    _cache.put(ntp1, batch1);
    _cache.put(ntp2, batch2);

    // Get batches
    auto retrieved1 = _cache.get(ntp1, model::offset(0));
    auto retrieved2 = _cache.get(ntp2, model::offset(10));

    ASSERT_TRUE(retrieved1.has_value());
    ASSERT_TRUE(retrieved2.has_value());

    ASSERT_EQ(retrieved1->base_offset(), batch1.base_offset());
    ASSERT_EQ(retrieved2->base_offset(), batch2.base_offset());

    // Try to get batch with wrong offset
    auto retrieved = _cache.get(ntp2, model::offset(0));
    ASSERT_TRUE(!retrieved.has_value());
}

TEST_F(batch_cache_test_fixture, test_batch_cache_eviction) {
    model::ntp test_ntp("ns", "topic", 0);
    auto batch = model::test::make_random_batch(model::offset(42), 10, false);

    // The cleanup will start in 100ms
    _cache.start().get();

    // Put batch in cache
    _cache.put(test_ntp, batch);
    auto retrieved = _cache.get(test_ntp, model::offset(42));
    ASSERT_TRUE(retrieved.has_value());

    ASSERT_TRUE(contains_ntp(test_ntp));

    evict_offset(test_ntp, model::offset(42));

    ASSERT_TRUE(contains_ntp(test_ntp));

    // This should evict the NTP
    ss::sleep(cache_check_interval * 2).get();

    ASSERT_FALSE(contains_ntp(test_ntp));

    _cache.stop().get();
}
