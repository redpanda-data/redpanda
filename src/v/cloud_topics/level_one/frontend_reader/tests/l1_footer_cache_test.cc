/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/frontend_reader/l1_footer_cache.h"
#include "config/mock_property.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

#include <chrono>

using namespace cloud_topics;
using namespace std::chrono_literals;

namespace cloud_topics::l1 {

namespace {

/// Construct a distinguishable footer by stamping the partition's
/// first_offset.
footer make_footer(int64_t marker) {
    footer f;
    f.partitions.emplace(
      model::topic_id_partition{
        model::topic_id(uuid_t::create()), model::partition_id(0)},
      footer::partition{.first_offset = kafka::offset{marker}});
    return f;
}

} // namespace

class L1FooterCacheTest : public seastar_test {
protected:
    static constexpr std::chrono::milliseconds default_eviction_timeout = 60s;
    static constexpr size_t default_max_size = 4;

    void SetUp() override {
        _eviction_timeout_binding = config::mock_binding(
          default_eviction_timeout);
        _cache = std::make_unique<l1_footer_cache>(
          _eviction_timeout_binding, _max_size_prop.bind());
    }

    ss::future<> TearDownAsync() override {
        if (_cache) {
            co_await _cache->stop();
            _cache.reset();
        }
    }

    void set_max_size(size_t v) { _max_size_prop.update(std::move(v)); }

    config::binding<std::chrono::milliseconds> _eviction_timeout_binding{
      config::mock_binding(default_eviction_timeout)};
    config::mock_property<size_t> _max_size_prop{default_max_size};
    std::unique_ptr<l1_footer_cache> _cache;
};

TEST_F(L1FooterCacheTest, MissOnEmpty) {
    auto miss = _cache->get(create_object_id());
    EXPECT_FALSE(miss.has_value());
    EXPECT_EQ(_cache->get_stats().cached_footers, 0);
}

TEST_F(L1FooterCacheTest, HitAfterPut) {
    auto oid = create_object_id();
    _cache->put(oid, footer{});

    auto hit = _cache->get(oid);
    EXPECT_TRUE(hit.has_value());
    EXPECT_EQ(_cache->get_stats().cached_footers, 1);
}

TEST_F(L1FooterCacheTest, MissOnDifferentOid) {
    auto oid1 = create_object_id();
    auto oid2 = create_object_id();
    ASSERT_NE(oid1, oid2);

    _cache->put(oid1, footer{});

    auto miss = _cache->get(oid2);
    EXPECT_FALSE(miss.has_value());
    auto hit = _cache->get(oid1);
    EXPECT_TRUE(hit.has_value());
}

TEST_F(L1FooterCacheTest, SizeWatcherEvicts) {
    auto oid1 = create_object_id();
    auto oid2 = create_object_id();
    ASSERT_NE(oid1, oid2);

    _cache->put(oid1, footer{});
    _cache->put(oid2, footer{});

    EXPECT_TRUE(_cache->get(oid1).has_value());
    EXPECT_TRUE(_cache->get(oid2).has_value());
    EXPECT_EQ(_cache->get_stats().cached_footers, 2);

    // Footer cache watches the size config binding and immediately
    // (synchronously) evicts down to configured size.
    set_max_size(1);

    EXPECT_EQ(_cache->get_stats().cached_footers, 1);
    EXPECT_FALSE(_cache->get(oid1).has_value());
    EXPECT_TRUE(_cache->get(oid2).has_value());

    // Now the size is (1), so the next put should evict oid2.
    _cache->put(oid1, footer{});
    EXPECT_TRUE(_cache->get(oid1).has_value());
    EXPECT_FALSE(_cache->get(oid2).has_value());
}

TEST_F(L1FooterCacheTest, OverflowEvictsLru) {
    std::vector<object_id> oids;
    oids.reserve(default_max_size + 2);
    for (size_t i = 0; i < default_max_size + 2; ++i) {
        oids.push_back(create_object_id());
        _cache->put(oids.back(), footer{});
    }

    auto stats = _cache->get_stats();
    EXPECT_EQ(stats.cached_footers, default_max_size);

    // First two oids should have been evicted as LRU.
    EXPECT_FALSE(_cache->get(oids[0]).has_value());
    EXPECT_FALSE(_cache->get(oids[1]).has_value());
    // The remaining oids should still be present.
    for (size_t i = 2; i < oids.size(); ++i) {
        EXPECT_TRUE(_cache->get(oids[i]).has_value())
          << "oid index " << i << " unexpectedly evicted";
    }
}

TEST_F(L1FooterCacheTest, ReplaceUpdatesValueAndOrder) {
    auto oid1 = create_object_id();
    auto oid2 = create_object_id();

    auto footer_a = make_footer(/*marker=*/1);
    auto footer_b = make_footer(/*marker=*/2);
    auto footer_c = make_footer(/*marker=*/3);

    _cache->put(oid1, footer_a.copy());
    _cache->put(oid2, footer_b.copy());
    _cache->put(oid1, footer_c.copy());

    EXPECT_EQ(_cache->get_stats().cached_footers, 2)
      << "duplicate insert should not grow the cache";

    auto got1 = _cache->get(oid1);
    ASSERT_TRUE(got1.has_value());
    EXPECT_EQ(**got1, footer_c)
      << "the replacement footer should be returned for oid1";

    // After the puts above, the LRU end is oid2. Push enough new entries
    // to overflow the cap by one and trigger a single eviction; oid2
    // should be the one to go.
    size_t puts_to_overflow = default_max_size
                              - _cache->get_stats().cached_footers + 1;
    for (size_t i = 0; i < puts_to_overflow; ++i) {
        _cache->put(create_object_id(), footer{});
    }
    EXPECT_FALSE(_cache->get(oid2).has_value())
      << "oid2 should have been the LRU end after the replace, "
         "and evicted first when the cache overflowed";
    EXPECT_TRUE(_cache->get(oid1).has_value())
      << "oid1 was MRU after the replace and should still be cached";
}

TEST_F(L1FooterCacheTest, PutReturnSharesWithCache) {
    auto oid = create_object_id();
    auto put_ret = _cache->put(oid, make_footer(/*marker=*/7));
    ASSERT_TRUE(put_ret);
    EXPECT_EQ(
      put_ret->partitions.begin()->second.first_offset, kafka::offset{7});

    auto got = _cache->get(oid);
    ASSERT_TRUE(got.has_value());
    // The pointer stored in the cache must be the same one `put` returned,
    // not a fresh wrap of an equal footer.
    EXPECT_EQ(put_ret.get(), got->get())
      << "put's return and get's return should share the cached storage";
}

TEST_F(L1FooterCacheTest, PutReturnsValidPointerWhenDisabled) {
    set_max_size(0);
    auto oid = create_object_id();
    auto put_ret = _cache->put(oid, make_footer(/*marker=*/11));

    // Cache is disabled, so nothing is stored.
    EXPECT_EQ(_cache->get_stats().cached_footers, 0);
    EXPECT_FALSE(_cache->get(oid).has_value());

    // But put still wraps and returns a usable shared_ptr so the caller
    // can use it as its own return value without an extra wrap.
    ASSERT_TRUE(put_ret);
    EXPECT_EQ(
      put_ret->partitions.begin()->second.first_offset, kafka::offset{11});
}

TEST_F(L1FooterCacheTest, GetUpdatesLastUsedForLru) {
    std::vector<object_id> oids;
    oids.reserve(default_max_size);
    for (size_t i = 0; i < default_max_size; ++i) {
        oids.push_back(create_object_id());
        _cache->put(oids.back(), footer{});
    }
    ASSERT_EQ(_cache->get_stats().cached_footers, default_max_size);

    // Touch oid[0] so it becomes most-recently-used.
    auto touched = _cache->get(oids[0]);
    ASSERT_TRUE(touched.has_value());

    // Insert a new entry; this should evict the now-LRU, which is oid[1].
    auto new_oid = create_object_id();
    _cache->put(new_oid, footer{});

    EXPECT_EQ(_cache->get_stats().cached_footers, default_max_size);
    EXPECT_TRUE(_cache->get(oids[0]).has_value())
      << "touched oid[0] should have been retained";
    EXPECT_FALSE(_cache->get(oids[1]).has_value())
      << "oid[1] should have been evicted as LRU";
    for (size_t i = 2; i < oids.size(); ++i) {
        EXPECT_TRUE(_cache->get(oids[i]).has_value());
    }
    EXPECT_TRUE(_cache->get(new_oid).has_value());
}

} // namespace cloud_topics::l1
