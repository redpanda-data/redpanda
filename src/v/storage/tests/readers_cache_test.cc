// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "config/base_property.h"
#include "config/config_store.h"
#include "config/property.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "storage/fs_utils.h"
#include "storage/log_reader.h"
#include "storage/probe.h"
#include "storage/readers_cache.h"
#include "storage/segment.h"
#include "storage/storage_resources.h"
#include "storage/types.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/util/defer.hh>

#include <fmt/ostream.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace storage {
struct readers_cache_test_fixture : seastar_test {
    using segment_ptr = ss::lw_shared_ptr<storage::segment>;
    bool intersects_locked_range(
      const readers_cache& cache, model::offset begin, model::offset end) {
        return cache.intersects_with_locked_range(begin, end);
    }

    void test_intersects_locked(
      const readers_cache& cache,
      model::offset::type begin,
      model::offset::type end,
      bool in_range) {
        ASSERT_EQ(
          intersects_locked_range(
            cache, model::offset(begin), model::offset(end)),
          in_range);
    }

    readers_cache make_cache(
      std::chrono::milliseconds max_age, config::property<size_t> max_size) {
        return storage::readers_cache(
          model::ntp("test", "test", 0), max_age, max_size);
    }

    config::property<size_t> make_max_size_property(size_t default_value) {
        return config::property<size_t>(
          store,
          "max_cache_size",
          "max_cache_size",
          {.needs_restart = config::needs_restart::no,
           .visibility = config::visibility::tunable},
          default_value);
    }

    std::unique_ptr<storage::log_reader>
    make_reader(ss::circular_buffer<segment_ptr> segments = {}) {
        auto lease = std::make_unique<lock_manager::lease>(
          segment_set(std::move(segments)));
        return std::make_unique<log_reader>(
          std::move(lease),
          storage::log_reader_config(
            model::offset(0),
            model::offset::max(),
            ss::default_priority_class()),
          probe,
          nullptr);
    }

    segment_ptr make_segment(model::offset base_offset) {
        segment_index idx(
          segment_full_path::mock("mocked"),
          base_offset,
          1024,
          features,
          std::nullopt);
        return ss::make_lw_shared<storage::segment>(
          storage::segment::offset_tracker(model::term_id(0), base_offset),
          nullptr,
          std::move(idx),
          nullptr,
          std::nullopt,
          std::nullopt,
          resources);
    }

    storage::storage_resources resources;
    ss::sharded<features::feature_table> features;
    config::config_store store;
    storage::probe probe;
};

} // namespace storage
using namespace storage;

TEST_F(readers_cache_test_fixture, test_range_is_correctly_locked) {
    readers_cache cache = make_cache(1h, make_max_size_property(10));
    {
        /**
         * Evict truncate locks range starting from give offset up to max
         */
        auto holder = cache.evict_truncate(model::offset(10)).get();

        // [0,1] is not in [10,inf]
        test_intersects_locked(cache, 0, 1, false);

        // [0,10] is in range [10,inf]
        test_intersects_locked(cache, 0, 10, true);

        // [9,12] is in range [10, inf]
        test_intersects_locked(cache, 9, 12, true);

        // [10,12] is in range [10, inf]
        test_intersects_locked(cache, 10, 12, true);

        // [20,25] is in range [10, inf]
        test_intersects_locked(cache, 20, 25, true);
    }

    {
        /**
         * Evict prefix truncate locks range starting from 0 up to given offset
         */
        auto holder = cache.evict_prefix_truncate(model::offset(10)).get();

        // [0,1] is in [0,10]
        test_intersects_locked(cache, 0, 1, true);

        // [0,10] is in range [0,10]
        test_intersects_locked(cache, 0, 10, true);

        // [9,12] is in range [0, 10]
        test_intersects_locked(cache, 9, 12, true);

        // [10,12] is in range [0, 10]
        test_intersects_locked(cache, 10, 12, true);

        // [20,25] is not in range [0, 10]
        test_intersects_locked(cache, 20, 25, false);
    }

    {
        /**
         * Evict range locks given range
         */
        auto holder
          = cache.evict_range(model::offset(5), model::offset(10)).get();

        // [0,1] is not in [5,10]
        test_intersects_locked(cache, 0, 1, false);

        // [0,20] is not in range [5,10] but [5,10] is in [0,20]
        test_intersects_locked(cache, 0, 20, true);

        // [9,12] is in range [5,10]
        test_intersects_locked(cache, 9, 12, true);

        // [10,12] is in range [5,10]
        test_intersects_locked(cache, 10, 12, true);

        // [20,25] is not in range [5,10]
        test_intersects_locked(cache, 20, 25, false);

        // [4,5] is in range [5,10]
        test_intersects_locked(cache, 4, 5, true);

        // [6,7] is in range [5,10]
        test_intersects_locked(cache, 6, 7, true);
    }
}

TEST_F(readers_cache_test_fixture, test_reader_with_no_lease_is_not_cached) {
    readers_cache cache = make_cache(30s, make_max_size_property(10));
    for (int i = 0; i < 10; ++i) {
        cache.put(make_reader());
    }

    EXPECT_EQ(cache.get_stats().cached_readers, 0);
    EXPECT_EQ(cache.get_stats().in_use_readers, 0);
}

TEST_F(readers_cache_test_fixture, test_size_based_eviction) {
    readers_cache cache = make_cache(30s, make_max_size_property(10));

    auto close = ss::defer([&cache] { cache.stop().get(); });

    for (int i = 0; i < 10; ++i) {
        ss::circular_buffer<segment_ptr> set;
        set.push_back(make_segment(model::offset(i)));
        cache.put(make_reader(std::move(set)));
    }
    /**
     * We are not using the reader. It should be returned to the cache
     * immediately.
     */
    EXPECT_EQ(cache.get_stats().cached_readers, 10);
    EXPECT_EQ(cache.get_stats().in_use_readers, 0);

    /**
     * Add few more readers, the cache should grow temporally as cleanup is done
     * in a background.
     */
    for (int i = 10; i < 15; ++i) {
        ss::circular_buffer<segment_ptr> set;
        set.push_back(make_segment(model::offset(i)));
        cache.put(make_reader(std::move(set)));
    }

    EXPECT_EQ(cache.get_stats().in_use_readers, 0);

    RPTEST_REQUIRE_EVENTUALLY(
      5s, [&] { return cache.get_stats().cached_readers == 10; });

    ss::circular_buffer<segment_ptr> set;
    set.push_back(make_segment(model::offset(100)));
    auto reader = cache.put(make_reader(std::move(set)));
    EXPECT_EQ(cache.get_stats().in_use_readers, 1);
    /**
     * Since one of the readers in in use, one of the additional cached readers
     * should eventually be evicted
     */
    RPTEST_REQUIRE_EVENTUALLY(
      5s, [&] { return cache.get_stats().cached_readers == 9; });
}
