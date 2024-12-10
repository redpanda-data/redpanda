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
#include "test_utils/test.h"
#include "utils/chunked_kv_cache.h"

#include <optional>

TEST(ChunkedKVTest, InsertGetTest) {
    using cache_type = utils::chunked_kv_cache<int, std::string>;

    cache_type cache(cache_type::config{.cache_size = 2, .small_size = 1});
    auto str = "avaluestr";

    EXPECT_EQ(cache.try_insert(1, ss::make_shared<std::string>(str)), true);
    auto v = cache.get_value(1);
    EXPECT_TRUE(v);
    EXPECT_EQ(**v, str);
}

TEST(ChunkedKVTest, InvalidGetTest) {
    using cache_type = utils::chunked_kv_cache<int, std::string>;

    cache_type cache(cache_type::config{.cache_size = 2, .small_size = 1});

    EXPECT_EQ(cache.get_value(0), std::nullopt);
}

TEST(ChunkedKVTest, ReinsertionTest) {
    using cache_type = utils::chunked_kv_cache<int, std::string>;

    cache_type cache(cache_type::config{.cache_size = 2, .small_size = 1});
    auto str = "avaluestr";

    EXPECT_EQ(cache.try_insert(0, ss::make_shared<std::string>(str)), true);
    auto stat = cache.stat();
    EXPECT_EQ(stat.main_queue_size, 0);
    EXPECT_EQ(stat.small_queue_size, 1);
    EXPECT_EQ(stat.index_size, 1);

    EXPECT_EQ(cache.try_insert(0, ss::make_shared<std::string>(str)), false);
    stat = cache.stat();
    EXPECT_EQ(stat.main_queue_size, 0);
    EXPECT_EQ(stat.small_queue_size, 1);
    EXPECT_EQ(stat.index_size, 1);
}

TEST(ChunkedKVTest, EvictionTest) {
    using cache_type = utils::chunked_kv_cache<int, std::string>;

    cache_type cache(cache_type::config{.cache_size = 2, .small_size = 1});
    auto str = "avaluestr";

    // Initial phase without any eviction. The `s3_fifo` cache allows for its
    // max size to be exceeded by one.
    for (int i = 0; i < 3; i++) {
        EXPECT_EQ(cache.try_insert(i, ss::make_shared<std::string>(str)), true);
        auto stat = cache.stat();
        EXPECT_EQ(stat.main_queue_size, 0);
        EXPECT_EQ(stat.small_queue_size, i + 1);
        EXPECT_EQ(stat.index_size, i + 1);
    }

    // Next phase with evictions and one ghost queue entry. Ensures that entries
    // that have been removed from the ghost queue are also removed from the
    // index.
    for (int i = 3; i < 10; i++) {
        EXPECT_EQ(cache.try_insert(i, ss::make_shared<std::string>(str)), true);
        auto stat = cache.stat();
        EXPECT_EQ(stat.main_queue_size, 0);
        EXPECT_EQ(stat.small_queue_size, 3);
        EXPECT_EQ(stat.index_size, 4);
    }
}

TEST(ChunkedKVTest, GhostToMainTest) {
    using cache_type = utils::chunked_kv_cache<int, std::string>;

    cache_type cache(cache_type::config{.cache_size = 4, .small_size = 1});
    auto str = "avaluestr";

    // Fill the cache
    for (int i = 0; i < 5; i++) {
        EXPECT_EQ(cache.try_insert(i, ss::make_shared<std::string>(str)), true);
        auto stat = cache.stat();
        EXPECT_EQ(stat.main_queue_size, 0);
        EXPECT_EQ(stat.small_queue_size, i + 1);
        EXPECT_EQ(stat.index_size, i + 1);
    }

    // Move one entry to the ghost queue
    EXPECT_EQ(cache.try_insert(5, ss::make_shared<std::string>(str)), true);
    auto stat = cache.stat();
    EXPECT_EQ(stat.main_queue_size, 0);
    EXPECT_EQ(stat.small_queue_size, 5);
    EXPECT_EQ(stat.index_size, 6);

    // Get the key for the entry in the ghost queue.
    int key = -1;
    for (int i = 0; i < 5; i++) {
        if (!cache.get_value(i)) {
            key = i;
            break;
        }
    }

    EXPECT_NE(key, -1);

    // Move entry from ghost queue to main queue.
    EXPECT_EQ(cache.try_insert(key, ss::make_shared<std::string>(str)), true);
    stat = cache.stat();
    EXPECT_EQ(stat.main_queue_size, 1);
    EXPECT_EQ(stat.small_queue_size, 4);
    EXPECT_EQ(stat.index_size, 6);
}
