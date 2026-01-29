/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/batch_cache/hydrated_object_index.h"
#include "storage/batch_cache.h"

#include <gtest/gtest.h>

namespace cloud_topics {

static storage::batch_cache::reclaim_options test_reclaim_opts = {
  .growth_window = std::chrono::milliseconds(3000),
  .stable_window = std::chrono::milliseconds(10000),
  .min_size = 128 << 10,
  .max_size = 4 << 20,
};

class partition_hydrated_index_test : public ::testing::Test {
public:
    partition_hydrated_index_test()
      : _cache(test_reclaim_opts) {}

    void TearDown() override {
        // Stop the batch_cache to clean up the background reclaimer
        _cache.stop().get();
    }

    static object_id make_object_id(cluster_epoch epoch = cluster_epoch{1}) {
        return object_id::create(epoch);
    }

    /// Create a factory that creates batch_cache_index instances
    batch_cache_index_factory make_factory() {
        return [this]() {
            return std::make_unique<storage::batch_cache_index>(_cache);
        };
    }

    /// Create a factory that always returns nullptr (disabled caching)
    static batch_cache_index_factory make_disabled_factory() {
        return []() { return storage::batch_cache_index_ptr{}; };
    }

    storage::batch_cache _cache;
};

TEST_F(partition_hydrated_index_test, put_and_has_extent) {
    partition_hydrated_index index(make_factory());
    auto id = make_object_id();
    auto byte_offset = first_byte_offset_t{100};
    size_t size_bytes = 1000;

    auto synthetic = index.put_extent(id, byte_offset, size_bytes);

    ASSERT_TRUE(synthetic.has_value());
    EXPECT_EQ(*synthetic, model::offset{0});
    EXPECT_EQ(index.extent_count(), 1);

    // Exact match
    EXPECT_TRUE(index.has_extent(id, byte_offset, size_bytes));
    // Non-existent offset
    EXPECT_FALSE(index.has_extent(id, first_byte_offset_t{0}, size_bytes));
}

TEST_F(
  partition_hydrated_index_test, put_same_extent_twice_returns_same_offset) {
    partition_hydrated_index index(make_factory());
    auto id = make_object_id();
    auto byte_offset = first_byte_offset_t{100};

    auto synthetic1 = index.put_extent(id, byte_offset, 1000);
    auto synthetic2 = index.put_extent(id, byte_offset, 1000);

    ASSERT_TRUE(synthetic1.has_value());
    ASSERT_TRUE(synthetic2.has_value());
    EXPECT_EQ(*synthetic1, *synthetic2);
    EXPECT_EQ(index.extent_count(), 1);
}

TEST_F(partition_hydrated_index_test, multiple_extents_from_same_object) {
    partition_hydrated_index index(make_factory());
    auto id = make_object_id();
    auto byte_offset1 = first_byte_offset_t{0};
    auto byte_offset2 = first_byte_offset_t{1000};

    auto synthetic1 = index.put_extent(id, byte_offset1, 1000);
    auto synthetic2 = index.put_extent(id, byte_offset2, 1000);

    ASSERT_TRUE(synthetic1.has_value());
    ASSERT_TRUE(synthetic2.has_value());
    EXPECT_NE(*synthetic1, *synthetic2);
    EXPECT_EQ(index.extent_count(), 2);
}

TEST_F(partition_hydrated_index_test, multiple_objects_same_epoch) {
    partition_hydrated_index index(make_factory());
    auto id1 = make_object_id();
    auto id2 = make_object_id();
    auto byte_offset = first_byte_offset_t{0};

    auto synthetic1 = index.put_extent(id1, byte_offset, 1000);
    auto synthetic2 = index.put_extent(id2, byte_offset, 1000);

    ASSERT_TRUE(synthetic1.has_value());
    ASSERT_TRUE(synthetic2.has_value());
    EXPECT_NE(*synthetic1, *synthetic2);
    EXPECT_EQ(index.extent_count(), 2);
    // Both in same epoch, so only one epoch_cache_index
    EXPECT_EQ(index.epoch_count(), 1);
}

TEST_F(partition_hydrated_index_test, get_nonexistent_extent) {
    partition_hydrated_index index(make_factory());
    auto id = make_object_id();

    EXPECT_FALSE(index.has_extent(id, first_byte_offset_t{0}, 100));
    auto data = index.get_extent(id, first_byte_offset_t{0}, 100);
    EXPECT_FALSE(data.has_value());
}

TEST_F(
  partition_hydrated_index_test, different_epochs_create_different_indices) {
    partition_hydrated_index index(make_factory());

    auto id_epoch1 = make_object_id(cluster_epoch{1});
    auto id_epoch2 = make_object_id(cluster_epoch{2});
    auto id_epoch3 = make_object_id(cluster_epoch{3});

    auto s1 = index.put_extent(id_epoch1, first_byte_offset_t{0}, 1000);
    auto s2 = index.put_extent(id_epoch2, first_byte_offset_t{0}, 1000);
    auto s3 = index.put_extent(id_epoch3, first_byte_offset_t{0}, 1000);

    ASSERT_TRUE(s1.has_value());
    ASSERT_TRUE(s2.has_value());
    ASSERT_TRUE(s3.has_value());

    // Each epoch has its own counter, so offsets start from 0 for each
    EXPECT_EQ(*s1, model::offset{0});
    EXPECT_EQ(*s2, model::offset{0});
    EXPECT_EQ(*s3, model::offset{0});

    EXPECT_EQ(index.extent_count(), 3);
    EXPECT_EQ(index.epoch_count(), 3);
}

TEST_F(partition_hydrated_index_test, per_epoch_offset_counter) {
    partition_hydrated_index index(make_factory());

    auto id_epoch1_obj1 = make_object_id(cluster_epoch{1});
    auto id_epoch1_obj2 = make_object_id(cluster_epoch{1});
    auto id_epoch2_obj1 = make_object_id(cluster_epoch{2});

    auto s1 = index.put_extent(id_epoch1_obj1, first_byte_offset_t{0}, 1000);
    auto s2 = index.put_extent(id_epoch1_obj2, first_byte_offset_t{0}, 1000);
    auto s3 = index.put_extent(id_epoch2_obj1, first_byte_offset_t{0}, 1000);

    // Within same epoch, offsets increase
    EXPECT_EQ(*s1, model::offset{0});
    EXPECT_EQ(*s2, model::offset{1});
    // New epoch starts from 0
    EXPECT_EQ(*s3, model::offset{0});
}

TEST_F(partition_hydrated_index_test, epochs_can_be_added_in_any_order) {
    partition_hydrated_index index(make_factory());

    // Add epochs in non-monotonic order - this should work now
    auto id_epoch3 = make_object_id(cluster_epoch{3});
    auto id_epoch1 = make_object_id(cluster_epoch{1});
    auto id_epoch2 = make_object_id(cluster_epoch{2});

    auto s1 = index.put_extent(id_epoch3, first_byte_offset_t{0}, 1000);
    auto s2 = index.put_extent(id_epoch1, first_byte_offset_t{0}, 1000);
    auto s3 = index.put_extent(id_epoch2, first_byte_offset_t{0}, 1000);

    ASSERT_TRUE(s1.has_value());
    ASSERT_TRUE(s2.has_value());
    ASSERT_TRUE(s3.has_value());

    EXPECT_EQ(index.extent_count(), 3);
    EXPECT_EQ(index.epoch_count(), 3);
}

TEST_F(partition_hydrated_index_test, truncate_epoch_removes_older_epochs) {
    partition_hydrated_index index(make_factory());

    auto id_epoch1 = make_object_id(cluster_epoch{1});
    auto id_epoch2 = make_object_id(cluster_epoch{2});
    auto id_epoch3 = make_object_id(cluster_epoch{3});

    index.put_extent(id_epoch1, first_byte_offset_t{0}, 1000);
    index.put_extent(id_epoch2, first_byte_offset_t{0}, 1000);
    index.put_extent(id_epoch3, first_byte_offset_t{0}, 1000);

    EXPECT_EQ(index.extent_count(), 3);
    EXPECT_EQ(index.epoch_count(), 3);

    // Truncate epochs strictly less than 3
    index.truncate_epoch(cluster_epoch{3});

    // Should have removed entries from epochs 1 and 2
    EXPECT_EQ(index.extent_count(), 1);
    EXPECT_EQ(index.epoch_count(), 1);

    // Only epoch 3 entry should remain
    EXPECT_FALSE(index.has_extent(id_epoch1, first_byte_offset_t{0}, 1000));
    EXPECT_FALSE(index.has_extent(id_epoch2, first_byte_offset_t{0}, 1000));
    EXPECT_TRUE(index.has_extent(id_epoch3, first_byte_offset_t{0}, 1000));
}

TEST_F(
  partition_hydrated_index_test, truncate_epoch_noop_when_no_older_epochs) {
    partition_hydrated_index index(make_factory());

    auto id_epoch5 = make_object_id(cluster_epoch{5});
    index.put_extent(id_epoch5, first_byte_offset_t{0}, 1000);

    EXPECT_EQ(index.extent_count(), 1);
    EXPECT_EQ(index.epoch_count(), 1);

    // Truncate epochs < 3, but we only have epoch 5
    index.truncate_epoch(cluster_epoch{3});

    EXPECT_EQ(index.extent_count(), 1);
    EXPECT_EQ(index.epoch_count(), 1);
    EXPECT_TRUE(index.has_extent(id_epoch5, first_byte_offset_t{0}, 1000));
}

TEST_F(
  partition_hydrated_index_test, get_batch_cache_index_returns_correct_index) {
    partition_hydrated_index index(make_factory());

    auto id_epoch1 = make_object_id(cluster_epoch{1});
    auto id_epoch2 = make_object_id(cluster_epoch{2});

    index.put_extent(id_epoch1, first_byte_offset_t{0}, 1000);
    index.put_extent(id_epoch2, first_byte_offset_t{0}, 1000);

    auto* idx1 = index.get_batch_cache_index(cluster_epoch{1});
    auto* idx2 = index.get_batch_cache_index(cluster_epoch{2});
    auto* idx3 = index.get_batch_cache_index(cluster_epoch{3});

    EXPECT_NE(idx1, nullptr);
    EXPECT_NE(idx2, nullptr);
    EXPECT_EQ(idx3, nullptr); // Non-existent epoch
    EXPECT_NE(idx1, idx2);    // Different epochs have different indices
}

TEST_F(partition_hydrated_index_test, disabled_cache_returns_nullopt) {
    partition_hydrated_index index(make_disabled_factory());
    auto id = make_object_id();

    auto synthetic = index.put_extent(id, first_byte_offset_t{0}, 1000);

    EXPECT_FALSE(synthetic.has_value());
    EXPECT_EQ(index.extent_count(), 0);
    EXPECT_EQ(index.epoch_count(), 0);
}

TEST_F(partition_hydrated_index_test, memory_usage_increases_with_entries) {
    partition_hydrated_index index(make_factory());

    auto initial_usage = index.memory_usage();
    EXPECT_EQ(initial_usage, 0);

    auto id = make_object_id();
    index.put_extent(id, first_byte_offset_t{0}, 1000);

    auto usage_after_one = index.memory_usage();
    EXPECT_GT(usage_after_one, initial_usage);

    index.put_extent(id, first_byte_offset_t{1000}, 1000);

    auto usage_after_two = index.memory_usage();
    EXPECT_GT(usage_after_two, usage_after_one);
}

TEST_F(partition_hydrated_index_test, has_extent_exact_match) {
    partition_hydrated_index index(make_factory());
    auto id = make_object_id();
    auto byte_offset = first_byte_offset_t{100};
    size_t size_bytes = 1000;

    EXPECT_FALSE(index.has_extent(id, byte_offset, size_bytes));

    index.put_extent(id, byte_offset, size_bytes);

    EXPECT_TRUE(index.has_extent(id, byte_offset, size_bytes));
    // Different offset - not found
    EXPECT_FALSE(index.has_extent(id, first_byte_offset_t{200}, size_bytes));
}

// Test subset query support
TEST_F(partition_hydrated_index_test, has_extent_subset_query) {
    partition_hydrated_index index(make_factory());
    auto id = make_object_id();

    // Cache a full extent at [0, 1024)
    index.put_extent(id, first_byte_offset_t{0}, 1024);

    // Query for exact match should succeed
    EXPECT_TRUE(index.has_extent(id, first_byte_offset_t{0}, 1024));

    // Query for subset [100, 300) should succeed
    EXPECT_TRUE(index.has_extent(id, first_byte_offset_t{100}, 200));

    // Query for subset starting at 0 [0, 512) should succeed
    EXPECT_TRUE(index.has_extent(id, first_byte_offset_t{0}, 512));

    // Query for subset ending at extent end [512, 1024) should succeed
    EXPECT_TRUE(index.has_extent(id, first_byte_offset_t{512}, 512));

    // Query exceeding the extent [500, 1100) should fail
    EXPECT_FALSE(index.has_extent(id, first_byte_offset_t{500}, 600));

    // Query starting before extent start should fail (no extent at offset -100)
    EXPECT_FALSE(index.has_extent(id, first_byte_offset_t{1100}, 100));
}

} // namespace cloud_topics
