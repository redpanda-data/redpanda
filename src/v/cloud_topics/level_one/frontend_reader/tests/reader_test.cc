/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iostream.h"
#include "cloud_topics/level_one/common/fake_io.h"
#include "cloud_topics/level_one/common/object.h"
#include "cloud_topics/level_one/common/object_id.h"
#include "cloud_topics/level_one/frontend_reader/l1_reader_cache.h"
#include "cloud_topics/level_one/frontend_reader/level_one_reader.h"
#include "cloud_topics/level_one/frontend_reader/tests/l1_reader_fixture.h"
#include "cloud_topics/level_one/metastore/simple_metastore.h"
#include "cloud_topics/log_reader_config.h"
#include "container/chunked_circular_buffer.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

#include <chrono>
#include <map>
#include <optional>

using namespace cloud_topics;
using namespace std::chrono_literals;

namespace {

chunked_circular_buffer<model::record_batch>
copy(chunked_circular_buffer<model::record_batch>& input) {
    chunked_circular_buffer<model::record_batch> ret;
    for (auto& b : input) {
        ret.push_back(b.share());
    }
    return ret;
}

// Slice by index range (copy-based, preserves original)
chunked_circular_buffer<model::record_batch> slice(
  chunked_circular_buffer<model::record_batch>& source,
  size_t start,
  size_t count) {
    chunked_circular_buffer<model::record_batch> result;
    for (size_t i = 0; i < count; ++i) {
        result.push_back(source[start + i].share());
    }
    return result;
}

// Slice batches by offset range (doesn't split batches).
chunked_circular_buffer<model::record_batch> slice_by_offset(
  chunked_circular_buffer<model::record_batch>& source,
  kafka::offset start_offset,
  kafka::offset end_offset) {
    chunked_circular_buffer<model::record_batch> result;
    for (auto& batch : source) {
        if (
          batch.last_offset() >= kafka::offset_cast(start_offset)
          && batch.base_offset() <= kafka::offset_cast(end_offset)) {
            result.push_back(batch.share());
        }
    }
    return result;
}

} // anonymous namespace

// ---------------------------------------------------------------------------
// Parameterized fixture: runs each test with and without the reader cache.
// GetParam() == true means cache is enabled.
// ---------------------------------------------------------------------------

class l1_reader_test
  : public l1::l1_reader_fixture
  , public ::testing::WithParamInterface<bool> {
public:
    l1_reader_test() { _cache_ptr = GetParam() ? &_cache : nullptr; }
};

INSTANTIATE_TEST_SUITE_P(
  WithAndWithoutCache,
  l1_reader_test,
  ::testing::Bool(),
  [](const ::testing::TestParamInfo<bool>& info) {
      return info.param ? "WithCache" : "WithoutCache";
  });

// ---------------------------------------------------------------------------
// Correctness tests — parameterized over cache/no-cache.
// ---------------------------------------------------------------------------

TEST_P(l1_reader_test, empty_read) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    auto reader = make_reader(ntp, tidp);

    auto result = read_all(std::move(reader));

    EXPECT_TRUE(result.empty());
}

TEST_P(l1_reader_test, read_single_object) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto batches = model::test::make_random_batches(model::offset{0}, 10).get();
    auto expected = copy(batches);

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    // Read everything.
    auto reader = make_reader(ntp, tidp);
    auto result = read_all(std::move(reader));
    EXPECT_EQ(result, expected);
}

TEST_P(l1_reader_test, read_multiple_objects) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto batches
      = model::test::make_random_batches(model::offset{0}, 250).get();

    for (auto start = 0; start <= 200; start += 50) {
        std::vector<tidp_batches_t> tidp_batches;
        auto subbatches = slice(batches, start, 50);
        tidp_batches.emplace_back(tidp, std::move(subbatches));
        make_l1_objects(std::move(tidp_batches)).get();
    }

    auto reader = make_reader(ntp, tidp);
    auto result = read_all(std::move(reader));

    EXPECT_EQ(result, batches);
}

TEST_P(l1_reader_test, read_multiple_ntps_multiple_objects) {
    auto ntps = std::vector<std::pair<model::ntp, model::topic_id_partition>>{
      make_ntidp("tapioca"), make_ntidp("taco"), make_ntidp("turkey")};

    std::vector<chunked_circular_buffer<model::record_batch>> batches;
    batches.push_back(
      model::test::make_random_batches(model::offset{0}, 250).get());
    batches.push_back(
      model::test::make_random_batches(model::offset{0}, 250).get());
    batches.push_back(
      model::test::make_random_batches(model::offset{0}, 250).get());

    for (auto start = 0; start <= 200; start += 50) {
        std::vector<tidp_batches_t> tidp_batches;
        for (auto i = 0; i < 3; i++) {
            auto subbatches = slice(batches[i], start, 50);
            tidp_batches.emplace_back(ntps[i].second, std::move(subbatches));
        }
        make_l1_objects(std::move(tidp_batches)).get();
    }

    for (auto i = 0; i < 3; i++) {
        auto [ntp, tidp] = ntps[i];
        auto reader = make_reader(ntp, tidp);
        auto result = read_all(std::move(reader));

        EXPECT_EQ(result, batches[i]);
    }
}

TEST_P(l1_reader_test, read_offset_range_one_object) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto batches = model::test::make_random_batches(model::offset{0}, 10).get();
    auto expected = copy(batches);

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    auto min = kafka::offset{10};
    auto max = kafka::offset{250};
    auto reader = make_reader(ntp, tidp, min, max);
    auto result = read_all(std::move(reader));
    EXPECT_EQ(result, slice_by_offset(expected, min, max));
}

TEST_P(l1_reader_test, read_offset_range_multiple_objects) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto batches = model::test::make_random_batches(model::offset{0}, 20).get();

    {
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, slice(batches, 0, 10));
        make_l1_objects(std::move(tidp_batches)).get();
    }
    {
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, slice(batches, 10, 10));
        make_l1_objects(std::move(tidp_batches)).get();
    }

    auto offset_in_second_obj = batches[15].base_offset();
    auto min = kafka::offset{10};
    auto max = model::offset_cast(offset_in_second_obj);
    auto reader = make_reader(ntp, tidp, min, max);
    auto result = read_all(std::move(reader));
    EXPECT_EQ(result, slice_by_offset(batches, min, max));
}

TEST_P(l1_reader_test, read_with_max_bytes) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    // Use a pretty big number so we don't randomly get too many small batches.
    auto batches
      = model::test::make_random_batches(model::offset{0}, 100).get();
    auto expected = copy(batches);

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    {
        // Set tiny max bytes to check we get one batch.
        auto reader = make_reader(
          ntp, tidp, kafka::offset::min(), kafka::offset::max(), 1);
        auto result = read_all(std::move(reader));
        EXPECT_EQ(result, slice(expected, 0, 1));
    }

    {
        // Set a bigger max bytes to check we get some batches but not all.
        auto reader = make_reader(
          ntp, tidp, kafka::offset::min(), kafka::offset::max(), 10_KiB);
        auto result = read_all(std::move(reader));
        EXPECT_LT(result.size(), expected.size());
    }
}

TEST_P(l1_reader_test, read_with_strict_max_bytes) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    // Use a pretty big number so we don't randomly get too many small batches.
    auto batches
      = model::test::make_random_batches(model::offset{0}, 100).get();
    auto expected = copy(batches);

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    {
        // Set tiny max bytes to check we get no batches
        auto reader = make_reader(
          ntp, tidp, kafka::offset::min(), kafka::offset::max(), 1, true);
        auto result = read_all(std::move(reader));
        EXPECT_TRUE(result.empty());
    }

    {
        // Set a bigger max bytes to check we get some batches but not all.
        auto reader = make_reader(
          ntp, tidp, kafka::offset::min(), kafka::offset::max(), 10_KiB, true);
        auto result = read_all(std::move(reader));
        EXPECT_LT(result.size(), expected.size());
    }
}

TEST_P(l1_reader_test, out_of_range) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    // Start the object at offset 100.
    auto batches
      = model::test::make_random_batches(model::offset{100}, 10).get();
    auto last_offset = model::offset_cast(batches.back().last_offset());

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    // Test reading before the available range.
    {
        auto reader = make_reader(
          ntp, tidp, kafka::offset{0}, kafka::offset{99});
        auto result = read_all(std::move(reader));
        EXPECT_TRUE(result.empty());
    }

    // Test reading after the available range.
    {
        auto reader = make_reader(ntp, tidp, kafka::next_offset(last_offset));
        auto result = read_all(std::move(reader));
        EXPECT_TRUE(result.empty());
    }
}

TEST_P(l1_reader_test, missing_object) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    // Register object in metastore but don't upload.
    // This is corruption and readers should throw.
    auto builder = _metastore.object_builder().get().value();
    auto oid = builder->get_or_create_object_for(tidp).get().value();
    builder
      ->add(
        oid,
        l1::metastore::object_metadata::ntp_metadata{
          .tidp = tidp,
          .base_offset = kafka::offset{0},
          .last_offset = kafka::offset{9},
          .max_timestamp = model::timestamp::now(),
          .pos = 0,
          .size = 1000,
        })
      .value();
    builder->finish(oid, 1001, 1500).value();

    l1::metastore::term_offset_map_t term_map;
    term_map[tidp].push_back(
      l1::metastore::term_offset{
        .term = model::term_id{1},
        .first_offset = kafka::offset{0},
      });

    std::ignore = _metastore.add_objects(*builder, term_map).get().value();

    auto reader = make_reader(ntp, tidp);
    EXPECT_THROW(read_all(std::move(reader)), std::runtime_error);
}

TEST_P(l1_reader_test, empty_offset_range) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    // Write some initial objects.
    auto batches
      = model::test::make_random_batches(model::offset{0}, 250).get();

    for (auto start = 0; start <= 200; start += 50) {
        std::vector<tidp_batches_t> tidp_batches;
        auto subbatches = slice(batches, start, 50);
        tidp_batches.emplace_back(tidp, std::move(subbatches));
        make_l1_objects(std::move(tidp_batches)).get();
    }

    // Mimic an object from which all partition data has been compacted.
    // The object has no data for the partition, but is registered
    // to cover a non-empty offset range in the metastore.
    auto meta_builder = _metastore.object_builder().get().value();

    auto oid = meta_builder->get_or_create_object_for(tidp).get().value();

    auto buf = iobuf{};
    auto builder = l1::object_builder::create(
      make_iobuf_ref_output_stream(buf), {});

    auto obj_info = builder->finish().get();
    builder->close().get();

    _io.put_object(oid, std::move(buf));

    auto high_watermark = kafka::next_offset(
      model::offset_cast(batches.back().last_offset()));
    meta_builder
      ->add(
        oid,
        l1::metastore::object_metadata::ntp_metadata{
          .tidp = tidp,
          .base_offset = high_watermark,
          .last_offset = high_watermark,
          .max_timestamp = model::timestamp::now(),
          .pos = 0,
          .size = 0,
        })
      .value();

    meta_builder->finish(oid, obj_info.footer_offset, obj_info.size_bytes)
      .value();

    l1::metastore::term_offset_map_t term_map;
    term_map[tidp].push_back(
      l1::metastore::term_offset{
        .term = model::term_id{1},
        .first_offset = high_watermark,
      });
    std::ignore = _metastore.add_objects(*meta_builder, term_map).get().value();

    // Write some objects after the empty object.
    auto final_batches
      = model::test::make_random_batches(
          kafka::offset_cast(kafka::next_offset(high_watermark)), 250)
          .get();

    for (auto start = 0; start <= 200; start += 50) {
        std::vector<tidp_batches_t> tidp_batches;
        auto subbatches = slice(final_batches, start, 50);
        tidp_batches.emplace_back(tidp, std::move(subbatches));
        make_l1_objects(std::move(tidp_batches)).get();
    }

    auto reader = make_reader(ntp, tidp);
    auto result = read_all(std::move(reader));

    batches.insert(
      batches.end(),
      std::make_move_iterator(final_batches.begin()),
      std::make_move_iterator(final_batches.end()));
    EXPECT_EQ(result, batches);
}

TEST_P(l1_reader_test, sparse_offset_ranges) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto all_batches
      = model::test::make_random_batches(model::offset{0}, 250).get();

    // Slice up the range of batches, and punch gaps within the slices.
    // This simulates compaction swiss-cheesing the log.
    auto slice0 = slice(all_batches, 0, 50);
    auto slice1 = slice(all_batches, 50, 150);
    auto slice2 = slice(all_batches, 200, 50);

    slice0.erase(slice0.begin() + 5, slice0.begin() + 25);
    slice1.erase(slice1.begin() + 10, slice1.begin() + 35);
    slice2.erase(slice2.begin() + 15, slice2.begin() + 30);

    {
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, copy(slice0));
        make_l1_objects(std::move(tidp_batches)).get();
    }
    {
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, copy(slice1));
        make_l1_objects(std::move(tidp_batches)).get();
    }
    {
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, copy(slice2));
        make_l1_objects(std::move(tidp_batches)).get();
    }

    auto reader = make_reader(ntp, tidp);
    auto result = read_all(std::move(reader));

    chunked_circular_buffer<model::record_batch> batches;
    batches.insert(
      batches.end(),
      std::make_move_iterator(slice0.begin()),
      std::make_move_iterator(slice0.end()));
    batches.insert(
      batches.end(),
      std::make_move_iterator(slice1.begin()),
      std::make_move_iterator(slice1.end()));
    batches.insert(
      batches.end(),
      std::make_move_iterator(slice2.begin()),
      std::make_move_iterator(slice2.end()));

    EXPECT_EQ(result, batches);
}

TEST_P(l1_reader_test, max_bytes_zero_behavior) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    // Create an object with multiple batches
    auto batches = model::test::make_random_batches(model::offset{0}, 10).get();
    auto expected = copy(batches);

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    {
        // Test max_bytes=0 with strict_max_bytes=false
        // Should return exactly one batch (non-strict mode allows at least one)
        auto reader = make_reader(
          ntp, tidp, kafka::offset::min(), kafka::offset::max(), 0, false);
        auto result = read_all(std::move(reader));
        EXPECT_EQ(result.size(), 1);
        EXPECT_EQ(result[0], expected[0]);
    }

    {
        // Test max_bytes=0 with strict_max_bytes=true
        // Should return zero batches (strict mode respects the byte limit)
        auto reader = make_reader(
          ntp, tidp, kafka::offset::min(), kafka::offset::max(), 0, true);
        auto result = read_all(std::move(reader));
        EXPECT_TRUE(result.empty());
    }
}

TEST_P(l1_reader_test, read_offset_range_multiple_objects2) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    /*
     * Populate test with 50 L1 objects. Each batch will contain 10 records.
     */
    auto source_batches = model::test::make_random_batches(
                            model::offset{0}, 50, false, std::nullopt, 10)
                            .get();

    /*
     * After the source batches, add a bunch of batches that are
     * predictably smaller than the ones created above (1 record vs 10 records).
     * We add a bunch because we need one of these smaller batches to land as
     * the first batch in an L1 object.
     */
    {
        auto small_batches = model::test::make_random_batches(
                               source_batches.back().last_offset()
                                 + model::offset{1},
                               100,
                               false,
                               std::nullopt,
                               1)
                               .get();
        for (auto& batch : small_batches) {
            source_batches.push_back(std::move(batch));
        }
    }

    // Populate all the the l1 objects from the 50 batches
    for (int i = 0; i < 15; ++i) {
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, slice(source_batches, i * 10, 10));
        make_l1_objects(std::move(tidp_batches)).get();
    }

    /*
     * Calculate max_bytes for the reader used in the next phase of the test.
     * What we want is a value that will be rejected by the speculative size
     * check in reader::read_batches, but not be rejected by the size check in
     * reader::fetch_metadata.
     *
     * When this condition is encountered the reader will bump the next offset
     * (read_batches returns 0 batches because byte limit has been exceeded),
     * but the higher level reader won't observe byte limits exceeded and
     * transition to end-of-stream state because the speculative byte limit
     * exceeded doesn't bump consumed bytes (or do anything else that causes the
     * reader to enter end-of-stream state).
     *
     * So the reader will keep going. If it happens to encounter a small enough
     * batch, one that is small enough be accepted by the speculative limit
     * check, then the batch will be read and cause the reader to return a batch
     * that skips offsets / creates a gap to the reader. The only time gaps are
     * allowed is when there are true gaps, such as those caused by compaction.
     *
     * NOTE: the bug that exists further requires that the small batch that ends
     * up being accepted is the first batch in an L1 object because
     * reader::read_batches will stop reading from an object as soon as
     * speculative byte limit is exceeeded, thus the errant acceptance needs to
     * be the first batch.
     */
    size_t max_bytes = 0;
    {
        auto batches = model::consume_reader_to_memory(
                         make_reader(ntp, tidp), model::no_timeout)
                         .get();
        ASSERT_EQ(batches.size(), 150);

        auto expected_offset = batches.front().base_offset();
        for (const auto& batch : batches) {
            ASSERT_EQ(batch.base_offset(), expected_offset);
            expected_offset += model::offset(batch.record_count() - 1);
            ASSERT_EQ(batch.last_offset(), expected_offset);
            expected_offset += 1;
        }

        // accumulated size of the first three batches
        max_bytes += batches[0].size_bytes();
        max_bytes += batches[1].size_bytes();
        max_bytes += batches[2].size_bytes();

        // add in enough extra allowance for one of the smaller batches so that
        // when the reader will accept it speculatively.
        max_bytes += batches[100].size_bytes() + 1;
    }

    auto batches
      = model::consume_reader_to_memory(
          make_reader(
            ntp, tidp, kafka::offset{0}, kafka::offset::max(), max_bytes),
          model::no_timeout)
          .get();

    EXPECT_EQ(batches.size(), 3);

    auto expected_offset = batches.front().base_offset();
    for (const auto& batch : batches) {
        EXPECT_EQ(batch.base_offset(), expected_offset);
        expected_offset += model::offset(batch.record_count() - 1);
        EXPECT_EQ(batch.last_offset(), expected_offset);
        expected_offset += 1;
    }
}

// ---------------------------------------------------------------------------
// Cache-specific tests — always use cache, and prove it is exercised.
// ---------------------------------------------------------------------------

class l1_reader_cache_test : public l1::l1_reader_fixture {};

// Verify that partial reads populate the cache, that multiple entries
// coexist for the same NTP, and that borrowing each produces a correct
// continuation.
TEST_F(l1_reader_cache_test, cache_is_populated_and_borrowed) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto batches
      = model::test::make_random_batches(model::offset{0}, 100).get();
    auto expected = copy(batches);

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    // Two partial reads at different split points populate two cache
    // entries for the same NTP.
    auto split_a = expected.size() / 3;
    auto split_b = 2 * expected.size() / 3;
    auto max_a = model::offset_cast(expected[split_a].last_offset());
    auto max_b = model::offset_cast(expected[split_b].last_offset());

    auto result_a = read_all(make_reader(ntp, tidp, kafka::offset{0}, max_a));
    ASSERT_FALSE(result_a.empty());
    auto next_a = kafka::next_offset(
      model::offset_cast(result_a.back().last_offset()));

    // Second read starts at 0 — cache miss (cached entry is at
    // next_a), so both entries coexist in the cache afterward.
    auto result_b = read_all(make_reader(ntp, tidp, kafka::offset{0}, max_b));
    ASSERT_FALSE(result_b.empty());
    auto next_b = kafka::next_offset(
      model::offset_cast(result_b.back().last_offset()));

    // Both entries should be independently borrowable.
    auto cached_a = _cache.take_reader(tidp, next_a);
    ASSERT_TRUE(cached_a.has_value())
      << "Expected cache entry at next_offset " << next_a;
    EXPECT_EQ(cached_a->next_offset, next_a);

    auto cached_b = _cache.take_reader(tidp, next_b);
    ASSERT_TRUE(cached_b.has_value())
      << "Expected cache entry at next_offset " << next_b;
    EXPECT_EQ(cached_b->next_offset, next_b);

    // Return both so subsequent reads can use them.
    _cache.return_reader(tidp, std::move(*cached_a)).get();
    _cache.return_reader(tidp, std::move(*cached_b)).get();

    // Continue from each cached position and verify correctness.
    auto tail_a = read_all(make_reader(ntp, tidp, next_a));
    auto tail_b = read_all(make_reader(ntp, tidp, next_b));

    chunked_circular_buffer<model::record_batch> combined_a;
    for (auto& b : result_a) {
        combined_a.push_back(std::move(b));
    }
    for (auto& b : tail_a) {
        combined_a.push_back(std::move(b));
    }
    EXPECT_EQ(combined_a, expected);

    chunked_circular_buffer<model::record_batch> combined_b;
    for (auto& b : result_b) {
        combined_b.push_back(std::move(b));
    }
    for (auto& b : tail_b) {
        combined_b.push_back(std::move(b));
    }
    EXPECT_EQ(combined_b, expected);
}

// Verify that a non-sequential read (different offset from the cached
// position) falls back to the standard metastore-lookup path correctly.
TEST_F(l1_reader_cache_test, cache_miss_different_offset) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto batches
      = model::test::make_random_batches(model::offset{0}, 100).get();
    auto expected = copy(batches);

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    // Read the first half — this caches a reader positioned mid-object.
    auto split_idx = expected.size() / 2;
    auto max_offset = model::offset_cast(expected[split_idx].last_offset());
    auto partial = read_all(
      make_reader(ntp, tidp, kafka::offset{0}, max_offset));
    ASSERT_FALSE(partial.empty());

    // Now read from the beginning again — cache miss since the cached reader
    // is positioned at a later offset. Should still produce correct results.
    auto full = read_all(make_reader(ntp, tidp));
    EXPECT_EQ(full, expected);
}

// Verify that reading across two L1 objects works correctly when the cache
// holds a reader for the first object.
TEST_F(l1_reader_cache_test, cache_cross_object_transition) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto batches
      = model::test::make_random_batches(model::offset{0}, 100).get();
    auto expected = copy(batches);

    // Split into two objects.
    {
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, slice(batches, 0, 50));
        make_l1_objects(std::move(tidp_batches)).get();
    }
    {
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, slice(batches, 50, 50));
        make_l1_objects(std::move(tidp_batches)).get();
    }

    // Read partial from the first object — stop well before the boundary.
    auto mid_first_obj = expected.size() / 4;
    auto max_offset = model::offset_cast(expected[mid_first_obj].last_offset());
    auto first_result = read_all(
      make_reader(ntp, tidp, kafka::offset{0}, max_offset));
    ASSERT_FALSE(first_result.empty());

    // Continue from where first reader stopped — should use cache for
    // remainder of first object, then transition to second object.
    auto next_start = kafka::next_offset(
      model::offset_cast(first_result.back().last_offset()));
    auto second_result = read_all(make_reader(ntp, tidp, next_start));
    ASSERT_FALSE(second_result.empty());

    chunked_circular_buffer<model::record_batch> combined;
    for (auto& b : first_result) {
        combined.push_back(std::move(b));
    }
    for (auto& b : second_result) {
        combined.push_back(std::move(b));
    }
    EXPECT_EQ(combined, expected);
}

// Verify that multiple sequential reads, each with a different max_offset
// boundary, all work correctly through the reader cache.
TEST_F(l1_reader_cache_test, cache_multiple_sequential_reads) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto batches
      = model::test::make_random_batches(model::offset{0}, 100).get();
    auto expected = copy(batches);

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    // Split into 4 sequential reads.
    chunked_circular_buffer<model::record_batch> combined;
    auto current_start = kafka::offset{0};
    size_t quarter = expected.size() / 4;

    for (size_t i = 0; i < 4; ++i) {
        kafka::offset max_off = (i < 3)
                                  ? model::offset_cast(
                                      expected[(i + 1) * quarter].last_offset())
                                  : kafka::offset::max();
        auto result = read_all(make_reader(ntp, tidp, current_start, max_off));
        ASSERT_FALSE(result.empty()) << "Empty result on iteration " << i;
        current_start = kafka::next_offset(
          model::offset_cast(result.back().last_offset()));
        for (auto& b : result) {
            combined.push_back(std::move(b));
        }
    }

    EXPECT_EQ(combined, expected);
}

// Verify that the reader cache works across byte-limit boundaries.
TEST_F(l1_reader_cache_test, cache_strict_max_bytes_resume) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto batches
      = model::test::make_random_batches(model::offset{0}, 100).get();
    auto expected = copy(batches);

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    size_t quarter = expected.size() / 4;
    size_t byte_limit = 0;
    for (size_t i = 0; i < quarter; ++i) {
        byte_limit += expected[i].size_bytes();
    }

    auto first_result = read_all(make_reader(
      ntp,
      tidp,
      kafka::offset{0},
      kafka::offset::max(),
      byte_limit,
      /*strict_max_bytes=*/true));
    ASSERT_FALSE(first_result.empty());
    ASSERT_LE(first_result.size(), quarter);

    auto next_start = kafka::next_offset(
      model::offset_cast(first_result.back().last_offset()));

    auto second_result = read_all(make_reader(ntp, tidp, next_start));
    ASSERT_FALSE(second_result.empty());

    chunked_circular_buffer<model::record_batch> combined;
    for (auto& b : first_result) {
        combined.push_back(std::move(b));
    }
    for (auto& b : second_result) {
        combined.push_back(std::move(b));
    }
    EXPECT_EQ(combined, expected);
}

// Lookahead tests: verify that lookahead_objects > 1 produces the same results
// as the default single-object lookup path.
TEST_P(l1_reader_test, lookahead_single_object) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto batches = model::test::make_random_batches(model::offset{0}, 10).get();
    auto expected = copy(batches);

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(std::move(tidp_batches)).get();

    // Read with lookahead enabled — should produce identical results.
    auto reader = make_reader(
      ntp,
      tidp,
      kafka::offset{0},
      kafka::offset::max(),
      std::numeric_limits<size_t>::max(),
      /*strict_max_bytes=*/false,
      /*lookahead_objects=*/10);
    auto result = read_all(std::move(reader));
    EXPECT_EQ(result, expected);
}

TEST_P(l1_reader_test, lookahead_multiple_objects) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    // Create 3 separate objects with batches at increasing offsets.
    auto batches1 = model::test::make_random_batches(model::offset{0}, 5).get();
    auto next_offset = batches1.back().last_offset() + model::offset{1};
    auto batches2 = model::test::make_random_batches(next_offset, 5).get();
    next_offset = batches2.back().last_offset() + model::offset{1};
    auto batches3 = model::test::make_random_batches(next_offset, 5).get();

    // Collect expected batches.
    chunked_circular_buffer<model::record_batch> expected;
    for (auto& b : batches1) {
        expected.push_back(b.share());
    }
    for (auto& b : batches2) {
        expected.push_back(b.share());
    }
    for (auto& b : batches3) {
        expected.push_back(b.share());
    }

    // Create three separate L1 objects.
    {
        std::vector<tidp_batches_t> tb;
        tb.emplace_back(tidp, std::move(batches1));
        make_l1_objects(std::move(tb)).get();
    }
    {
        std::vector<tidp_batches_t> tb;
        tb.emplace_back(tidp, std::move(batches2));
        make_l1_objects(std::move(tb)).get();
    }
    {
        std::vector<tidp_batches_t> tb;
        tb.emplace_back(tidp, std::move(batches3));
        make_l1_objects(std::move(tb)).get();
    }

    // Read with lookahead=10, which should batch-lookup all 3 objects.
    auto reader = make_reader(
      ntp,
      tidp,
      kafka::offset{0},
      kafka::offset::max(),
      std::numeric_limits<size_t>::max(),
      /*strict_max_bytes=*/false,
      /*lookahead_objects=*/10);
    auto result = read_all(std::move(reader));
    EXPECT_EQ(result, expected);

    // Also verify without lookahead (regression test).
    auto reader_no_prefetch = make_reader(ntp, tidp);
    auto result_no_prefetch = read_all(std::move(reader_no_prefetch));
    EXPECT_EQ(result_no_prefetch, expected);
}
