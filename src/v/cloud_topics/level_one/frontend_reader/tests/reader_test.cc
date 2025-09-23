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
#include "cloud_topics/level_one/frontend_reader/reader.h"
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

class l1_reader_test : public l1::l1_reader_fixture {};

TEST_F(l1_reader_test, empty_read) {
    auto [ntp, tidp] = make_ntidp("test_topic");
    auto reader = make_reader(ntp, tidp);

    auto result = read_all(std::move(reader));

    EXPECT_TRUE(result.empty());
}

TEST_F(l1_reader_test, read_single_object) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto batches = model::test::make_random_batches(model::offset{0}, 10).get();
    auto expected = copy(batches);

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(tidp_batches);

    // Read everything.
    auto reader = make_reader(ntp, tidp);
    auto result = read_all(std::move(reader));
    EXPECT_EQ(result, expected);
}

TEST_F(l1_reader_test, read_multiple_objects) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto batches
      = model::test::make_random_batches(model::offset{0}, 250).get();

    for (auto start = 0; start <= 200; start += 50) {
        std::vector<tidp_batches_t> tidp_batches;
        auto subbatches = slice(batches, start, 50);
        tidp_batches.emplace_back(tidp, std::move(subbatches));
        make_l1_objects(tidp_batches);
    }

    auto reader = make_reader(ntp, tidp);
    auto result = read_all(std::move(reader));

    EXPECT_EQ(result, batches);
}

TEST_F(l1_reader_test, read_multiple_ntps_multiple_objects) {
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
        make_l1_objects(tidp_batches);
    }

    for (auto i = 0; i < 3; i++) {
        auto [ntp, tidp] = ntps[i];
        auto reader = make_reader(ntp, tidp);
        auto result = read_all(std::move(reader));

        EXPECT_EQ(result, batches[i]);
    }
}

TEST_F(l1_reader_test, read_offset_range_one_object) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto batches = model::test::make_random_batches(model::offset{0}, 10).get();
    auto expected = copy(batches);

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(tidp_batches);

    auto min = kafka::offset{10};
    auto max = kafka::offset{250};
    auto reader = make_reader(ntp, tidp, min, max);
    auto result = read_all(std::move(reader));
    EXPECT_EQ(result, slice_by_offset(expected, min, max));
}

TEST_F(l1_reader_test, read_offset_range_multiple_objects) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    auto batches = model::test::make_random_batches(model::offset{0}, 20).get();

    {
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, slice(batches, 0, 10));
        make_l1_objects(tidp_batches);
    }
    {
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, slice(batches, 10, 10));
        make_l1_objects(tidp_batches);
    }

    auto offset_in_second_obj = batches[15].base_offset();
    auto min = kafka::offset{10};
    auto max = model::offset_cast(offset_in_second_obj);
    auto reader = make_reader(ntp, tidp, min, max);
    auto result = read_all(std::move(reader));
    EXPECT_EQ(result, slice_by_offset(batches, min, max));
}

TEST_F(l1_reader_test, read_with_max_bytes) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    // Use a pretty big number so we don't randomly get too many small batches.
    auto batches
      = model::test::make_random_batches(model::offset{0}, 100).get();
    auto expected = copy(batches);

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(tidp_batches);

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

TEST_F(l1_reader_test, read_with_strict_max_bytes) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    // Use a pretty big number so we don't randomly get too many small batches.
    auto batches
      = model::test::make_random_batches(model::offset{0}, 100).get();
    auto expected = copy(batches);

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(tidp_batches);

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

TEST_F(l1_reader_test, out_of_range) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    // Start the object at offset 100.
    auto batches
      = model::test::make_random_batches(model::offset{100}, 10).get();
    auto last_offset = model::offset_cast(batches.back().last_offset());

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(tidp_batches);

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

TEST_F(l1_reader_test, missing_object) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    // Register object in metastore but don't upload.
    // This is corruption and readers should throw.
    auto builder = _metastore.object_builder();
    auto oid = builder->get_or_create_object_for(tidp);
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

    _metastore.add_objects(std::move(builder), term_map).get().value();

    auto reader = make_reader(ntp, tidp);
    EXPECT_THROW(read_all(std::move(reader)), std::runtime_error);
}

TEST_F(l1_reader_test, empty_offset_range) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    // Write some initial objects.
    auto batches
      = model::test::make_random_batches(model::offset{0}, 250).get();

    for (auto start = 0; start <= 200; start += 50) {
        std::vector<tidp_batches_t> tidp_batches;
        auto subbatches = slice(batches, start, 50);
        tidp_batches.emplace_back(tidp, std::move(subbatches));
        make_l1_objects(tidp_batches);
    }

    // Mimic an object from which all partition data has been compacted.
    // The object has no data for the partition, but is registered
    // to cover a non-empty offset range in the metastore.
    auto meta_builder = _metastore.object_builder();

    auto oid = meta_builder->get_or_create_object_for(tidp);

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
    _metastore.add_objects(std::move(meta_builder), term_map).get().value();

    // Write some objects after the empty object.
    auto final_batches
      = model::test::make_random_batches(
          kafka::offset_cast(kafka::next_offset(high_watermark)), 250)
          .get();

    for (auto start = 0; start <= 200; start += 50) {
        std::vector<tidp_batches_t> tidp_batches;
        auto subbatches = slice(final_batches, start, 50);
        tidp_batches.emplace_back(tidp, std::move(subbatches));
        make_l1_objects(tidp_batches);
    }

    auto reader = make_reader(ntp, tidp);
    auto result = read_all(std::move(reader));

    batches.insert(
      batches.end(),
      std::make_move_iterator(final_batches.begin()),
      std::make_move_iterator(final_batches.end()));
    EXPECT_EQ(result, batches);
}

TEST_F(l1_reader_test, sparse_offset_ranges) {
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
        make_l1_objects(tidp_batches);
    }
    {
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, copy(slice1));
        make_l1_objects(tidp_batches);
    }
    {
        std::vector<tidp_batches_t> tidp_batches;
        tidp_batches.emplace_back(tidp, copy(slice2));
        make_l1_objects(tidp_batches);
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

TEST_F(l1_reader_test, max_bytes_zero_behavior) {
    auto [ntp, tidp] = make_ntidp("test_topic");

    // Create an object with multiple batches
    auto batches = model::test::make_random_batches(model::offset{0}, 10).get();
    auto expected = copy(batches);

    std::vector<tidp_batches_t> tidp_batches;
    tidp_batches.emplace_back(tidp, std::move(batches));
    make_l1_objects(tidp_batches);

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
