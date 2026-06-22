// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/bytes.h"
#include "compaction/key.h"
#include "compaction/key_offset_map.h"
#include "compaction/reducer.h"
#include "compaction/tests/simple_reducer.h"
#include "container/chunked_circular_buffer.h"
#include "model/batch_compression.h"
#include "model/fundamental.h"
#include "reflection/adl.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/batch_generators.h"

#include <seastar/core/coroutine.hh>

#include <gtest/gtest.h>

#include <limits>
#include <vector>

static const auto test_ntp = model::ntp(
  model::ns("kafka"), model::topic("tapioca"), model::partition_id(0));

TEST(CompactionReducerTest, SimpleReducer) {
    int num_batches = 10;
    auto gen = linear_int_kv_batch_generator();
    auto spec = model::test::record_batch_spec{
      .allow_compression = false, .count = 10};
    auto input_batches = gen(spec, num_batches);
    chunked_circular_buffer<model::record_batch> output_batches;

    auto src = std::make_unique<compaction::simple_source>(
      std::move(input_batches), test_ntp);
    auto sink = std::make_unique<compaction::simple_sink>(output_batches);
    auto reducer = compaction::sliding_window_reducer(
      std::move(src), std::move(sink));

    std::move(reducer).run().get();
    ASSERT_EQ(output_batches.size(), num_batches);
    linear_int_kv_batch_generator::validate_post_compaction(
      std::move(output_batches));
}

namespace {
// Runs the filter over a single uncompressed batch of `num_records` unique
// integer keys [0, num_records), removing the records whose key is in
// `removed` (by indexing them in the map at a higher offset). Returns the
// integer keys of the records present in the output, in order.
std::vector<int>
filter_kept_keys(model::record_batch batch, const std::vector<int>& removed) {
    compaction::simple_key_offset_map map{};
    for (int i : removed) {
        auto key = compaction::compaction_key{
          iobuf_to_bytes(reflection::to_iobuf(i))};
        // Index at an offset strictly above any record offset so the record is
        // treated as superseded and dropped.
        map.put(key, model::offset(std::numeric_limits<int>::max())).get();
    }

    chunked_circular_buffer<model::record_batch> output;
    compaction::simple_sink sink(output);
    compaction::simple_map_filter filter(sink, map, test_ntp);
    filter(std::move(batch)).get();

    std::vector<int> kept;
    if (!output.empty()) {
        output.front().for_each_record([&kept](model::record r) {
            kept.push_back(reflection::from_iobuf<int>(r.key().copy()));
        });
    }
    return kept;
}

model::record_batch make_int_key_batch(int num_records) {
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    for (int i = 0; i < num_records; ++i) {
        builder.add_raw_kv(reflection::to_iobuf(i), reflection::to_iobuf(i));
    }
    return std::move(builder).build();
}
} // namespace

// The rebuild loop matches records against the kept-deltas vector with an
// advancing cursor. Exercise the cases that distinguish a correct subsequence
// match from a position-based one: removing the first, middle, last, and
// non-contiguous sets of records.
TEST(CompactionReducerTest, FilterKeepsCorrectRecordsAfterRemoval) {
    constexpr int n = 8;
    using vec = std::vector<int>;
    EXPECT_EQ(
      filter_kept_keys(make_int_key_batch(n), {}),
      (vec{0, 1, 2, 3, 4, 5, 6, 7}));
    EXPECT_EQ(
      filter_kept_keys(make_int_key_batch(n), {0}), (vec{1, 2, 3, 4, 5, 6, 7}));
    EXPECT_EQ(
      filter_kept_keys(make_int_key_batch(n), {7}), (vec{0, 1, 2, 3, 4, 5, 6}));
    EXPECT_EQ(
      filter_kept_keys(make_int_key_batch(n), {3, 4}), (vec{0, 1, 2, 5, 6, 7}));
    EXPECT_EQ(
      filter_kept_keys(make_int_key_batch(n), {0, 2, 4, 6}), (vec{1, 3, 5, 7}));
    EXPECT_EQ(
      filter_kept_keys(make_int_key_batch(n), {0, 1, 2, 3, 4, 5, 6}), (vec{7}));
}

// A batch that has already been compacted carries non-contiguous offset deltas;
// the surviving records keep their original deltas. Feeding such a batch back
// through must match on those sparse deltas, not on record positions.
TEST(CompactionReducerTest, FilterKeepsCorrectRecordsWithNonContiguousDeltas) {
    using vec = std::vector<int>;
    // First pass removes the middle records {3, 4}, leaving keys/deltas
    // {0, 1, 2, 5, 6, 7}.
    chunked_circular_buffer<model::record_batch> output;
    {
        compaction::simple_key_offset_map map{};
        for (int i : {3, 4}) {
            auto key = compaction::compaction_key{
              iobuf_to_bytes(reflection::to_iobuf(i))};
            map.put(key, model::offset(std::numeric_limits<int>::max())).get();
        }
        compaction::simple_sink sink(output);
        compaction::simple_map_filter filter(sink, map, test_ntp);
        filter(make_int_key_batch(8)).get();
    }
    ASSERT_EQ(output.size(), 1);

    // Second pass over the sparse batch removes key 5 (delta 5).
    EXPECT_EQ(
      filter_kept_keys(std::move(output.front()), {5}), (vec{0, 1, 2, 6, 7}));
}

// When filtering removes nothing from a compressed batch, the filter should
// reuse the original compressed payload rather than re-compressing it.
TEST(CompactionReducerTest, FilterReusesCompressedBatchWhenUnchanged) {
    // Build a batch with unique keys (so nothing is removed) and compress it.
    constexpr int num_records = 5;
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));
    for (int i = 0; i < num_records; ++i) {
        builder.add_raw_kv(reflection::to_iobuf(i), reflection::to_iobuf(i));
    }
    auto batch = model::compress_batch_sync(
      model::compression::zstd, std::move(builder).build());
    ASSERT_TRUE(batch.compressed());
    ASSERT_EQ(batch.record_count(), num_records);

    compaction::simple_key_offset_map map{};
    chunked_circular_buffer<model::record_batch> output;
    compaction::simple_sink sink(output);
    compaction::simple_map_filter filter(sink, map, test_ntp);
    filter(std::move(batch)).get();

    auto stats = filter.end_of_stream();
    EXPECT_EQ(stats.records_discarded, 0);
    EXPECT_EQ(stats.compressed_batches_reused, 1);
    ASSERT_EQ(output.size(), 1);
    EXPECT_TRUE(output.front().compressed());
}
