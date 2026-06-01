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
