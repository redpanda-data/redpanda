// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/bytes.h"
#include "compaction/reducer.h"
#include "compaction/tests/simple_reducer.h"
#include "container/chunked_circular_buffer.h"
#include "model/fundamental.h"
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
