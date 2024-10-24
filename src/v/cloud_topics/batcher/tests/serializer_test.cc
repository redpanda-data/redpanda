/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/batcher/serializer.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"

#include <seastar/core/circular_buffer.hh>

#include <gtest/gtest.h>

namespace cloud_topics = experimental::cloud_topics;

TEST(SerializerTest, EmptyReader) {
    auto res = cloud_topics::details::serialize_in_memory_record_batch_reader(
                 model::make_empty_record_batch_reader())
                 .get();
    ASSERT_TRUE(res.payload.empty());
    ASSERT_TRUE(res.batches.empty());
}

class SerializerFixture
  : public ::testing::TestWithParam<std::tuple<int, int>> {};

TEST_P(SerializerFixture, Consume) {
    auto num_batches = std::get<0>(GetParam());
    auto num_records = std::get<1>(GetParam());
    auto test_data = model::make_memory_record_batch_reader(
      model::test::make_random_batches(
        {.count = num_batches, .records = num_records})
        .get());
    auto res = cloud_topics::details::serialize_in_memory_record_batch_reader(
                 std::move(test_data))
                 .get();
    ASSERT_GT(res.payload.size_bytes(), 0);
    ASSERT_EQ(res.batches.size(), num_batches);
    ASSERT_TRUE(
      res.batches.back().physical_offset + res.batches.back().size_bytes
      == res.payload.size_bytes());
}

INSTANTIATE_TEST_SUITE_P(
  SerializerRoundTrip,
  SerializerFixture,
  ::testing::Combine(::testing::Range(1, 10), ::testing::Range(0, 10)));
