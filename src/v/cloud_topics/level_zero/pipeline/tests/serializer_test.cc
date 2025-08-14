/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_zero/pipeline/serializer.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"

#include <gtest/gtest.h>

#include <iterator>

namespace cloud_topics = experimental::cloud_topics;

TEST(SerializerTest, EmptyReader) {
    auto res = cloud_topics::l0::serialize_batches({}).get();
    ASSERT_TRUE(res.payload.empty());
    ASSERT_TRUE(res.extents.empty());
}

class SerializerFixture
  : public ::testing::TestWithParam<std::tuple<int, int>> {};

TEST_P(SerializerFixture, Consume) {
    auto num_batches = std::get<0>(GetParam());
    auto num_records = std::get<1>(GetParam());
    auto test_data = model::test::make_random_batches(
                       {.count = num_batches, .records = num_records})
                       .get();
    chunked_vector<model::record_batch> batches;
    std::ranges::move(std::move(test_data), std::back_inserter(batches));
    auto res = cloud_topics::l0::serialize_batches(std::move(batches)).get();
    ASSERT_GT(res.payload.size_bytes(), 0);
    ASSERT_EQ(res.extents.size(), num_batches);
    ASSERT_TRUE(
      res.extents.back().first_byte_offset()
        + res.extents.back().byte_range_size()
      == res.payload.size_bytes());
}

INSTANTIATE_TEST_SUITE_P(
  SerializerRoundTrip,
  SerializerFixture,
  ::testing::Combine(::testing::Range(1, 10), ::testing::Range(0, 10)));
