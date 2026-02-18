// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/produce_validation.h"
#include "model/namespace.h"
#include "model/record.h"
#include "model/tests/random_batch.h"
#include "storage/record_batch_builder.h"

#include <gtest/gtest.h>

namespace {

const model::ntp test_ntp(
  model::kafka_namespace, model::topic("test-topic"), model::partition_id(0));

model::record_batch make_batch(int num_records = 3) {
    return model::test::make_random_batch(
      model::test::record_batch_spec{
        .offset = model::offset(0),
        .allow_compression = false,
        .count = num_records,
      });
}

model::record_batch make_batch_mutate_header(
  std::function<void(model::record_batch_header&)> mutate) {
    auto batch = make_batch();
    mutate(batch.header());
    batch.header().reset_size_checksum_metadata(batch.data());
    return batch;
}

} // namespace

class ValidateBatchHeaderStrictTest : public ::testing::Test {};

TEST_F(ValidateBatchHeaderStrictTest, RejectNonZeroBaseOffset) {
    auto batch = make_batch_mutate_header(
      [](auto& hdr) { hdr.base_offset = model::offset(42); });
    auto res = kafka::testing::validate_batch_header_strict(batch, test_ntp);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->err, kafka::error_code::invalid_record);
}

TEST_F(ValidateBatchHeaderStrictTest, RejectZeroRecordCount) {
    auto batch = make_batch_mutate_header(
      [](auto& hdr) { hdr.record_count = 0; });
    auto res = kafka::testing::validate_batch_header_strict(batch, test_ntp);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->err, kafka::error_code::invalid_record);
}

TEST_F(ValidateBatchHeaderStrictTest, RejectNegativeLastOffsetDelta) {
    auto batch = make_batch_mutate_header(
      [](auto& hdr) { hdr.last_offset_delta = -1; });
    auto res = kafka::testing::validate_batch_header_strict(batch, test_ntp);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->err, kafka::error_code::invalid_record);
}

TEST_F(ValidateBatchHeaderStrictTest, RejectOffsetCountMismatch) {
    auto batch = make_batch_mutate_header(
      [](auto& hdr) { hdr.last_offset_delta = 10; });
    auto res = kafka::testing::validate_batch_header_strict(batch, test_ntp);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->err, kafka::error_code::invalid_record);
}

TEST_F(ValidateBatchHeaderStrictTest, RejectControlBatch) {
    auto batch = make_batch_mutate_header(
      [](auto& hdr) { hdr.attrs.set_control_type(); });
    auto res = kafka::testing::validate_batch_header_strict(batch, test_ntp);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->err, kafka::error_code::invalid_record);
}

TEST_F(ValidateBatchHeaderStrictTest, RejectAppendTimeAttribute) {
    auto batch = make_batch_mutate_header([](auto& hdr) {
        hdr.attrs.set_timestamp_type(model::timestamp_type::append_time);
    });
    auto res = kafka::testing::validate_batch_header_strict(batch, test_ntp);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->err, kafka::error_code::invalid_record);
}

TEST_F(ValidateBatchHeaderStrictTest, RejectNegativeSequenceNum) {
    auto batch = make_batch_mutate_header([](auto& hdr) {
        hdr.producer_id = 1;
        hdr.base_sequence = -3;
    });
    auto res = kafka::testing::validate_batch_header_strict(batch, test_ntp);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res->err, kafka::error_code::invalid_record);
}

TEST_F(ValidateBatchHeaderStrictTest, AcceptValidBatch) {
    auto batch = make_batch();
    auto res = kafka::testing::validate_batch_header_strict(batch, test_ntp);
    EXPECT_FALSE(res.has_value());
}
