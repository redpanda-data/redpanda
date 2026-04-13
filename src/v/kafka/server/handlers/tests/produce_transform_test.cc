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

#include "model/record.h"
#include "model/record_utils.h"
#include "model/tests/random_batch.h"
#include "model/timestamp.h"
#include "model/transform.h"

#include <seastar/core/chunked_fifo.hh>

#include <gtest/gtest.h>

namespace kafka {
namespace {

/// Build a source batch with idempotent-produce identity fields set,
/// simulating what a real Kafka producer would send.
model::record_batch make_idempotent_batch(int record_count) {
    return model::test::make_random_batch(
      {.allow_compression = false,
       .count = record_count,
       .enable_idempotence = true,
       .producer_id = 42,
       .producer_epoch = 7,
       .base_sequence = 100});
}

/// Extract records from a batch into transformed_data, the same way the
/// produce-path code collects engine output via the transform callback.
ss::chunked_fifo<model::transformed_data>
records_to_transformed(model::record_batch& batch) {
    ss::chunked_fifo<model::transformed_data> out;
    for (const auto& r : batch.copy_records()) {
        out.push_back(model::transformed_data::from_record(r.copy()));
    }
    return out;
}

/// Mirrors the batch reconstruction logic in produce.cc:
///   1. make_batch from transformed_data
///   2. transplant identity fields from the original header
model::record_batch reconstruct_batch(
  const model::record_batch_header& orig_header,
  ss::chunked_fifo<model::transformed_data> records) {
    auto new_batch = model::transformed_data::make_batch(
      orig_header.first_timestamp, std::move(records));
    new_batch.header().producer_id = orig_header.producer_id;
    new_batch.header().producer_epoch = orig_header.producer_epoch;
    new_batch.header().base_sequence = orig_header.base_sequence;
    new_batch.header().attrs = orig_header.attrs;
    return new_batch;
}

TEST(ProduceTransformTest, BatchReconstructionPreservesIdentity) {
    auto batch = make_idempotent_batch(4);
    auto orig_header = batch.header();
    auto transformed = records_to_transformed(batch);

    auto result = reconstruct_batch(orig_header, std::move(transformed));

    EXPECT_EQ(result.header().producer_id, 42);
    EXPECT_EQ(result.header().producer_epoch, 7);
    EXPECT_EQ(result.header().base_sequence, 100);
    EXPECT_EQ(result.header().record_count, 4);
    EXPECT_EQ(result.header().first_timestamp, orig_header.first_timestamp);
}

TEST(ProduceTransformTest, BatchReconstructionPreservesAttributes) {
    auto batch = make_idempotent_batch(2);
    auto orig_header = batch.header();
    auto transformed = records_to_transformed(batch);

    auto result = reconstruct_batch(orig_header, std::move(transformed));

    EXPECT_EQ(result.header().attrs, orig_header.attrs);
}

TEST(ProduceTransformTest, BatchReconstructionPreservesRecordContent) {
    auto batch = make_idempotent_batch(3);
    auto orig_header = batch.header();
    auto expected_records = batch.copy_records();
    auto transformed = records_to_transformed(batch);

    auto result = reconstruct_batch(orig_header, std::move(transformed));
    auto actual_records = result.copy_records();

    ASSERT_EQ(actual_records.size(), expected_records.size());
    for (size_t i = 0; i < expected_records.size(); ++i) {
        EXPECT_EQ(actual_records[i].key(), expected_records[i].key())
          << "record " << i << " key mismatch";
        EXPECT_EQ(actual_records[i].value(), expected_records[i].value())
          << "record " << i << " value mismatch";
        EXPECT_EQ(actual_records[i].headers(), expected_records[i].headers())
          << "record " << i << " headers mismatch";
        EXPECT_EQ(
          actual_records[i].offset_delta(), expected_records[i].offset_delta())
          << "record " << i << " offset_delta mismatch";
    }
}

TEST(ProduceTransformTest, MakeBatchProducesValidChecksums) {
    auto batch = make_idempotent_batch(3);
    auto transformed = records_to_transformed(batch);

    // Verify checksums on the batch returned by make_batch, before any
    // identity transplant is applied.
    auto result = model::transformed_data::make_batch(
      model::timestamp::now(), std::move(transformed));

    EXPECT_EQ(result.header().crc, model::crc_record_batch(result));
    EXPECT_EQ(
      result.header().header_crc,
      model::internal_header_only_crc(result.header()));
    EXPECT_EQ(result.header().size_bytes, result.size_bytes());
}

TEST(ProduceTransformTest, EmptyTransformOutputIsDetected) {
    ss::chunked_fifo<model::transformed_data> empty;
    EXPECT_TRUE(empty.empty());
}

TEST(ProduceTransformTest, NonIdempotentBatchIdentityPreserved) {
    auto batch = model::test::make_random_batch(
      {.allow_compression = false, .count = 2});
    auto orig_header = batch.header();
    auto transformed = records_to_transformed(batch);

    auto result = reconstruct_batch(orig_header, std::move(transformed));

    // Non-idempotent batches have producer_id == -1
    EXPECT_EQ(result.header().producer_id, orig_header.producer_id);
    EXPECT_EQ(result.header().producer_epoch, orig_header.producer_epoch);
    EXPECT_EQ(result.header().base_sequence, orig_header.base_sequence);
}

TEST(ProduceTransformTest, SingleRecordBatch) {
    auto batch = make_idempotent_batch(1);
    auto orig_header = batch.header();
    auto transformed = records_to_transformed(batch);

    auto result = reconstruct_batch(orig_header, std::move(transformed));

    EXPECT_EQ(result.header().record_count, 1);
    EXPECT_EQ(result.header().last_offset_delta, 0);
    EXPECT_EQ(result.header().producer_id, 42);
}

} // namespace
} // namespace kafka
