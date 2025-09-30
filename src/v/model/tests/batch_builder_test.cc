/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "model/batch_builder.h"
#include "model/compression.h"
#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/timestamp.h"

#include <gtest/gtest.h>

namespace model {

TEST(BatchBuilderTest, AddSimpleRecord) {
    batch_builder builder;
    iobuf key = iobuf{};
    key.append_str("test_key");
    iobuf value = iobuf{};
    value.append_str("test_value");

    builder.add_record({.key = std::move(key), .value = std::move(value)});
    auto batch = builder.build_sync();

    EXPECT_EQ(batch.header().record_count, 1);
    auto records = batch.copy_records();
    EXPECT_EQ(records.size(), 1);
    EXPECT_EQ(records[0].key_size(), 8);
    EXPECT_EQ(records[0].value_size(), 10);
    EXPECT_EQ(records[0].offset_delta(), 0);
    EXPECT_EQ(records[0].timestamp_delta(), 0);
}

TEST(BatchBuilderTest, AddMultipleSimpleRecords) {
    batch_builder builder;

    for (int i = 0; i < 5; ++i) {
        iobuf key = iobuf{};
        key.append_str(std::to_string(i));
        iobuf value = iobuf{};
        value.append_str("value" + std::to_string(i));
        builder.add_record({.key = std::move(key), .value = std::move(value)});
    }

    auto batch = builder.build_sync();
    EXPECT_EQ(batch.header().record_count, 5);
    EXPECT_EQ(batch.header().base_offset(), model::offset{0});
    EXPECT_EQ(batch.header().last_offset(), model::offset{4});

    auto records = batch.copy_records();
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ(records[i].offset_delta(), i);
        EXPECT_EQ(records[i].key_size(), 1);
        EXPECT_EQ(records[i].value_size(), 6);
    }
}

TEST(BatchBuilderTest, AddFullRecord) {
    batch_builder builder;

    iobuf key = iobuf{};
    key.append_str("full_key");
    iobuf value = iobuf{};
    value.append_str("full_value");

    record r(
      /*attributes=*/{},
      /*timestamp_delta=*/5000,
      /*offset_delta=*/10,
      /*key=*/std::move(key),
      /*value=*/std::move(value),
      /*hdrs=*/{});

    builder.add_record(std::move(r));
    auto batch = builder.build_sync();

    EXPECT_EQ(batch.header().record_count, 1);
    EXPECT_EQ(batch.header().last_offset(), model::offset{10});

    auto records = batch.copy_records();
    EXPECT_EQ(records[0].offset_delta(), 10);
    EXPECT_EQ(records[0].timestamp_delta(), 5000);
    EXPECT_EQ(records[0].key_size(), 8);
    EXPECT_EQ(records[0].value_size(), 10);
}

TEST(BatchBuilderTest, SetBatchType) {
    batch_builder builder;
    builder.add_record({.key = iobuf{}, .value = iobuf{}});
    builder.set_batch_type(record_batch_type::tx_prepare);
    auto batch = builder.build_sync();

    EXPECT_EQ(batch.header().type, record_batch_type::tx_prepare);
}

TEST(BatchBuilderTest, SetCompression) {
    batch_builder builder;
    builder.add_record({.key = iobuf{}, .value = iobuf{}});
    builder.set_compression(compression::gzip);
    auto batch = builder.build_sync();

    EXPECT_EQ(batch.header().attrs.compression(), compression::gzip);
}

TEST(BatchBuilderTest, SetControl) {
    batch_builder builder;
    builder.add_record({.key = iobuf{}, .value = iobuf{}});
    builder.set_control();
    auto batch = builder.build_sync();

    EXPECT_TRUE(batch.header().attrs.is_control());
}

TEST(BatchBuilderTest, SetTransactional) {
    batch_builder builder;
    builder.add_record({.key = iobuf{}, .value = iobuf{}});
    builder.set_transactional();
    auto batch = builder.build_sync();

    EXPECT_TRUE(batch.header().attrs.is_transactional());
}

TEST(BatchBuilderTest, SetProducerFields) {
    batch_builder builder;
    builder.add_record({.key = iobuf{}, .value = iobuf{}});
    builder.set_producer_id(12345);
    builder.set_producer_epoch(42);
    builder.set_base_sequence(987);
    auto batch = builder.build_sync();

    EXPECT_EQ(batch.header().producer_id, 12345);
    EXPECT_EQ(batch.header().producer_epoch, 42);
    EXPECT_EQ(batch.header().base_sequence, 987);
}

TEST(BatchBuilderTest, SetBaseOffset) {
    batch_builder builder;
    builder.add_record({.key = iobuf{}, .value = iobuf{}});
    builder.set_base_offset(kafka::offset{1000});
    auto batch = builder.build_sync();

    EXPECT_EQ(batch.header().base_offset, model::offset{1000});
}

TEST(BatchBuilderTest, SetLastOffset) {
    batch_builder builder;
    builder.add_record({.key = iobuf{}, .value = iobuf{}});
    builder.set_last_offset(kafka::offset{500});
    auto batch = builder.build_sync();

    // With base offset 0 and last offset override of 500,
    // last_offset_delta should be 500
    EXPECT_EQ(batch.header().last_offset_delta, 500);
}

TEST(BatchBuilderTest, SetTerm) {
    batch_builder builder;
    builder.add_record({.key = iobuf{}, .value = iobuf{}});
    builder.set_term(term_id{999});
    auto batch = builder.build_sync();

    EXPECT_EQ(batch.header().ctx.term, term_id{999});
}

TEST(BatchBuilderTest, SetBatchTimestampCreateTime) {
    batch_builder builder;
    auto now = timestamp::now();
    builder.set_batch_timestamp(timestamp_type::create_time, now);

    iobuf value = iobuf{};
    value.append_str("test");

    record r(
      /*attributes=*/{},
      /*timestamp_delta=*/1000,
      /*offset_delta=*/0,
      /*key=*/{},
      /*value=*/std::move(value),
      /*hdrs=*/{});
    builder.add_record(std::move(r));

    auto batch = builder.build_sync();

    EXPECT_EQ(batch.header().first_timestamp, now);
    EXPECT_EQ(batch.header().max_timestamp, timestamp{now() + 1000});
    EXPECT_EQ(
      batch.header().attrs.timestamp_type(), timestamp_type::create_time);
}

TEST(BatchBuilderTest, SetBatchTimestampAppendTime) {
    batch_builder builder;
    auto now = timestamp::now();
    builder.set_batch_timestamp(timestamp_type::append_time, now);

    iobuf value = iobuf{};
    value.append_str("test");

    record r(
      /*attributes=*/{},
      /*timestamp_delta=*/1000, // Should be ignored with append_time
      /*offset_delta=*/0,
      /*key=*/{},
      /*value=*/std::move(value),
      /*hdrs=*/{});
    builder.add_record(std::move(r));

    auto batch = builder.build_sync();

    EXPECT_EQ(batch.header().first_timestamp, now);
    EXPECT_EQ(batch.header().max_timestamp, now);
    EXPECT_EQ(
      batch.header().attrs.timestamp_type(), timestamp_type::append_time);
}

TEST(BatchBuilderTest, ComplexBatch) {
    batch_builder builder;

    // Configure all builder options
    builder.set_batch_type(record_batch_type::controller);
    builder.set_compression(compression::snappy);
    builder.set_control();
    builder.set_transactional();
    builder.set_producer_id(54321);
    builder.set_producer_epoch(7);
    builder.set_base_sequence(100);
    builder.set_base_offset(kafka::offset{2000});
    builder.set_term(term_id{42});

    auto now = timestamp::now();
    builder.set_batch_timestamp(timestamp_type::create_time, now);

    // Add multiple records with different timestamp deltas
    for (int i = 0; i < 3; ++i) {
        iobuf key = iobuf{};
        key.append_str("key" + std::to_string(i));
        iobuf value = iobuf{};
        value.append_str("value" + std::to_string(i));

        record r(
          /*attributes=*/{},
          /*timestamp_delta=*/i * 1000,
          /*offset_delta=*/i,
          /*key=*/std::move(key),
          /*value=*/std::move(value),
          /*hdrs=*/{});
        builder.add_record(std::move(r));
    }

    auto batch = builder.build_sync();

    EXPECT_EQ(batch.header().type, record_batch_type::controller);
    EXPECT_EQ(batch.header().attrs.compression(), compression::snappy);
    EXPECT_TRUE(batch.header().attrs.is_control());
    EXPECT_TRUE(batch.header().attrs.is_transactional());
    EXPECT_EQ(batch.header().producer_id, 54321);
    EXPECT_EQ(batch.header().producer_epoch, 7);
    EXPECT_EQ(batch.header().base_sequence, 100);
    EXPECT_EQ(batch.header().base_offset, model::offset{2000});
    EXPECT_EQ(batch.header().ctx.term, term_id{42});
    EXPECT_EQ(batch.header().record_count, 3);
    EXPECT_EQ(batch.header().last_offset_delta, 2);
    EXPECT_EQ(batch.header().first_timestamp, now);
    EXPECT_EQ(batch.header().max_timestamp, timestamp{now() + 2000});
}

TEST(BatchBuilderTest, AsyncBuild) {
    batch_builder builder;
    iobuf key = iobuf{};
    key.append_str("async_key");
    iobuf value = iobuf{};
    value.append_str("async_value");
    builder.add_record({.key = std::move(key), .value = std::move(value)});

    auto batch_future = builder.build();
    auto batch = batch_future.get();

    EXPECT_EQ(batch.header().record_count, 1);
    auto records = batch.copy_records();
    EXPECT_EQ(records[0].key_size(), 9);
    EXPECT_EQ(records[0].value_size(), 11);
}

} // namespace model
