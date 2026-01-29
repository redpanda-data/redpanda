// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/batch_compression.h"
#include "model/record.h"
#include "model/record_utils.h"
#include "model/tests/random_batch.h"
#include "model/timestamp.h"

#include <gtest/gtest.h>

class RecordBatchTest : public ::testing::Test {};

class RecordBatchAttributesTest
  : public ::testing::TestWithParam<
      std::tuple<model::compression, model::timestamp_type>> {};

TEST_P(RecordBatchAttributesTest, TestAttributes) {
    auto [c, ts_tp] = GetParam();
    model::record_batch_attributes attrs;
    attrs |= c;
    attrs |= ts_tp;

    EXPECT_EQ(attrs.compression(), c);
    EXPECT_EQ(attrs.timestamp_type(), ts_tp);
}

INSTANTIATE_TEST_SUITE_P(
  CompressionAndTimestampTypes,
  RecordBatchAttributesTest,
  ::testing::Combine(
    ::testing::Values(
      model::compression::none,
      model::compression::gzip,
      model::compression::snappy,
      model::compression::zstd,
      model::compression::lz4),
    ::testing::Values(
      model::timestamp_type::append_time, model::timestamp_type::create_time)));

TEST_F(RecordBatchTest, SetMaxTimestamp) {
    auto batch = model::test::make_random_batch(model::offset(0), 10, true);

    // nothing changes if set to same values
    auto crc = batch.header().crc;
    auto hdr_crc = batch.header().header_crc;
    batch.set_max_timestamp(
      batch.header().attrs.timestamp_type(), batch.header().max_timestamp);
    EXPECT_EQ(crc, batch.header().crc);
    EXPECT_EQ(hdr_crc, batch.header().header_crc);

    // ts change updates crcs
    batch.set_max_timestamp(
      model::timestamp_type::append_time,
      model::timestamp(batch.header().max_timestamp() + 1));
    EXPECT_NE(crc, batch.header().crc);
    EXPECT_NE(hdr_crc, batch.header().header_crc);

    // same ts produces orig crcs
    batch.set_max_timestamp(
      model::timestamp_type::create_time,
      model::timestamp(batch.header().max_timestamp() - 1));
    EXPECT_EQ(crc, batch.header().crc);
    EXPECT_EQ(hdr_crc, batch.header().header_crc);
}

TEST_F(RecordBatchTest, Iterator) {
    auto b = model::test::make_random_batch(model::offset(0), 10, false);

    auto it = model::record_batch_iterator::create(b);
    for (int i = 0; i < b.record_count(); ++i) {
        EXPECT_TRUE(it.has_next());
        model::record r = it.next();
        EXPECT_EQ(r.offset_delta(), i);
    }
    EXPECT_FALSE(it.has_next());
}

TEST_F(RecordBatchTest, ExtraBytesIterator) {
    auto b = model::test::make_random_batch(model::offset(0), 1, false);
    auto buf = b.data().copy();
    // If there are extra bytes at the end of the batch we should throw.
    constexpr std::string_view extra_data = "foobar";
    buf.append(extra_data.data(), extra_data.size());
    auto header = b.header();
    header.size_bytes = static_cast<int32_t>(
      model::packed_record_batch_header_size + buf.size_bytes());
    b = model::record_batch(
      header, std::move(buf), model::record_batch::tag_ctor_ng{});
    auto it = model::record_batch_iterator::create(b);
    EXPECT_TRUE(it.has_next());
    EXPECT_THROW(it.next(), std::out_of_range);
}

TEST_F(RecordBatchTest, TestCorruptedRecordBytes) {
    auto b = model::test::make_random_batch(model::offset(0), 10, false);
    // use the trick to get mutable access to the records
    auto fields = b.serde_fields();
    auto& records = std::get<1>(fields);
    for (auto& f : records) {
        std::fill_n(f.get_write(), f.size(), 0xFF);
    }
    auto f = model::for_each_record(
      b, [](model::record& r) { EXPECT_GE(r.offset_delta(), 0); });
    EXPECT_THROW(f.get(), std::out_of_range);
}

class RecordBatchCompressionTest
  : public ::testing::TestWithParam<model::compression> {};

TEST_P(RecordBatchCompressionTest, Compression) {
    auto b = model::test::make_random_batch({
      .offset = model::offset(0),
      .allow_compression = false,
      .count = 10,
    });
    if (GetParam() == model::compression::none) {
        EXPECT_ANY_THROW(model::decompress_batch(b).get());
        EXPECT_ANY_THROW(
          model::compress_batch(model::compression::none, std::move(b)).get());
    } else {
        auto c = model::compress_batch(GetParam(), std::move(b)).get();
        EXPECT_TRUE(c.compressed());
        EXPECT_EQ(c.header().attrs.compression(), GetParam());
        auto u_copy = model::decompress_batch(c).get();
        auto u = model::decompress_batch(c).get();
        EXPECT_FALSE(u.compressed());
        EXPECT_EQ(u.header().attrs.compression(), model::compression::none);
        EXPECT_EQ(u_copy, u);
    }
}

INSTANTIATE_TEST_SUITE_P(
  CompressionTypes,
  RecordBatchCompressionTest,
  ::testing::Values(
    model::compression::none,
    model::compression::gzip,
    model::compression::snappy,
    model::compression::zstd,
    model::compression::lz4));
