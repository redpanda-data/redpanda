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

namespace {
void check_parse_record_metadata(bool fully_parse) {
    constexpr int num_records = 10;
    auto b = model::test::make_random_batch(
      model::offset(0), num_records, false);

    std::vector<std::tuple<int64_t, int32_t, int32_t>> expected;
    auto it = model::record_batch_iterator::create(b);
    while (it.has_next()) {
        auto r = it.next();
        expected.emplace_back(
          r.timestamp_delta(), r.offset_delta(), r.key_size());
    }
    ASSERT_EQ(expected.size(), num_records);

    auto parser = iobuf_const_parser(b.data());
    for (int i = 0; i < num_records; ++i) {
        auto r = model::parse_record_metadata_from_buffer(parser, fully_parse);
        EXPECT_EQ(r.timestamp_delta(), std::get<0>(expected[i]));
        EXPECT_EQ(r.offset_delta(), std::get<1>(expected[i]));
        EXPECT_EQ(r.key_length(), std::get<2>(expected[i]));
    }
    EXPECT_EQ(parser.bytes_left(), 0);
}
} // namespace

TEST_F(RecordBatchTest, ParseRecordMetadataSkipFields) {
    check_parse_record_metadata(false);
}

TEST_F(RecordBatchTest, ParseRecordMetadataFullParse) {
    check_parse_record_metadata(true);
}

namespace {
// Serialize a record and verify that the size_bytes prefix matches the actual
// serialized body length.
void check_serialization_size(const model::record& r) {
    iobuf buf;
    model::append_record_to_buffer(buf, r);

    auto parser = iobuf_const_parser(buf);
    auto [written_size, _] = parser.read_varlong();
    auto body_size = static_cast<int64_t>(parser.bytes_left());

    EXPECT_EQ(r.size_bytes(), written_size);
    EXPECT_EQ(r.size_bytes(), body_size);
}
} // namespace

TEST_F(RecordBatchTest, RecordSizeBytesWithKeyAndValue) {
    auto r = model::record(
      model::record_attributes(0),
      0,
      0,
      iobuf::from("key"),
      iobuf::from("value"),
      {});
    check_serialization_size(r);
}

TEST_F(RecordBatchTest, RecordSizeBytesWithNullKey) {
    auto r = model::record(
      model::record_attributes(0),
      0,
      0,
      std::nullopt,
      iobuf::from("value"),
      {});
    check_serialization_size(r);
}

TEST_F(RecordBatchTest, RecordSizeBytesWithNullValue) {
    auto r = model::record(
      model::record_attributes(0), 0, 0, iobuf::from("key"), std::nullopt, {});
    check_serialization_size(r);
}

TEST_F(RecordBatchTest, RecordSizeBytesWithNullKeyAndValue) {
    auto r = model::record(
      model::record_attributes(0), 0, 0, std::nullopt, std::nullopt, {});
    check_serialization_size(r);
}

TEST_F(RecordBatchTest, RecordSizeBytesWithEmptyKey) {
    auto r = model::record(
      model::record_attributes(0), 0, 0, iobuf{}, iobuf::from("value"), {});
    check_serialization_size(r);
}

TEST_F(RecordBatchTest, RecordSizeBytesWithHeaders) {
    std::vector<model::record_header> headers;
    headers.emplace_back(3, iobuf::from("hdr"), 2, iobuf::from("hv"));
    auto r = model::record(
      model::record_attributes(0),
      0,
      0,
      iobuf::from("key"),
      iobuf::from("value"),
      std::move(headers));
    check_serialization_size(r);
}

TEST_F(RecordBatchTest, RecordSizeBytesWithNullHeaderValues) {
    std::vector<model::record_header> headers;
    headers.emplace_back(3, iobuf::from("hdr"), -1, iobuf{});
    auto r = model::record(
      model::record_attributes(0),
      0,
      0,
      iobuf::from("key"),
      iobuf::from("value"),
      std::move(headers));
    check_serialization_size(r);
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
