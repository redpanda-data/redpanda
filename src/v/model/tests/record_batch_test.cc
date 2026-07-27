// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/bytes.h"
#include "bytes/iobuf_parser.h"
#include "container/chunked_vector.h"
#include "model/batch_compression.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "model/record_utils.h"
#include "model/tests/random_batch.h"
#include "model/timestamp.h"

#include <gtest/gtest.h>

#include <optional>
#include <string_view>
#include <vector>

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

    auto it = model::record_batch_copy_iterator::create(b);
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
    auto it = model::record_batch_copy_iterator::create(b);
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

    std::vector<std::pair<int64_t, int32_t>> expected;
    auto it = model::record_batch_copy_iterator::create(b);
    while (it.has_next()) {
        auto r = it.next();
        expected.emplace_back(r.timestamp_delta(), r.offset_delta());
    }
    ASSERT_EQ(expected.size(), num_records);

    auto parser = iobuf_const_parser(b.data());
    for (int i = 0; i < num_records; ++i) {
        auto r = model::parse_record_metadata_from_buffer(parser, fully_parse);
        EXPECT_EQ(r.timestamp_delta(), expected[i].first);
        EXPECT_EQ(r.offset_delta(), expected[i].second);
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
    chunked_vector<model::record_header> headers;
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
    chunked_vector<model::record_header> headers;
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

TEST_F(RecordBatchTest, SetTimestampDeltaKeepsSizeBytesAccurate) {
    auto r = model::record(
      model::record_attributes(0),
      0,
      0,
      iobuf::from("key"),
      iobuf::from("value"),
      {});
    check_serialization_size(r);

    // Cross vint width boundaries in both directions (zigzag: -64..63 fits in
    // one byte), including negative and multi-day deltas.
    for (int64_t delta :
         {int64_t{63},
          int64_t{64},
          int64_t{1209600000}, // 14 days in ms
          int64_t{-1},
          int64_t{-64},
          int64_t{-65},
          int64_t{-1209600000},
          int64_t{0}}) {
        r.set_timestamp_delta(delta);
        EXPECT_EQ(r.timestamp_delta(), delta);
        check_serialization_size(r);
    }
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

namespace {

// Serialize records into a raft_data batch (deterministic; for key-parse
// tests).
model::record_batch batch_from_records(std::vector<model::record> recs) {
    iobuf body;
    const auto count = static_cast<int32_t>(recs.size());
    for (const auto& r : recs) {
        model::append_record_to_buffer(body, r);
    }
    model::record_batch_header hdr{};
    hdr.type = model::record_batch_type::raft_data;
    hdr.base_offset = model::offset(0);
    hdr.record_count = count;
    hdr.last_offset_delta = count - 1;
    hdr.first_timestamp = model::timestamp(1234);
    hdr.max_timestamp = model::timestamp(1234);
    hdr.reset_size_checksum_metadata(body);
    return model::record_batch(
      hdr, std::move(body), model::record_batch::tag_ctor_ng{});
}

model::record make_record(
  int32_t offset_delta,
  std::optional<iobuf> key,
  std::optional<iobuf> value,
  chunked_vector<model::record_header> headers = {}) {
    return model::record(
      model::record_attributes{},
      /*timestamp_delta=*/0,
      offset_delta,
      std::move(key),
      std::move(value),
      std::move(headers));
}

} // namespace

// parse_record_key_from_buffer must return the key/offset/tombstone flag and
// advance the parser past the (skipped) value and headers, so the *next* record
// parses from the correct position. The final bytes_left() == 0 is the check
// that value+headers were skipped by exactly the right amount.
TEST_F(RecordBatchTest, ParseRecordKeySkipsValueAndHeaders) {
    std::vector<model::record> recs;
    {
        chunked_vector<model::record_header> h;
        h.emplace_back(2, iobuf::from("hk"), 3, iobuf::from("hdr"));
        recs.push_back(make_record(
          0,
          iobuf::from("key-0"),
          iobuf::from("a-biggish-value"),
          std::move(h)));
    }
    // Tombstone: null value, non-empty key.
    recs.push_back(make_record(1, iobuf::from("key-1"), std::nullopt));
    auto batch = batch_from_records(std::move(recs));

    iobuf_const_parser p(batch.data());

    auto k0 = model::parse_record_key_from_buffer(p);
    EXPECT_EQ(k0.offset_delta, 0);
    EXPECT_FALSE(k0.is_tombstone);
    EXPECT_EQ(k0.key, iobuf_to_bytes(iobuf::from("key-0")));

    auto k1 = model::parse_record_key_from_buffer(p);
    EXPECT_EQ(k1.offset_delta, 1);
    EXPECT_TRUE(k1.is_tombstone);
    EXPECT_EQ(k1.key, iobuf_to_bytes(iobuf::from("key-1")));

    EXPECT_EQ(p.bytes_left(), 0u);
}

// A null key yields an empty key view; is_tombstone reflects the value, not the
// key (here: null key but a present value -> not a tombstone).
TEST_F(RecordBatchTest, ParseRecordKeyNullKeyIsNotTombstone) {
    std::vector<model::record> recs;
    recs.push_back(make_record(0, std::nullopt, iobuf::from("v")));
    auto batch = batch_from_records(std::move(recs));

    iobuf_const_parser p(batch.data());
    auto k = model::parse_record_key_from_buffer(p);
    EXPECT_TRUE(k.key.empty());
    EXPECT_FALSE(k.is_tombstone);
    EXPECT_EQ(p.bytes_left(), 0u);
}

// for_each_record_key must yield exactly the same key/offset/tombstone info as
// full materialization via for_each_record, across normal records, a tombstone,
// a null-key record, and a record with headers.
TEST_F(RecordBatchTest, ForEachRecordKeyMatchesFullParse) {
    std::vector<model::record> recs;
    recs.push_back(make_record(0, iobuf::from("k0"), iobuf::from("v0")));
    recs.push_back(
      make_record(1, iobuf::from("k1"), std::nullopt)); // tombstone
    recs.push_back(make_record(2, std::nullopt, iobuf::from("v2"))); // null key
    {
        chunked_vector<model::record_header> h;
        h.emplace_back(1, iobuf::from("a"), 1, iobuf::from("b"));
        recs.push_back(
          make_record(3, iobuf::from("k3"), iobuf::from("v3"), std::move(h)));
    }
    auto batch = batch_from_records(std::move(recs));

    struct row {
        int32_t offset_delta;
        bool is_tombstone;
        bytes key;
        bool operator==(const row&) const = default;
    };

    // Ground truth: full record materialization.
    std::vector<row> full;
    batch.for_each_record([&full](model::record r) {
        full.push_back(
          {r.offset_delta(), r.is_tombstone(), iobuf_to_bytes(r.key())});
    });

    // Key-only iteration.
    std::vector<row> key_only;
    batch.for_each_record_key([&key_only](model::record_key_metadata kv) {
        key_only.push_back(
          {kv.offset_delta, kv.is_tombstone, std::move(kv.key)});
    });

    ASSERT_EQ(full.size(), 4u);
    EXPECT_EQ(key_only, full);
}

// for_each_record_key_async honours ss::stop_iteration returned by the
// callback.
TEST_F(RecordBatchTest, ForEachRecordKeyAsyncStops) {
    std::vector<model::record> recs;
    for (int32_t i = 0; i < 5; ++i) {
        recs.push_back(make_record(i, iobuf::from("k"), iobuf::from("v")));
    }
    auto batch = batch_from_records(std::move(recs));

    std::vector<int32_t> seen;
    batch
      .for_each_record_key_async(
        [&seen](
          this auto,
          model::record_key_metadata kv) -> ss::future<ss::stop_iteration> {
            seen.push_back(kv.offset_delta);
            co_return kv.offset_delta == 2 ? ss::stop_iteration::yes
                                           : ss::stop_iteration::no;
        })
      .get();

    EXPECT_EQ(seen, (std::vector<int32_t>{0, 1, 2}));
}
