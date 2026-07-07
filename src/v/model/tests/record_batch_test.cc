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
#include "model/record_fields.h"
#include "model/record_utils.h"
#include "model/tests/random_batch.h"
#include "model/timestamp.h"
#include "utils/vint.h"

#include <gtest/gtest.h>

#include <limits>
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

namespace {

// Membership probes: true iff the field is an accessible member of `PR`. Used
// to assert, at compile time, that a parsed_record exposes exactly the
// requested fields and nothing else.
template<typename PR>
concept has_offset_delta = requires(PR pr) { pr.offset_delta; };
template<typename PR>
concept has_timestamp_delta = requires(PR pr) { pr.timestamp_delta; };
template<typename PR>
concept has_attributes = requires(PR pr) { pr.attributes; };
template<typename PR>
concept has_is_tombstone = requires(PR pr) { pr.is_tombstone; };
template<typename PR>
concept has_key = requires(PR pr) { pr.key; };
template<typename PR>
concept has_value = requires(PR pr) { pr.value; };
template<typename PR>
concept has_headers = requires(PR pr) { pr.headers; };

} // namespace

// The returned type exposes exactly the requested fields; every unrequested
// field is absent (not merely uninitialized), so touching it would fail to
// compile. These static_asserts *are* the compile-time-safety guarantee.
TEST_F(RecordBatchTest, ParsedRecordExposesOnlyRequestedFields) {
    using kv_t = model::
      parsed_record<model::record_field::key, model::record_field::value>;
    static_assert(has_key<kv_t>);
    static_assert(has_value<kv_t>);
    static_assert(!has_offset_delta<kv_t>);
    static_assert(!has_timestamp_delta<kv_t>);
    static_assert(!has_attributes<kv_t>);
    static_assert(!has_is_tombstone<kv_t>);
    static_assert(!has_headers<kv_t>);

    using key_only_t = model::parsed_record<
      model::record_field::offset_delta,
      model::record_field::is_tombstone,
      model::record_field::key>;
    static_assert(has_offset_delta<key_only_t>);
    static_assert(has_is_tombstone<key_only_t>);
    static_assert(has_key<key_only_t>);
    static_assert(!has_value<key_only_t>);
    static_assert(!has_timestamp_delta<key_only_t>);
    static_assert(!has_attributes<key_only_t>);
    static_assert(!has_headers<key_only_t>);

    // The `has<F>` compile-time membership helper mirrors the probes above.
    static_assert(kv_t::has<model::record_field::key>);
    static_assert(!kv_t::has<model::record_field::headers>);
}

// for_each_record<Fields...> yields the same values as a full parse for every
// selectable field, across normal records, a tombstone, a null-key record, and
// a record with headers.
TEST_F(RecordBatchTest, ForEachRecordFieldsMatchesFullParse) {
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
        int64_t timestamp_delta;
        bool is_tombstone;
        bytes key;
        bytes value;
        size_t header_count;
        bool operator==(const row&) const = default;
    };

    std::vector<row> full;
    batch.for_each_record([&full](model::record r) {
        full.push_back(
          {r.offset_delta(),
           r.timestamp_delta(),
           r.is_tombstone(),
           iobuf_to_bytes(r.key()),
           iobuf_to_bytes(r.value()),
           r.headers().size()});
    });

    std::vector<row> selected;
    batch.for_each_record<
      model::record_field::offset_delta,
      model::record_field::timestamp_delta,
      model::record_field::is_tombstone,
      model::record_field::key,
      model::record_field::value,
      model::record_field::headers>([&selected](auto pr) {
        selected.push_back(
          {pr.offset_delta,
           pr.timestamp_delta,
           pr.is_tombstone,
           iobuf_to_bytes(pr.key),
           iobuf_to_bytes(pr.value),
           pr.headers.size()});
    });

    ASSERT_EQ(full.size(), 4u);
    EXPECT_EQ(selected, full);
}

// Requesting a strict subset materializes only those fields; the loop still
// walks every record correctly because unrequested fields are skipped in the
// buffer by exactly the right number of bytes.
TEST_F(RecordBatchTest, ForEachRecordFieldsValueOnly) {
    std::vector<model::record> recs;
    recs.push_back(make_record(0, iobuf::from("k0"), iobuf::from("v0")));
    recs.push_back(make_record(1, iobuf::from("k1"), iobuf::from("v1")));
    recs.push_back(make_record(2, iobuf::from("k2"), std::nullopt));
    auto batch = batch_from_records(std::move(recs));

    std::vector<bytes> values;
    batch.for_each_record<model::record_field::value>(
      [&values](auto pr) { values.push_back(iobuf_to_bytes(pr.value)); });

    EXPECT_EQ(
      values,
      (std::vector<bytes>{
        iobuf_to_bytes(iobuf::from("v0")),
        iobuf_to_bytes(iobuf::from("v1")),
        bytes{}}));
}

// parsed_record's bases are laid out in a canonical order regardless of the
// order the fields were requested in, so reversed packs have identical sizes
// (a naive request-ordered layout would pad them differently).
TEST_F(RecordBatchTest, ParsedRecordLayoutIsOrderIndependent) {
    using fwd_t = model::parsed_record<
      model::record_field::is_tombstone,
      model::record_field::attributes,
      model::record_field::offset_delta,
      model::record_field::timestamp_delta>;
    using rev_t = model::parsed_record<
      model::record_field::timestamp_delta,
      model::record_field::offset_delta,
      model::record_field::attributes,
      model::record_field::is_tombstone>;
    static_assert(sizeof(fwd_t) == sizeof(rev_t));

    using kv_fwd_t = model::parsed_record<
      model::record_field::is_tombstone,
      model::record_field::key,
      model::record_field::value,
      model::record_field::headers>;
    using kv_rev_t = model::parsed_record<
      model::record_field::headers,
      model::record_field::value,
      model::record_field::key,
      model::record_field::is_tombstone>;
    static_assert(sizeof(kv_fwd_t) == sizeof(kv_rev_t));
}

// Every field parsed via parse_record_fields matches the fully materialized
// record, across randomized batches (random keys/values/headers/timestamps),
// and each record consumes exactly its bytes so the loop stays aligned.
TEST_F(RecordBatchTest, ParseRecordFieldsRandomizedMatchesFullParse) {
    for (int iter = 0; iter < 20; ++iter) {
        auto b = model::test::make_random_batch(
          model::offset(0),
          /*num_records=*/1 + iter,
          /*allow_compression=*/false);

        std::vector<model::record> full;
        b.for_each_record(
          [&full](model::record r) { full.push_back(std::move(r)); });

        auto parser = iobuf_const_parser(b.data());
        for (const auto& r : full) {
            auto pr = model::parse_record_fields<
              model::record_field::size_bytes,
              model::record_field::attributes,
              model::record_field::timestamp_delta,
              model::record_field::offset_delta,
              model::record_field::key,
              model::record_field::is_tombstone,
              model::record_field::value,
              model::record_field::headers>(parser);
            EXPECT_EQ(pr.size_bytes, r.size_bytes());
            EXPECT_EQ(pr.attributes, r.attributes());
            EXPECT_EQ(pr.timestamp_delta, r.timestamp_delta());
            EXPECT_EQ(pr.offset_delta, r.offset_delta());
            EXPECT_EQ(pr.key, r.key());
            EXPECT_EQ(pr.is_tombstone, r.is_tombstone());
            EXPECT_EQ(pr.value, r.value());
            ASSERT_EQ(pr.headers.size(), r.headers().size());
            for (size_t i = 0; i < pr.headers.size(); ++i) {
                EXPECT_EQ(pr.headers[i], r.headers()[i]);
            }
        }
        EXPECT_EQ(parser.bytes_left(), 0u);
    }
}

// Requesting each field on its own must both return the right value and skip
// the rest of the record by exactly the right number of bytes (checked by the
// next record parsing correctly and by bytes_left() == 0 at the end).
TEST_F(RecordBatchTest, ParseRecordFieldsEachFieldIndividually) {
    std::vector<model::record> recs;
    {
        chunked_vector<model::record_header> h;
        h.emplace_back(2, iobuf::from("hk"), 3, iobuf::from("hdr"));
        recs.push_back(make_record(
          0, iobuf::from("key-0"), iobuf::from("value-0"), std::move(h)));
    }
    recs.push_back(make_record(1, iobuf::from("key-1"), std::nullopt));
    recs.push_back(make_record(2, std::nullopt, iobuf::from("value-2")));
    auto batch = batch_from_records(std::move(recs));

    std::vector<model::record> full;
    batch.for_each_record(
      [&full](model::record r) { full.push_back(std::move(r)); });
    ASSERT_EQ(full.size(), 3u);

    auto parse_each = [&batch, &full]<model::record_field F>(auto&& check_one) {
        auto parser = iobuf_const_parser(batch.data());
        for (const auto& r : full) {
            check_one(model::parse_record_fields<F>(parser), r);
        }
        EXPECT_EQ(parser.bytes_left(), 0u);
    };

    parse_each.operator()<model::record_field::size_bytes>(
      [](auto pr, const auto& r) { EXPECT_EQ(pr.size_bytes, r.size_bytes()); });
    parse_each.operator()<model::record_field::attributes>(
      [](auto pr, const auto& r) { EXPECT_EQ(pr.attributes, r.attributes()); });
    parse_each.operator()<model::record_field::timestamp_delta>(
      [](auto pr, const auto& r) {
          EXPECT_EQ(pr.timestamp_delta, r.timestamp_delta());
      });
    parse_each.operator()<model::record_field::offset_delta>(
      [](auto pr, const auto& r) {
          EXPECT_EQ(pr.offset_delta, r.offset_delta());
      });
    parse_each.operator()<model::record_field::key>(
      [](auto pr, const auto& r) { EXPECT_EQ(pr.key, r.key()); });
    parse_each.operator()<model::record_field::is_tombstone>(
      [](auto pr, const auto& r) {
          EXPECT_EQ(pr.is_tombstone, r.is_tombstone());
      });
    parse_each.operator()<model::record_field::value>(
      [](auto pr, const auto& r) { EXPECT_EQ(pr.value, r.value()); });
    parse_each.operator()<model::record_field::headers>(
      [](auto pr, const auto& r) {
          ASSERT_EQ(pr.headers.size(), r.headers().size());
          for (size_t i = 0; i < pr.headers.size(); ++i) {
              EXPECT_EQ(pr.headers[i], r.headers()[i]);
          }
      });
}

namespace {

void append_vint(iobuf& b, int64_t v) {
    auto vb = vint::to_bytes(v);
    b.append(vb.data(), vb.size());
}

// Assemble one raw record: attributes byte, zero timestamp/offset deltas, the
// given key/value (nullopt encodes as length -1) and no headers, prefixed with
// its size varint. `declared_size_delta` shifts the size prefix away from the
// true body size to simulate corruption.
iobuf make_raw_record(
  std::optional<std::string_view> key,
  std::optional<std::string_view> value,
  int64_t declared_size_delta = 0,
  std::optional<int64_t> key_length_override = std::nullopt) {
    iobuf body;
    const uint8_t attr = 0;
    body.append(&attr, 1);
    append_vint(body, 0); // timestamp delta
    append_vint(body, 0); // offset delta
    append_vint(
      body,
      key_length_override.value_or(
        key ? static_cast<int64_t>(key->size()) : -1));
    if (key) {
        body.append(key->data(), key->size());
    }
    append_vint(body, value ? static_cast<int64_t>(value->size()) : -1);
    if (value) {
        body.append(value->data(), value->size());
    }
    append_vint(body, 0); // header count
    iobuf rec;
    append_vint(
      rec, static_cast<int64_t>(body.size_bytes()) + declared_size_delta);
    rec.append(std::move(body));
    return rec;
}

} // namespace

// A record size prefix that exceeds the bytes actually present must throw.
TEST_F(RecordBatchTest, ParseRecordFieldsTruncatedBuffer) {
    auto rec = make_raw_record("key", "value", /*declared_size_delta=*/100);
    auto parser = iobuf_const_parser(rec);
    EXPECT_THROW(
      model::parse_record_fields<model::record_field::value>(parser),
      std::out_of_range);
}

// A key length larger than the record itself must throw rather than read into
// the next record's bytes.
TEST_F(RecordBatchTest, ParseRecordFieldsOversizedKeyLength) {
    auto rec = make_raw_record(
      "key", "value", /*declared_size_delta=*/0, /*key_length_override=*/1000);
    // Enough trailing bytes that the parser could physically read them if the
    // length check were missing.
    rec.append(iobuf::from(std::string(1000, 'x')));
    auto parser = iobuf_const_parser(rec);
    EXPECT_THROW(
      model::parse_record_fields<model::record_field::key>(parser),
      std::out_of_range);
}

// A record whose fields overrun its declared size must throw when the parser
// skips the tail, instead of silently misaligning on the next record.
TEST_F(RecordBatchTest, ParseRecordFieldsFieldsOverrunDeclaredSize) {
    // Shrink the declared size below what the decoded fields occupy, while
    // leaving enough bytes in the buffer to pass the initial size check. The
    // key alone (17 bytes plus the preceding varints) overruns the shrunken
    // size, so skipping the tail after it must throw.
    auto rec = make_raw_record(
      "a-long-enough-key", "value", /*declared_size_delta=*/-10);
    auto parser = iobuf_const_parser(rec);
    EXPECT_THROW(
      model::parse_record_fields<model::record_field::key>(parser),
      std::out_of_range);
}

// An absurd header count (corrupted varint) must throw before attempting to
// reserve/parse that many headers.
TEST_F(RecordBatchTest, ParseRecordFieldsCorruptHeaderCount) {
    iobuf body;
    const uint8_t attr = 0;
    body.append(&attr, 1);
    append_vint(body, 0);                                   // timestamp delta
    append_vint(body, 0);                                   // offset delta
    append_vint(body, -1);                                  // null key
    append_vint(body, -1);                                  // null value
    append_vint(body, std::numeric_limits<int32_t>::max()); // header count
    iobuf rec;
    append_vint(rec, static_cast<int64_t>(body.size_bytes()));
    rec.append(std::move(body));
    auto parser = iobuf_const_parser(rec);
    EXPECT_THROW(
      model::parse_record_fields<model::record_field::headers>(parser),
      std::out_of_range);
}

// Slack bytes hidden inside the declared record size are undetectable when
// skipping (they are simply skipped over), but a full parse walks every field
// and must flag the mismatch.
TEST_F(RecordBatchTest, ParseRecordFieldsFullParseDetectsSlack) {
    auto rec = make_raw_record("key", "value", /*declared_size_delta=*/2);
    rec.append(iobuf::from("xx")); // the slack the size prefix accounts for
    {
        auto parser = iobuf_const_parser(rec);
        auto pr = model::parse_record_fields<model::record_field::offset_delta>(
          parser);
        EXPECT_EQ(pr.offset_delta, 0);
        EXPECT_EQ(parser.bytes_left(), 0u);
    }
    {
        auto parser = iobuf_const_parser(rec);
        EXPECT_THROW(
          (model::parse_record_fields<true, model::record_field::offset_delta>(
            parser)),
          std::out_of_range);
    }
}

// for_each_record_async<Fields...> honours ss::stop_iteration.
TEST_F(RecordBatchTest, ForEachRecordFieldsAsyncStops) {
    std::vector<model::record> recs;
    for (int32_t i = 0; i < 5; ++i) {
        recs.push_back(make_record(i, iobuf::from("k"), iobuf::from("v")));
    }
    auto batch = batch_from_records(std::move(recs));

    std::vector<int32_t> seen;
    batch
      .for_each_record_async<model::record_field::offset_delta>(
        [&seen](this auto, auto pr) -> ss::future<ss::stop_iteration> {
            seen.push_back(pr.offset_delta);
            co_return pr.offset_delta == 2 ? ss::stop_iteration::yes
                                           : ss::stop_iteration::no;
        })
      .get();

    EXPECT_EQ(seen, (std::vector<int32_t>{0, 1, 2}));
}
