/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iobuf.h"
#include "model/record.h"
#include "model/record_batch_types.h"
#include "random/generators.h"
#include "storage/record_batch_utils.h"

#include <gtest/gtest.h>

#include <array>
#include <limits>
#include <type_traits>

namespace {

model::record_batch_header random_header() {
    return model::record_batch_header{
      .header_crc = random_generators::get_int<uint32_t>(),
      .size_bytes = random_generators::get_int<int32_t>(0, 100000),
      .base_offset = model::offset(random_generators::get_int<int64_t>(0, 1e9)),
      .type = model::record_batch_type::raft_data,
      .crc = random_generators::get_int<uint32_t>(),
      .attrs = model::record_batch_attributes(
        random_generators::get_int<int16_t>()),
      .last_offset_delta = random_generators::get_int<int32_t>(0, 1000),
      .first_timestamp = model::timestamp(
        random_generators::get_int<int64_t>(0, 1e12)),
      .max_timestamp = model::timestamp(
        random_generators::get_int<int64_t>(0, 1e12)),
      .producer_id = random_generators::get_int<int64_t>(),
      .producer_epoch = random_generators::get_int<int16_t>(),
      .base_sequence = random_generators::get_int<int32_t>(),
      .record_count = random_generators::get_int<int32_t>(0, 1000)};
}

// record_batch_header::operator== compares only a subset of fields, so assert
// every serialized field individually.
void expect_all_fields_eq(
  const model::record_batch_header& got,
  const model::record_batch_header& want) {
    EXPECT_EQ(got.header_crc, want.header_crc);
    EXPECT_EQ(got.size_bytes, want.size_bytes);
    EXPECT_EQ(got.base_offset, want.base_offset);
    EXPECT_EQ(got.type, want.type);
    EXPECT_EQ(got.crc, want.crc);
    EXPECT_EQ(got.attrs, want.attrs);
    EXPECT_EQ(got.last_offset_delta, want.last_offset_delta);
    EXPECT_EQ(got.first_timestamp, want.first_timestamp);
    EXPECT_EQ(got.max_timestamp, want.max_timestamp);
    EXPECT_EQ(got.producer_id, want.producer_id);
    EXPECT_EQ(got.producer_epoch, want.producer_epoch);
    EXPECT_EQ(got.base_sequence, want.base_sequence);
    EXPECT_EQ(got.record_count, want.record_count);
}

// Serializes a header to its disk form and parses it back through the
// contiguous-buffer parser, mirroring how the read path parses directly from
// the stream's contiguous read buffer.
model::record_batch_header round_trip(const model::record_batch_header& hdr) {
    auto buf = storage::batch_header_to_disk_iobuf(hdr);
    EXPECT_EQ(buf.size_bytes(), model::packed_record_batch_header_size);
    std::array<char, model::packed_record_batch_header_size> stack_buf{};
    iobuf::iterator_consumer(buf.cbegin(), buf.cend())
      .consume_to(stack_buf.size(), stack_buf.data());
    return storage::batch_header_from_disk_buf(stack_buf);
}

} // namespace

// batch_header_from_disk_buf must be the exact inverse of reflection::serialize
// (via batch_header_to_disk_iobuf) for every field.
TEST(record_batch_utils, disk_buf_round_trips_all_fields) {
    for (int i = 0; i < 100; ++i) {
        auto hdr = random_header();
        expect_all_fields_eq(round_trip(hdr), hdr);
    }
}

// Pins the on-disk field offsets, sizes, order, and little-endian decoding
// independently of the serializer: distinct bytes hand-placed in little-endian
// order at the documented offsets must decode to the exact expected field
// values. The bytes are written LE explicitly (not via memcpy of a native
// integer) so the test validates endianness regardless of host byte order.
TEST(record_batch_utils, disk_buf_golden_layout) {
    std::array<char, model::packed_record_batch_header_size> buf{};
    auto put = [&](size_t off, auto v) {
        auto u = static_cast<std::make_unsigned_t<decltype(v)>>(v);
        for (size_t i = 0; i < sizeof(u); ++i) {
            buf[off + i] = static_cast<char>((u >> (8 * i)) & 0xFF);
        }
    };
    put(0, uint32_t{0xA1A2A3A4});
    put(4, int32_t{0x1B2B3C4D});
    put(8, int64_t{0x0102030405060708});
    put(16, static_cast<int8_t>(model::record_batch_type::ghost_batch));
    put(17, uint32_t{0xB1B2B3B4});
    put(21, int16_t{0x0102});
    put(23, int32_t{0x11223344});
    put(27, int64_t{0x2122232425262728});
    put(35, int64_t{0x3132333435363738});
    put(43, int64_t{0x4142434445464748});
    put(51, int16_t{0x5152});
    put(53, int32_t{0x61626364});
    put(57, int32_t{0x71727374});

    auto h = storage::batch_header_from_disk_buf(buf);
    EXPECT_EQ(h.header_crc, 0xA1A2A3A4U);
    EXPECT_EQ(h.size_bytes, 0x1B2B3C4D);
    EXPECT_EQ(h.base_offset(), 0x0102030405060708);
    EXPECT_EQ(h.type, model::record_batch_type::ghost_batch);
    EXPECT_EQ(h.crc, 0xB1B2B3B4U);
    EXPECT_EQ(h.attrs.value(), int16_t(0x0102));
    EXPECT_EQ(h.last_offset_delta, 0x11223344);
    EXPECT_EQ(h.first_timestamp.value(), 0x2122232425262728);
    EXPECT_EQ(h.max_timestamp.value(), 0x3132333435363738);
    EXPECT_EQ(h.producer_id, 0x4142434445464748);
    EXPECT_EQ(h.producer_epoch, int16_t(0x5152));
    EXPECT_EQ(h.base_sequence, 0x61626364);
    EXPECT_EQ(h.record_count, 0x71727374);
}

// Boundary values for every field must round-trip without truncation or
// sign-extension errors.
TEST(record_batch_utils, disk_buf_extreme_values) {
    constexpr auto i16max = std::numeric_limits<int16_t>::max();
    constexpr auto i16min = std::numeric_limits<int16_t>::min();
    constexpr auto i32max = std::numeric_limits<int32_t>::max();
    constexpr auto i32min = std::numeric_limits<int32_t>::min();
    constexpr auto i64max = std::numeric_limits<int64_t>::max();
    constexpr auto i64min = std::numeric_limits<int64_t>::min();
    constexpr auto u32max = std::numeric_limits<uint32_t>::max();

    model::record_batch_header all_max{
      .header_crc = u32max,
      .size_bytes = i32max,
      .base_offset = model::offset(i64max),
      .type = model::record_batch_type::raft_data,
      .crc = u32max,
      .attrs = model::record_batch_attributes(i16max),
      .last_offset_delta = i32max,
      .first_timestamp = model::timestamp(i64max),
      .max_timestamp = model::timestamp(i64max),
      .producer_id = i64max,
      .producer_epoch = i16max,
      .base_sequence = i32max,
      .record_count = i32max};

    model::record_batch_header all_min{
      .header_crc = 0,
      .size_bytes = i32min,
      .base_offset = model::offset(i64min),
      .type = model::record_batch_type::raft_data,
      .crc = 0,
      .attrs = model::record_batch_attributes(i16min),
      .last_offset_delta = i32min,
      .first_timestamp = model::timestamp(i64min),
      .max_timestamp = model::timestamp(i64min),
      .producer_id = i64min,
      .producer_epoch = i16min,
      .base_sequence = i32min,
      .record_count = i32min};

    expect_all_fields_eq(round_trip(all_max), all_max);
    expect_all_fields_eq(round_trip(all_min), all_min);
}

// Every record_batch_type value (a single byte) must decode correctly.
TEST(record_batch_utils, disk_buf_all_batch_types) {
    for (int8_t t = 1; t <= static_cast<int8_t>(model::record_batch_type::MAX);
         ++t) {
        auto hdr = random_header();
        hdr.type = static_cast<model::record_batch_type>(t);
        EXPECT_EQ(round_trip(hdr).type, hdr.type);
    }
}

// The contiguous-buffer and iobuf entry points share parse_header, so they must
// agree for any serialized header.
TEST(record_batch_utils, disk_buf_matches_iobuf) {
    for (int i = 0; i < 100; ++i) {
        auto hdr = random_header();
        auto buf = storage::batch_header_to_disk_iobuf(hdr);
        std::array<char, model::packed_record_batch_header_size> stack_buf{};
        iobuf::iterator_consumer(buf.cbegin(), buf.cend())
          .consume_to(stack_buf.size(), stack_buf.data());
        auto from_buf = storage::batch_header_from_disk_buf(stack_buf);
        auto from_iobuf = storage::batch_header_from_disk_iobuf(std::move(buf));
        expect_all_fields_eq(from_buf, from_iobuf);
    }
}
