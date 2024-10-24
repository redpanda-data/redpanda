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

#include "serde/thrift/compact.h"
#include "src/v/serde/thrift/compact.h"
#include "utils/vint.h"

#include <gtest/gtest.h>

#include <initializer_list>
#include <limits>
#include <utility>

// NOLINTNEXTLINE(*internal-linkage*)
void PrintTo(const iobuf& b, std::ostream* os) {
    *os << b.hexdump(b.size_bytes());
}

namespace serde::thrift {

namespace {

void buf_append(iobuf& b, uint8_t byte) { b.append(&byte, 1); }
void buf_append(iobuf& b, const bytes& byte) {
    b.append(byte.data(), byte.size());
}

template<typename... Args>
iobuf buf_from(const Args&... args) {
    iobuf b;
    (buf_append(b, args), ...);
    return b;
}

} // namespace

TEST(StructEncoding, Empty) {
    EXPECT_EQ(struct_encoder().write_stop(), buf_from(0));
}

TEST(StructEncoding, ShortFormStart) {
    struct_encoder encoder;
    encoder.write_field(1, field_type::i32, vint::to_bytes(1));
    EXPECT_EQ(std::move(encoder).write_stop(), buf_from(0b00010101, 2, 0));
}

TEST(StructEncoding, LongFormStart) {
    struct_encoder encoder;
    constexpr int16_t large_field_id = 25;
    encoder.write_field(large_field_id, field_type::i32, vint::to_bytes(1));
    EXPECT_EQ(
      std::move(encoder).write_stop(),
      buf_from(0b00000101, unsigned_vint::to_bytes(large_field_id), 2, 0));
}

TEST(StructEncoding, SmallFieldDelta) {
    struct_encoder encoder;
    encoder.write_field(1, field_type::i32, vint::to_bytes(1));
    encoder.write_field(2, field_type::i32, vint::to_bytes(1));
    EXPECT_EQ(
      std::move(encoder).write_stop(),
      buf_from(
        0b00010101, vint::to_bytes(1), 0b00010101, vint::to_bytes(1), 0));
}

TEST(StructEncoding, LargeFieldDelta) {
    struct_encoder encoder;
    encoder.write_field(1, field_type::i32, vint::to_bytes(1));
    constexpr int16_t large_field_id = 25;
    encoder.write_field(large_field_id, field_type::i32, vint::to_bytes(1));
    EXPECT_EQ(
      std::move(encoder).write_stop(),
      buf_from(
        0b00010101,
        vint::to_bytes(1),
        0b00000101,
        unsigned_vint::to_bytes(large_field_id),
        vint::to_bytes(1),
        0));
}

TEST(StructEncoding, NegativeFieldDelta) {
    struct_encoder encoder;
    constexpr int16_t large_field_id = 25;
    encoder.write_field(large_field_id, field_type::i32, vint::to_bytes(1));
    encoder.write_field(1, field_type::i32, vint::to_bytes(1));
    EXPECT_EQ(
      std::move(encoder).write_stop(),
      buf_from(
        0b00000101,
        unsigned_vint::to_bytes(large_field_id),
        vint::to_bytes(1),
        0b00000101,
        unsigned_vint::to_bytes(1),
        vint::to_bytes(1),
        0));
}

TEST(ListEncoding, Empty) {
    EXPECT_EQ(list_encoder(0, field_type::i32).finish(), buf_from(0b00000101));
}

TEST(ListEncoding, Single) {
    list_encoder encoder(1, field_type::i32);
    encoder.write_element(vint::to_bytes(1));
    EXPECT_EQ(
      std::move(encoder).finish(), buf_from(0b00010101, vint::to_bytes(1)));
}

TEST(ListEncoding, Large) {
    constexpr size_t large_count = 10'000;
    list_encoder encoder(large_count, field_type::i32);
    for (size_t i = 0; i < large_count; ++i) {
        encoder.write_element(vint::to_bytes(static_cast<int32_t>(i)));
    }
    EXPECT_EQ(
      std::move(encoder).finish().share(0, 3),
      buf_from(0b11110101, unsigned_vint::to_bytes(large_count)));
}

TEST(StringEncoding, Empty) {
    EXPECT_EQ(bytes_to_iobuf(encode_string("")), buf_from(0));
}

TEST(StringEncoding, Small) {
    EXPECT_EQ(bytes_to_iobuf(encode_string("a")), buf_from(1, 'a'));
}

TEST(StringEncoding, Large) {
    constexpr size_t large_count = 10'000;
    std::string str(large_count, 'a');
    EXPECT_EQ(
      bytes_to_iobuf(encode_string(str)).share(0, 2),
      buf_from(unsigned_vint::to_bytes(large_count)));
}

} // namespace serde::thrift
