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
#include "utils/vint.h"

#include <gtest/gtest.h>

#include <utility>

// NOLINTNEXTLINE(*internal-linkage*)
void PrintTo(const iobuf& b, std::ostream* os) {
    *os << "iobuf size: " << b.size_bytes();
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
    encoder.write_field(field_id(1), field_type::i32, vint::to_bytes(1));
    EXPECT_EQ(std::move(encoder).write_stop(), buf_from(0b00010101, 2, 0));
}

TEST(StructEncoding, LongFormStart) {
    struct_encoder encoder;
    constexpr field_id large_field_id = field_id(25);
    encoder.write_field(large_field_id, field_type::i32, vint::to_bytes(1));
    EXPECT_EQ(
      std::move(encoder).write_stop(),
      buf_from(0b00000101, vint::to_bytes(large_field_id), 2, 0));
}

TEST(StructEncoding, SmallFieldDelta) {
    struct_encoder encoder;
    encoder.write_field(field_id(1), field_type::i32, vint::to_bytes(1));
    encoder.write_field(field_id(2), field_type::i32, vint::to_bytes(1));
    EXPECT_EQ(
      std::move(encoder).write_stop(),
      buf_from(
        0b00010101, vint::to_bytes(1), 0b00010101, vint::to_bytes(1), 0));
}

TEST(StructEncoding, LargeFieldDelta) {
    struct_encoder encoder;
    encoder.write_field(field_id(1), field_type::i32, vint::to_bytes(1));
    constexpr auto large_field_id = field_id(25);
    encoder.write_field(large_field_id, field_type::i32, vint::to_bytes(1));
    EXPECT_EQ(
      std::move(encoder).write_stop(),
      buf_from(
        0b00010101,
        vint::to_bytes(1),
        0b00000101,
        vint::to_bytes(large_field_id),
        vint::to_bytes(1),
        0));
}

TEST(StructEncoding, NegativeFieldDelta) {
    struct_encoder encoder;
    constexpr auto large_field_id = field_id(25);
    encoder.write_field(large_field_id, field_type::i32, vint::to_bytes(1));
    encoder.write_field(field_id(1), field_type::i32, vint::to_bytes(1));
    EXPECT_EQ(
      std::move(encoder).write_stop(),
      buf_from(
        0b00000101,
        vint::to_bytes(large_field_id),
        vint::to_bytes(1),
        0b00000101,
        vint::to_bytes(1),
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

TEST(StructDecoding, Empty) {
    iobuf data = struct_encoder().write_stop();
    iobuf_parser parser(std::move(data));
    struct_decoder dec(parser);
    EXPECT_EQ(dec.read_field_header(), std::nullopt);
}

TEST(StructDecoding, SingleI32Field) {
    struct_encoder enc;
    enc.write_field(field_id(1), field_type::i32, vint::to_bytes(42));
    iobuf data = std::move(enc).write_stop();
    iobuf_parser parser(std::move(data));
    struct_decoder dec(parser);
    auto hdr = dec.read_field_header();
    ASSERT_TRUE(hdr.has_value());
    EXPECT_EQ(hdr->id, field_id(1));
    EXPECT_EQ(hdr->type, field_type::i32);
    EXPECT_EQ(decode_i32(parser), 42);
    EXPECT_EQ(dec.read_field_header(), std::nullopt);
}

TEST(StructDecoding, MultipleFields) {
    struct_encoder enc;
    enc.write_field(field_id(1), field_type::i32, vint::to_bytes(100));
    enc.write_field(field_id(2), field_type::i64, vint::to_bytes(int64_t{200}));
    enc.write_field(field_id(3), field_type::binary, encode_string("hello"));
    iobuf data = std::move(enc).write_stop();
    iobuf_parser parser(std::move(data));
    struct_decoder dec(parser);

    auto hdr1 = dec.read_field_header();
    ASSERT_TRUE(hdr1.has_value());
    EXPECT_EQ(hdr1->id, field_id(1));
    EXPECT_EQ(decode_i32(parser), 100);

    auto hdr2 = dec.read_field_header();
    ASSERT_TRUE(hdr2.has_value());
    EXPECT_EQ(hdr2->id, field_id(2));
    EXPECT_EQ(decode_i64(parser), 200);

    auto hdr3 = dec.read_field_header();
    ASSERT_TRUE(hdr3.has_value());
    EXPECT_EQ(hdr3->id, field_id(3));
    EXPECT_EQ(decode_string(parser), "hello");

    EXPECT_EQ(dec.read_field_header(), std::nullopt);
}

TEST(StructDecoding, LargeFieldId) {
    struct_encoder enc;
    enc.write_field(field_id(25), field_type::i32, vint::to_bytes(7));
    iobuf data = std::move(enc).write_stop();
    iobuf_parser parser(std::move(data));
    struct_decoder dec(parser);
    auto hdr = dec.read_field_header();
    ASSERT_TRUE(hdr.has_value());
    EXPECT_EQ(hdr->id, field_id(25));
    EXPECT_EQ(decode_i32(parser), 7);
    EXPECT_EQ(dec.read_field_header(), std::nullopt);
}

TEST(StructDecoding, NegativeFieldDelta) {
    struct_encoder enc;
    enc.write_field(field_id(25), field_type::i32, vint::to_bytes(1));
    enc.write_field(field_id(1), field_type::i32, vint::to_bytes(2));
    iobuf data = std::move(enc).write_stop();
    iobuf_parser parser(std::move(data));
    struct_decoder dec(parser);

    auto hdr1 = dec.read_field_header();
    ASSERT_TRUE(hdr1.has_value());
    EXPECT_EQ(hdr1->id, field_id(25));
    EXPECT_EQ(decode_i32(parser), 1);

    auto hdr2 = dec.read_field_header();
    ASSERT_TRUE(hdr2.has_value());
    EXPECT_EQ(hdr2->id, field_id(1));
    EXPECT_EQ(decode_i32(parser), 2);

    EXPECT_EQ(dec.read_field_header(), std::nullopt);
}

TEST(StructDecoding, BooleanFields) {
    struct_encoder enc;
    enc.write_field(field_id(1), field_type::boolean_true, bytes{});
    enc.write_field(field_id(2), field_type::boolean_false, bytes{});
    iobuf data = std::move(enc).write_stop();
    iobuf_parser parser(std::move(data));
    struct_decoder dec(parser);

    auto hdr1 = dec.read_field_header();
    ASSERT_TRUE(hdr1.has_value());
    EXPECT_EQ(hdr1->id, field_id(1));
    EXPECT_EQ(hdr1->type, field_type::boolean_true);

    auto hdr2 = dec.read_field_header();
    ASSERT_TRUE(hdr2.has_value());
    EXPECT_EQ(hdr2->id, field_id(2));
    EXPECT_EQ(hdr2->type, field_type::boolean_false);

    EXPECT_EQ(dec.read_field_header(), std::nullopt);
}

TEST(StructDecoding, SkipUnknownFields) {
    struct_encoder enc;
    enc.write_field(field_id(1), field_type::i32, vint::to_bytes(42));
    enc.write_field(field_id(2), field_type::i64, vint::to_bytes(int64_t{99}));
    enc.write_field(field_id(3), field_type::i32, vint::to_bytes(7));
    iobuf data = std::move(enc).write_stop();
    iobuf_parser parser(std::move(data));
    struct_decoder dec(parser);

    // Read field 1
    auto hdr1 = dec.read_field_header();
    EXPECT_EQ(hdr1->id, field_id(1));
    EXPECT_EQ(decode_i32(parser), 42);

    // Skip field 2 (unknown)
    auto hdr2 = dec.read_field_header();
    EXPECT_EQ(hdr2->id, field_id(2));
    dec.skip_field(hdr2->type);

    // Read field 3
    auto hdr3 = dec.read_field_header();
    EXPECT_EQ(hdr3->id, field_id(3));
    EXPECT_EQ(decode_i32(parser), 7);

    EXPECT_EQ(dec.read_field_header(), std::nullopt);
}

TEST(StructDecoding, NestedStruct) {
    // Inner struct: field 1 = i32(99)
    struct_encoder inner;
    inner.write_field(field_id(1), field_type::i32, vint::to_bytes(99));
    auto inner_data = std::move(inner).write_stop();

    // Outer struct: field 1 = i32(42), field 2 = inner struct
    struct_encoder outer;
    outer.write_field(field_id(1), field_type::i32, vint::to_bytes(42));
    outer.write_field(
      field_id(2), field_type::structure, std::move(inner_data));
    iobuf data = std::move(outer).write_stop();

    iobuf_parser parser(std::move(data));
    struct_decoder dec(parser);

    auto hdr1 = dec.read_field_header();
    EXPECT_EQ(hdr1->id, field_id(1));
    EXPECT_EQ(decode_i32(parser), 42);

    auto hdr2 = dec.read_field_header();
    EXPECT_EQ(hdr2->id, field_id(2));
    EXPECT_EQ(hdr2->type, field_type::structure);

    // Decode nested struct
    struct_decoder nested(parser);
    auto nhdr = nested.read_field_header();
    ASSERT_TRUE(nhdr.has_value());
    EXPECT_EQ(nhdr->id, field_id(1));
    EXPECT_EQ(decode_i32(parser), 99);
    EXPECT_EQ(nested.read_field_header(), std::nullopt);

    EXPECT_EQ(dec.read_field_header(), std::nullopt);
}

TEST(StructDecoding, SkipNestedStruct) {
    struct_encoder inner;
    inner.write_field(field_id(1), field_type::i32, vint::to_bytes(99));
    inner.write_field(field_id(2), field_type::binary, encode_string("nested"));
    auto inner_data = std::move(inner).write_stop();

    struct_encoder outer;
    outer.write_field(
      field_id(1), field_type::structure, std::move(inner_data));
    outer.write_field(field_id(2), field_type::i32, vint::to_bytes(7));
    iobuf data = std::move(outer).write_stop();

    iobuf_parser parser(std::move(data));
    struct_decoder dec(parser);

    // Skip nested struct
    auto hdr1 = dec.read_field_header();
    EXPECT_EQ(hdr1->type, field_type::structure);
    dec.skip_field(hdr1->type);

    // Read the field after the nested struct
    auto hdr2 = dec.read_field_header();
    EXPECT_EQ(hdr2->id, field_id(2));
    EXPECT_EQ(decode_i32(parser), 7);

    EXPECT_EQ(dec.read_field_header(), std::nullopt);
}

TEST(ListDecoding, DecodeEmpty) {
    auto data = list_encoder(0, field_type::i32).finish();
    iobuf_parser parser(std::move(data));
    list_decoder dec(parser);
    EXPECT_EQ(dec.element_type(), field_type::i32);
    EXPECT_EQ(dec.size(), 0);
}

TEST(ListDecoding, DecodeSingle) {
    list_encoder enc(1, field_type::i32);
    enc.write_element(vint::to_bytes(42));
    auto data = std::move(enc).finish();
    iobuf_parser parser(std::move(data));
    list_decoder dec(parser);
    EXPECT_EQ(dec.element_type(), field_type::i32);
    EXPECT_EQ(dec.size(), 1);
    EXPECT_EQ(decode_i32(parser), 42);
}

TEST(ListDecoding, DecodeLarge) {
    constexpr size_t count = 10'000;
    list_encoder enc(count, field_type::i32);
    for (size_t i = 0; i < count; ++i) {
        enc.write_element(vint::to_bytes(static_cast<int32_t>(i)));
    }
    auto data = std::move(enc).finish();
    iobuf_parser parser(std::move(data));
    list_decoder dec(parser);
    EXPECT_EQ(dec.element_type(), field_type::i32);
    EXPECT_EQ(dec.size(), count);
    for (size_t i = 0; i < count; ++i) {
        EXPECT_EQ(decode_i32(parser), static_cast<int32_t>(i));
    }
}

TEST(ListDecoding, ListOfStrings) {
    list_encoder enc(3, field_type::binary);
    enc.write_element(bytes_to_iobuf(encode_string("alpha")));
    enc.write_element(bytes_to_iobuf(encode_string("beta")));
    enc.write_element(bytes_to_iobuf(encode_string("gamma")));
    auto data = std::move(enc).finish();
    iobuf_parser parser(std::move(data));
    list_decoder dec(parser);
    EXPECT_EQ(dec.size(), 3);
    EXPECT_EQ(decode_string(parser), "alpha");
    EXPECT_EQ(decode_string(parser), "beta");
    EXPECT_EQ(decode_string(parser), "gamma");
}

TEST(ListDecoding, SkipList) {
    // Struct: field 1 = list of i32, field 2 = i32(7)
    list_encoder list_enc(3, field_type::i32);
    list_enc.write_element(vint::to_bytes(1));
    list_enc.write_element(vint::to_bytes(2));
    list_enc.write_element(vint::to_bytes(3));

    struct_encoder enc;
    enc.write_field(
      field_id(1), field_type::list, std::move(list_enc).finish());
    enc.write_field(field_id(2), field_type::i32, vint::to_bytes(7));
    iobuf data = std::move(enc).write_stop();

    iobuf_parser parser(std::move(data));
    struct_decoder dec(parser);

    auto hdr1 = dec.read_field_header();
    EXPECT_EQ(hdr1->type, field_type::list);
    dec.skip_field(hdr1->type);

    auto hdr2 = dec.read_field_header();
    EXPECT_EQ(hdr2->id, field_id(2));
    EXPECT_EQ(decode_i32(parser), 7);
}

TEST(StringDecoding, RoundTrip) {
    auto encoded = bytes_to_iobuf(encode_string("hello world"));
    iobuf_parser parser(std::move(encoded));
    EXPECT_EQ(decode_string(parser), "hello world");
    EXPECT_EQ(parser.bytes_left(), 0);
}

TEST(StringDecoding, Empty) {
    auto encoded = bytes_to_iobuf(encode_string(""));
    iobuf_parser parser(std::move(encoded));
    EXPECT_EQ(decode_string(parser), "");
    EXPECT_EQ(parser.bytes_left(), 0);
}

TEST(BinaryDecoding, RoundTrip) {
    iobuf original;
    original.append("binary\x00data", 11);
    auto encoded = encode_binary(original.copy());
    iobuf_parser parser(std::move(encoded));
    auto decoded = decode_binary(parser);
    EXPECT_EQ(decoded, original);
    EXPECT_EQ(parser.bytes_left(), 0);
}

TEST(IntDecoding, NegativeValues) {
    auto data = buf_from(vint::to_bytes(int64_t{-1}));
    iobuf_parser parser(std::move(data));
    EXPECT_EQ(decode_i32(parser), -1);
}

TEST(IntDecoding, I64LargeValues) {
    auto data = buf_from(
      vint::to_bytes(int64_t{std::numeric_limits<int64_t>::max()}));
    iobuf_parser parser(std::move(data));
    EXPECT_EQ(decode_i64(parser), std::numeric_limits<int64_t>::max());
}

TEST(IntDecoding, I64MinValue) {
    auto data = buf_from(
      vint::to_bytes(int64_t{std::numeric_limits<int64_t>::min()}));
    iobuf_parser parser(std::move(data));
    EXPECT_EQ(decode_i64(parser), std::numeric_limits<int64_t>::min());
}

TEST(IntDecoding, I16RoundTrip) {
    auto data = buf_from(
      vint::to_bytes(int64_t{std::numeric_limits<int16_t>::max()}));
    iobuf_parser parser(std::move(data));
    EXPECT_EQ(decode_i16(parser), std::numeric_limits<int16_t>::max());
}

TEST(IntDecoding, I16MinValue) {
    auto data = buf_from(
      vint::to_bytes(int64_t{std::numeric_limits<int16_t>::min()}));
    iobuf_parser parser(std::move(data));
    EXPECT_EQ(decode_i16(parser), std::numeric_limits<int16_t>::min());
}

TEST(IntDecoding, I16Overflow) {
    auto data = buf_from(
      vint::to_bytes(int64_t{std::numeric_limits<int16_t>::max() + 1}));
    iobuf_parser parser(std::move(data));
    EXPECT_THROW(decode_i16(parser), std::out_of_range);
}

TEST(IntDecoding, I32Overflow) {
    auto data = buf_from(
      vint::to_bytes(
        int64_t{int64_t{std::numeric_limits<int32_t>::max()} + 1}));
    iobuf_parser parser(std::move(data));
    EXPECT_THROW(decode_i32(parser), std::out_of_range);
}

TEST(StructDecoding, SkipMap) {
    // Hand-craft a map: 2 entries, key=i32, val=binary
    iobuf map_data;
    auto count_bytes = unsigned_vint::to_bytes(2);
    map_data.append(count_bytes.data(), count_bytes.size());
    // kv_type byte: key=i32(5) in high nibble, val=binary(8) in low nibble
    uint8_t kv_type = (0x05 << 4) | 0x08;
    map_data.append(&kv_type, 1);
    // entry 1
    auto k1 = vint::to_bytes(1);
    map_data.append(k1.data(), k1.size());
    map_data.append(bytes_to_iobuf(encode_string("ab")));
    // entry 2
    auto k2 = vint::to_bytes(2);
    map_data.append(k2.data(), k2.size());
    map_data.append(bytes_to_iobuf(encode_string("cd")));

    struct_encoder enc;
    enc.write_field(field_id(1), field_type::map, std::move(map_data));
    enc.write_field(field_id(2), field_type::i32, vint::to_bytes(42));
    iobuf data = std::move(enc).write_stop();

    iobuf_parser parser(std::move(data));
    struct_decoder dec(parser);
    auto hdr1 = dec.read_field_header();
    EXPECT_EQ(hdr1->type, field_type::map);
    dec.skip_field(hdr1->type);

    auto hdr2 = dec.read_field_header();
    EXPECT_EQ(hdr2->id, field_id(2));
    EXPECT_EQ(decode_i32(parser), 42);
}

TEST(StructDecoding, SkipEmptyMap) {
    // Empty map: count=0, no kv_type byte
    iobuf map_data;
    auto count_bytes = unsigned_vint::to_bytes(0);
    map_data.append(count_bytes.data(), count_bytes.size());

    struct_encoder enc;
    enc.write_field(field_id(1), field_type::map, std::move(map_data));
    enc.write_field(field_id(2), field_type::i32, vint::to_bytes(7));
    iobuf data = std::move(enc).write_stop();

    iobuf_parser parser(std::move(data));
    struct_decoder dec(parser);
    auto hdr1 = dec.read_field_header();
    dec.skip_field(hdr1->type);

    auto hdr2 = dec.read_field_header();
    EXPECT_EQ(hdr2->id, field_id(2));
    EXPECT_EQ(decode_i32(parser), 7);
}

TEST(ListDecoding, ShortFormBoundary) {
    // Size 14 is the max short form value
    list_encoder enc14(14, field_type::i32);
    for (int i = 0; i < 14; ++i) {
        enc14.write_element(vint::to_bytes(i));
    }
    auto data14 = std::move(enc14).finish();
    iobuf_parser parser14(std::move(data14));
    list_decoder dec14(parser14);
    EXPECT_EQ(dec14.size(), 14);
    for (int i = 0; i < 14; ++i) {
        EXPECT_EQ(decode_i32(parser14), i);
    }

    // Size 15 triggers long form
    list_encoder enc15(15, field_type::i32);
    for (int i = 0; i < 15; ++i) {
        enc15.write_element(vint::to_bytes(i));
    }
    auto data15 = std::move(enc15).finish();
    iobuf_parser parser15(std::move(data15));
    list_decoder dec15(parser15);
    EXPECT_EQ(dec15.size(), 15);
    for (int i = 0; i < 15; ++i) {
        EXPECT_EQ(decode_i32(parser15), i);
    }
}

} // namespace serde::thrift
