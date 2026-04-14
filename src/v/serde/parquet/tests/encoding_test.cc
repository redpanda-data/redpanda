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

#include "serde/parquet/encoding.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <limits>

// NOLINTNEXTLINE(*internal-linkage*)
void PrintTo(const iobuf& b, std::ostream* os) {
    *os << b.hexdump(b.size_bytes());
}

namespace serde::parquet {

namespace {
void buf_append(iobuf& b, uint8_t byte) { b.append(&byte, 1); }
template<typename... Args>
iobuf buf_from(const Args&... args) {
    iobuf b;
    (buf_append(b, args), ...);
    return b;
}

template<size_t N>
iobuf buf(const char* bytes) {
    iobuf b;
    b.append(bytes, N);
    return b;
}

template<typename ET, typename VT>
ET& add_values(ET& encoder, chunked_vector<VT> values) {
    for (auto& v : values) {
        encoder.add_value(std::move(v));
    }
    return encoder;
}
} // namespace

// NOLINTBEGIN(*magic-number*)
TEST(PlainEncoding, BooleanBitPacking) {
    plain_encoder<boolean_value> encoder{};
    auto encoded = encoder.get_encoded_buf();
    EXPECT_EQ(encoded, buf_from());
    encoded = add_values(encoder, chunked_vector<boolean_value>{{true}})
                .get_encoded_buf();
    EXPECT_EQ(encoded, buf_from(0b00000001));
    encoded = add_values(encoder, chunked_vector<boolean_value>{{true}, {true}})
                .get_encoded_buf();
    EXPECT_EQ(encoded, buf_from(0b00000011));
    encoded = add_values(
                encoder, chunked_vector<boolean_value>{{true}, {false}, {true}})
                .get_encoded_buf();
    EXPECT_EQ(encoded, buf_from(0b00000101));
    encoded = add_values(
                encoder,
                chunked_vector<boolean_value>{
                  {true},
                  {false},
                  {true},
                  {true},
                  {true},
                  {false},
                  {false},
                  {false},
                  {true},
                  {false},
                  {true}})
                .get_encoded_buf();
    EXPECT_EQ(encoded, buf_from(0b00011101, 0b00000101));
}

TEST(PlainEncoding, Int32) {
    plain_encoder<int32_value> encoder{};
    auto encoded = add_values(
                     encoder,
                     chunked_vector<int32_value>{
                       {42},
                       {0},
                       {65},
                       {std::numeric_limits<int32_t>::max()},
                       {std::numeric_limits<int32_t>::min()}})
                     .get_encoded_buf();
    EXPECT_EQ(
      encoded,
      buf<5 * 4>("\x2A\x00\x00\x00"
                 "\x00\x00\x00\x00"
                 "\x41\x00\x00\x00"
                 "\xFF\xFF\xFF\x7F"
                 "\x00\x00\x00\x80"));
}

TEST(PlainEncoding, Int64) {
    plain_encoder<int64_value> encoder{};
    auto encoded = add_values(
                     encoder,
                     chunked_vector<int64_value>{
                       {42},
                       {0},
                       {65},
                       {std::numeric_limits<int32_t>::min()},
                       {std::numeric_limits<int32_t>::max()},
                       {std::numeric_limits<int64_t>::min()},
                       {std::numeric_limits<int64_t>::max()}})
                     .get_encoded_buf();
    EXPECT_EQ(
      encoded,
      buf<8 * 7>("\x2A\x00\x00\x00\x00\x00\x00\x00"
                 "\x00\x00\x00\x00\x00\x00\x00\x00"
                 "\x41\x00\x00\x00\x00\x00\x00\x00"
                 "\x00\x00\x00\x80\xFF\xFF\xFF\xFF"
                 "\xFF\xFF\xFF\x7F\x00\x00\x00\x00"
                 "\x00\x00\x00\x00\x00\x00\x00\x80"
                 "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x7F"));
}

TEST(PlainEncoding, Float32) {
    plain_encoder<float32_value> encoder{};
    auto encoded = add_values(
                     encoder,
                     chunked_vector<float32_value>{
                       {std::numeric_limits<float>::min()},
                       {std::numeric_limits<float>::max()},
                       {std::numeric_limits<float>::quiet_NaN()},
                       {std::numeric_limits<float>::signaling_NaN()},
                       {+0.0},
                       {-0.0},
                     })
                     .get_encoded_buf();
    EXPECT_EQ(
      encoded,
      buf<4 * 6>("\x00\x00\x80\x00"
                 "\xFF\xFF\x7F\x7F"
                 "\x00\x00\xC0\x7F"
                 "\x00\x00\xA0\x7F"
                 "\x00\x00\x00\x00"
                 "\x00\x00\x00\x80"));
}

TEST(PlainEncoding, Float64) {
    plain_encoder<float64_value> encoder{};
    auto encoded = add_values(
                     encoder,
                     chunked_vector<float64_value>{
                       {std::numeric_limits<double>::min()},
                       {std::numeric_limits<double>::max()},
                       {std::numeric_limits<double>::quiet_NaN()},
                       {std::numeric_limits<double>::signaling_NaN()},
                       {+0.0},
                       {-0.0},
                     })
                     .get_encoded_buf();
    EXPECT_EQ(
      encoded,
      buf<8 * 6>("\x00\x00\x00\x00\x00\x00\x10\x00"
                 "\xFF\xFF\xFF\xFF\xFF\xFF\xEF\x7F"
                 "\x00\x00\x00\x00\x00\x00\xF8\x7F"
                 "\x00\x00\x00\x00\x00\x00\xF4\x7F"
                 "\x00\x00\x00\x00\x00\x00\x00\x00"
                 "\x00\x00\x00\x00\x00\x00\x00\x80"));
}

namespace {

template<typename T>
chunked_vector<T> buffers(std::initializer_list<iobuf> buffers) {
    chunked_vector<T> v;
    for (auto& b : buffers) {
        v.push_back(T{b.copy()});
    }
    return v;
}

} // namespace

TEST(PlainEncoding, VarBytes) {
    plain_encoder<byte_array_value> encoder;
    auto encoded = add_values(
                     encoder,
                     buffers<byte_array_value>(
                       {buf<3>("\x00\x01\x02"), buf<2>("\xFF\xF8")}))
                     .get_encoded_buf();
    EXPECT_EQ(
      encoded,
      buf<13>("\x03\x00\x00\x00\x00\x01\x02"
              "\x02\x00\x00\x00\xFF\xF8"));
}

TEST(PlainEncoding, FixedBytes) {
    plain_encoder<fixed_byte_array_value> encoder;
    auto encoded = add_values(
                     encoder,
                     buffers<fixed_byte_array_value>({
                       buf<1>("\x00"),
                       buf<1>("\x05"),
                       buf<1>("\xFF"),
                       buf<1>("\xEE"),
                     }))
                     .get_encoded_buf();
    EXPECT_EQ(encoded, buf<4>("\x00\x05\xFF\xEE"));
}

struct level_encoding_test_case {
    std::vector<int32_t> levels;
    std::vector<uint8_t> encoding;

    friend void PrintTo(const level_encoding_test_case& b, std::ostream* os) {
        *os << fmt::format("{{levels:[{}]}}", fmt::join(b.levels, ","));
    }
};

class LevelEncoding
  : public testing::TestWithParam<level_encoding_test_case> {};

TEST_P(LevelEncoding, CanEncode) {
    auto testcase = GetParam();
    chunked_vector<rep_level> levels;
    for (uint8_t l : testcase.levels) {
        levels.emplace_back(static_cast<int16_t>(l));
    }
    rep_level max_value = rep_level(0);
    if (!levels.empty()) {
        max_value = *std::ranges::max_element(levels);
    }
    iobuf actual = encode_levels(max_value, levels);
    iobuf expected;
    expected.append(testcase.encoding.data(), testcase.encoding.size());
    EXPECT_EQ(actual, expected);
}

// These test cases are from the parquet-go library
// Although we have a different (simpler less optimal) algorithm, so we get
// different encodings
INSTANTIATE_TEST_SUITE_P(
  ParquetGoTestCases,
  LevelEncoding,
  testing::Values(
    level_encoding_test_case{
      .levels = {},
      .encoding = {0x00, 0x00},
    },
    level_encoding_test_case{
      .levels = {0},
      .encoding = {0x02, 0x00},
    },
    level_encoding_test_case{
      .levels = {1},
      .encoding = {0x02, 0x01},
    },
    level_encoding_test_case{
      .levels = {0, 1, 0, 2, 3, 4, 5, 6, 127, 127, 0},
      .encoding = {
        0x02, 0x00,
        0x02, 0x01,
        0x02, 0x00,
        0x02, 0x02,
        0x02, 0x03,
        0x02, 0x04,
        0x02, 0x05,
        0x02, 0x06,
        0x04, 0x7F,
        0x02, 0x00,
      },
    },
    level_encoding_test_case{
      // 24 of the same value
      .levels = {42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42,
                 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42},
      .encoding = {0x30, 42}
    },
    level_encoding_test_case{
      // no repeats
      .levels = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
                 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F,
                 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
                 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F},
      .encoding = {
        0x02, 0x00,
        0x02, 0x01,
        0x02, 0x02,
        0x02, 0x03,
        0x02, 0x04,
        0x02, 0x05,
        0x02, 0x06,
        0x02, 0x07,
        0x02, 0x08,
        0x02, 0x09,
        0x02, 0x0A,
        0x02, 0x0B,
        0x02, 0x0C,
        0x02, 0x0D,
        0x02, 0x0E,
        0x02, 0x0F,
        0x02, 0x10,
        0x02, 0x11,
        0x02, 0x12,
        0x02, 0x13,
        0x02, 0x14,
        0x02, 0x15,
        0x02, 0x16,
        0x02, 0x17,
        0x02, 0x18,
        0x02, 0x19,
        0x02, 0x1A,
        0x02, 0x1B,
        0x02, 0x1C,
        0x02, 0x1D,
        0x02, 0x1E,
        0x02, 0x1F,
      }
    },
    level_encoding_test_case{
      // lots of repeats
      .levels = {
        0, 0, 0, 0,
        1, 1, 1, 1,
        2, 2, 2, 2,
        3, 3, 3, 3,
        4, 4, 4, 4,
        5, 5, 5, 5,
        6, 6, 6,
        7, 7, 7,
        8, 8, 8,
        9, 9, 9,
      },
      .encoding = {
        0x08, 0x00,
        0x08, 0x01,
        0x08, 0x02,
        0x08, 0x03,
        0x08, 0x04,
        0x08, 0x05,
        0x06, 0x06,
        0x06, 0x07,
        0x06, 0x08,
        0x06, 0x09,
      }
    }));

// --- Decoder Round-Trip Tests ---

TEST(LevelDecoding, RoundTripRLE) {
    chunked_vector<def_level> levels;
    for (int i = 0; i < 100; ++i) {
        levels.push_back(def_level(static_cast<int16_t>(i % 3)));
    }
    def_level max_val = def_level(2);
    auto encoded = encode_levels(max_val, levels);
    auto byte_length = static_cast<int32_t>(encoded.size_bytes());
    iobuf_parser parser(std::move(encoded));
    auto decoded = decode_levels(parser, 100, byte_length, max_val);
    ASSERT_EQ(decoded.size(), levels.size());
    for (size_t i = 0; i < levels.size(); ++i) {
        EXPECT_EQ(decoded[i], levels[i]) << "mismatch at index " << i;
    }
}

TEST(LevelDecoding, RoundTripAllSameValue) {
    chunked_vector<rep_level> levels;
    for (int i = 0; i < 50; ++i) {
        levels.push_back(rep_level(3));
    }
    rep_level max_val = rep_level(3);
    auto encoded = encode_levels(max_val, levels);
    auto byte_length = static_cast<int32_t>(encoded.size_bytes());
    iobuf_parser parser(std::move(encoded));
    auto decoded = decode_levels(parser, 50, byte_length, max_val);
    ASSERT_EQ(decoded.size(), 50);
    for (const auto& v : decoded) {
        EXPECT_EQ(v, rep_level(3));
    }
}

TEST(LevelDecoding, RoundTripAllZeros) {
    chunked_vector<def_level> levels;
    for (int i = 0; i < 20; ++i) {
        levels.push_back(def_level(0));
    }
    def_level max_val = def_level(0);
    auto encoded = encode_levels(max_val, levels);
    auto byte_length = static_cast<int32_t>(encoded.size_bytes());
    iobuf_parser parser(std::move(encoded));
    auto decoded = decode_levels(parser, 20, byte_length, max_val);
    ASSERT_EQ(decoded.size(), 20);
    for (const auto& v : decoded) {
        EXPECT_EQ(v, def_level(0));
    }
}

TEST(LevelDecoding, BitpackedInput) {
    // Hand-craft a bitpack encoded input:
    // bit_width=2, 1 group of 8 values: [0, 1, 2, 3, 0, 1, 2, 3]
    // Header: (1 << 1) | 1 = 3 (1 group, bitpack marker)
    // Data: 8 values * 2 bits = 16 bits = 2 bytes
    // Values packed LSB first:
    //   0=00, 1=01, 2=10, 3=11, 0=00, 1=01, 2=10, 3=11
    //   byte 0: 11 10 01 00 = 0xE4
    //   byte 1: 11 10 01 00 = 0xE4
    iobuf encoded;
    uint8_t header = 3; // (1 group << 1) | 1
    encoded.append(&header, 1);
    uint8_t b0 = 0xE4;
    uint8_t b1 = 0xE4;
    encoded.append(&b0, 1);
    encoded.append(&b1, 1);
    auto byte_length = static_cast<int32_t>(encoded.size_bytes());
    iobuf_parser parser(std::move(encoded));
    auto decoded = decode_levels(parser, 8, byte_length, def_level(3));
    ASSERT_EQ(decoded.size(), 8);
    EXPECT_EQ(decoded[0], def_level(0));
    EXPECT_EQ(decoded[1], def_level(1));
    EXPECT_EQ(decoded[2], def_level(2));
    EXPECT_EQ(decoded[3], def_level(3));
    EXPECT_EQ(decoded[4], def_level(0));
    EXPECT_EQ(decoded[5], def_level(1));
    EXPECT_EQ(decoded[6], def_level(2));
    EXPECT_EQ(decoded[7], def_level(3));
}

TEST(PlainDecoding, BooleanRoundTrip) {
    plain_encoder<boolean_value> enc;
    chunked_vector<boolean_value> values{
      {true},
      {false},
      {true},
      {true},
      {false},
      {false},
      {true},
      {false},
      {true}};
    for (auto& v : values) {
        enc.add_value(v);
    }
    auto encoded = enc.get_encoded_buf();
    iobuf_parser parser(std::move(encoded));
    plain_decoder<boolean_value> dec(parser);
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(dec.read_value(), values[i]) << "mismatch at " << i;
    }
}

TEST(PlainDecoding, Int32RoundTrip) {
    plain_encoder<int32_value> enc;
    chunked_vector<int32_value> values{
      {0},
      {42},
      {-1},
      {std::numeric_limits<int32_t>::max()},
      {std::numeric_limits<int32_t>::min()}};
    for (auto& v : values) {
        enc.add_value(v);
    }
    auto encoded = enc.get_encoded_buf();
    iobuf_parser parser(std::move(encoded));
    plain_decoder<int32_value> dec(parser);
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(dec.read_value(), values[i]) << "mismatch at " << i;
    }
}

TEST(PlainDecoding, Int64RoundTrip) {
    plain_encoder<int64_value> enc;
    chunked_vector<int64_value> values{
      {0},
      {-999},
      {std::numeric_limits<int64_t>::max()},
      {std::numeric_limits<int64_t>::min()}};
    for (auto& v : values) {
        enc.add_value(v);
    }
    auto encoded = enc.get_encoded_buf();
    iobuf_parser parser(std::move(encoded));
    plain_decoder<int64_value> dec(parser);
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(dec.read_value(), values[i]) << "mismatch at " << i;
    }
}

TEST(PlainDecoding, Float32RoundTrip) {
    plain_encoder<float32_value> enc;
    chunked_vector<float32_value> values{
      {0.0f},
      {-1.5f},
      {std::numeric_limits<float>::max()},
      {std::numeric_limits<float>::min()}};
    for (auto& v : values) {
        enc.add_value(v);
    }
    auto encoded = enc.get_encoded_buf();
    iobuf_parser parser(std::move(encoded));
    plain_decoder<float32_value> dec(parser);
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(dec.read_value(), values[i]) << "mismatch at " << i;
    }
}

TEST(PlainDecoding, Float64RoundTrip) {
    plain_encoder<float64_value> enc;
    chunked_vector<float64_value> values{
      {0.0}, {3.14159}, {std::numeric_limits<double>::max()}};
    for (auto& v : values) {
        enc.add_value(v);
    }
    auto encoded = enc.get_encoded_buf();
    iobuf_parser parser(std::move(encoded));
    plain_decoder<float64_value> dec(parser);
    for (size_t i = 0; i < values.size(); ++i) {
        EXPECT_EQ(dec.read_value(), values[i]) << "mismatch at " << i;
    }
}

TEST(PlainDecoding, ByteArrayRoundTrip) {
    plain_encoder<byte_array_value> enc;
    enc.add_value(byte_array_value{iobuf::from("hello")});
    enc.add_value(byte_array_value{iobuf::from("")});
    enc.add_value(byte_array_value{iobuf::from("world")});
    auto encoded = enc.get_encoded_buf();
    iobuf_parser parser(std::move(encoded));
    plain_decoder<byte_array_value> dec(parser);
    EXPECT_EQ(dec.read_value().val, iobuf::from("hello"));
    EXPECT_EQ(dec.read_value().val, iobuf::from(""));
    EXPECT_EQ(dec.read_value().val, iobuf::from("world"));
}

TEST(PlainDecoding, FixedByteArrayRoundTrip) {
    plain_encoder<fixed_byte_array_value> enc;
    enc.add_value(fixed_byte_array_value{iobuf::from("\x01\x02\x03\x04")});
    enc.add_value(fixed_byte_array_value{iobuf::from("\x05\x06\x07\x08")});
    auto encoded = enc.get_encoded_buf();
    iobuf_parser parser(std::move(encoded));
    plain_decoder<fixed_byte_array_value> dec(parser, 4);
    EXPECT_EQ(dec.read_value().val, iobuf::from("\x01\x02\x03\x04"));
    EXPECT_EQ(dec.read_value().val, iobuf::from("\x05\x06\x07\x08"));
}

TEST(RleBpInt32, RoundTripViaLevels) {
    chunked_vector<def_level> levels;
    for (int i = 0; i < 20; ++i) {
        levels.push_back(def_level(static_cast<int16_t>(i % 4)));
    }
    auto encoded = encode_levels(def_level(3), levels);
    auto byte_length = static_cast<int32_t>(encoded.size_bytes());
    iobuf_parser parser(std::move(encoded));
    auto decoded = decode_rle_bp_int32(parser, 20, byte_length, 2);
    ASSERT_EQ(decoded.size(), 20);
    for (int i = 0; i < 20; ++i) {
        EXPECT_EQ(decoded[i], i % 4) << "index " << i;
    }
}

TEST(RleBpInt32, AllZeros) {
    chunked_vector<def_level> levels;
    for (int i = 0; i < 10; ++i) {
        levels.push_back(def_level(0));
    }
    auto encoded = encode_levels(def_level(0), levels);
    auto byte_length = static_cast<int32_t>(encoded.size_bytes());
    iobuf_parser parser(std::move(encoded));
    auto decoded = decode_rle_bp_int32(parser, 10, byte_length, 0);
    ASSERT_EQ(decoded.size(), 10);
    for (const auto& v : decoded) {
        EXPECT_EQ(v, 0);
    }
}

// NOLINTEND(*magic-number*)

} // namespace serde::parquet
