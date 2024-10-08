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
} // namespace

// NOLINTBEGIN(*magic-number*)
TEST(PlainEncoding, BooleanBitPacking) {
    auto encoded = encode_plain(chunked_vector<boolean_value>{});
    EXPECT_EQ(encoded, buf_from());
    encoded = encode_plain(chunked_vector<boolean_value>{{true}});
    EXPECT_EQ(encoded, buf_from(0b00000001));
    encoded = encode_plain(chunked_vector<boolean_value>{{true}, {true}});
    EXPECT_EQ(encoded, buf_from(0b00000011));
    encoded = encode_plain(
      chunked_vector<boolean_value>{{true}, {false}, {true}});
    EXPECT_EQ(encoded, buf_from(0b00000101));
    encoded = encode_plain(chunked_vector<boolean_value>{
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
      {true}});
    EXPECT_EQ(encoded, buf_from(0b00011101, 0b00000101));
}

TEST(PlainEncoding, Int32) {
    auto encoded = encode_plain(chunked_vector<int32_value>{
      {42},
      {0},
      {65},
      {std::numeric_limits<int32_t>::max()},
      {std::numeric_limits<int32_t>::min()}});
    EXPECT_EQ(
      encoded,
      buf<5 * 4>("\x2A\x00\x00\x00"
                 "\x00\x00\x00\x00"
                 "\x41\x00\x00\x00"
                 "\xFF\xFF\xFF\x7F"
                 "\x00\x00\x00\x80"));
}

TEST(PlainEncoding, Int64) {
    auto encoded = encode_plain(chunked_vector<int64_value>{
      {42},
      {0},
      {65},
      {std::numeric_limits<int32_t>::min()},
      {std::numeric_limits<int32_t>::max()},
      {std::numeric_limits<int64_t>::min()},
      {std::numeric_limits<int64_t>::max()}});
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
    auto encoded = encode_plain(chunked_vector<float32_value>{
      {std::numeric_limits<float>::min()},
      {std::numeric_limits<float>::max()},
      {std::numeric_limits<float>::quiet_NaN()},
      {std::numeric_limits<float>::signaling_NaN()},
      {+0.0},
      {-0.0},
    });
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
    auto encoded = encode_plain(chunked_vector<float64_value>{
      {std::numeric_limits<double>::min()},
      {std::numeric_limits<double>::max()},
      {std::numeric_limits<double>::quiet_NaN()},
      {std::numeric_limits<double>::signaling_NaN()},
      {+0.0},
      {-0.0},
    });
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
    auto encoded = encode_plain(
      buffers<byte_array_value>({buf<3>("\x00\x01\x02"), buf<2>("\xFF\xF8")}));
    EXPECT_EQ(
      encoded,
      buf<13>("\x03\x00\x00\x00\x00\x01\x02"
              "\x02\x00\x00\x00\xFF\xF8"));
}

TEST(PlainEncoding, FixedBytes) {
    auto encoded = encode_plain(buffers<fixed_byte_array_value>({
      buf<1>("\x00"),
      buf<1>("\x05"),
      buf<1>("\xFF"),
      buf<1>("\xEE"),
    }));
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
    chunked_vector<uint8_t> levels(
      testcase.levels.begin(), testcase.levels.end());
    uint8_t max_value = 0;
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

// NOLINTEND(*magic-number*)

} // namespace serde::parquet
