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
// NOLINTEND(*magic-number*)

} // namespace serde::parquet
