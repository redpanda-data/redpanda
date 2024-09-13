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

#include "bytes/bytes.h"
#include "bytes/iobuf_parser.h"
#include "gtest/gtest.h"
#include "wasm/parser/leb128.h"

#include <gtest/gtest.h>

namespace wasm::parser::leb128 {

template<typename T>
struct test_data {
    T decoded;
    bytes encoded;
};

template<typename T>
testing::AssertionResult run_test(const test_data<T>& testcase) {
    if (encode<T>(testcase.decoded) != testcase.encoded) {
        return testing::AssertionFailure() << "encoding: " << testcase.decoded;
    }

    auto p = iobuf_parser(bytes_to_iobuf(testcase.encoded));
    if (decode<T>(&p) != testcase.decoded) {
        return testing::AssertionFailure() << "decoding: " << testcase.decoded;
    }
    return testing::AssertionSuccess();
}

template<typename T>
class RoundTripTest : public testing::TestWithParam<test_data<T>> {};

class SignedInt32RoundTripTest : public RoundTripTest<int32_t> {};
class UnsignedInt32RoundTripTest : public RoundTripTest<uint32_t> {};
class SignedInt64RoundTripTest : public RoundTripTest<int64_t> {};
class UnsignedInt64RoundTripTest : public RoundTripTest<uint64_t> {};

TEST_P(SignedInt32RoundTripTest, RoundTrip) {
    EXPECT_TRUE(run_test(GetParam()));
}
TEST_P(UnsignedInt32RoundTripTest, RoundTrip) {
    EXPECT_TRUE(run_test(GetParam()));
}
TEST_P(SignedInt64RoundTripTest, RoundTrip) {
    EXPECT_TRUE(run_test(GetParam()));
}
TEST_P(UnsignedInt64RoundTripTest, RoundTrip) {
    EXPECT_TRUE(run_test(GetParam()));
}

// NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers)
INSTANTIATE_TEST_SUITE_P(
  InterestingSignedIntegers,
  SignedInt32RoundTripTest,
  testing::ValuesIn<std::vector<test_data<int32_t>>>(
    {{.decoded = std::numeric_limits<int32_t>::min(),
      .encoded = {0x80, 0x80, 0x80, 0x80, 0x78}},
     {.decoded = -165675008, .encoded = {0x80, 0x80, 0x80, 0xb1, 0x7f}},
     {.decoded = -624485, .encoded = {0x9b, 0xf1, 0x59}},
     {.decoded = -16256, .encoded = {0x80, 0x81, 0x7f}},
     {.decoded = -4, .encoded = {0x7c}},
     {.decoded = -1, .encoded = {0x7f}},
     {.decoded = 0, .encoded = {0x00}},
     {.decoded = 1, .encoded = {0x01}},
     {.decoded = 4, .encoded = {0x04}},
     {.decoded = 16256, .encoded = {0x80, 0xff, 0x0}},
     {.decoded = 624485, .encoded = {0xe5, 0x8e, 0x26}},
     {.decoded = 165675008, .encoded = {0x80, 0x80, 0x80, 0xcf, 0x0}},
     {.decoded = std::numeric_limits<int32_t>::max(),
      .encoded = {0xff, 0xff, 0xff, 0xff, 0x7}}}));

INSTANTIATE_TEST_SUITE_P(
  InterestingUnsignedIntegers,
  UnsignedInt32RoundTripTest,
  testing::ValuesIn<std::vector<test_data<uint32_t>>>(
    {{.decoded = 0, .encoded = {0x00}},
     {.decoded = 1, .encoded = {0x01}},
     {.decoded = 4, .encoded = {0x04}},
     {.decoded = 16256, .encoded = {0x80, 0x7f}},
     {.decoded = 624485, .encoded = {0xe5, 0x8e, 0x26}},
     {.decoded = 165675008, .encoded = {0x80, 0x80, 0x80, 0x4f}},
     {.decoded = std::numeric_limits<uint32_t>::max(),
      .encoded = {0xff, 0xff, 0xff, 0xff, 0xf}}}));

INSTANTIATE_TEST_SUITE_P(
  InterestingUnsignedLongs,
  UnsignedInt64RoundTripTest,
  testing::ValuesIn<std::vector<test_data<uint64_t>>>(
    {{.decoded = 0, .encoded = {0x00}},
     {.decoded = 1, .encoded = {0x01}},
     {.decoded = 4, .encoded = {0x04}},
     {.decoded = 16256, .encoded = {0x80, 0x7f}},
     {.decoded = 624485, .encoded = {0xe5, 0x8e, 0x26}},
     {.decoded = 165675008, .encoded = {0x80, 0x80, 0x80, 0x4f}},
     {.decoded = std::numeric_limits<uint32_t>::max(),
      .encoded = {0xff, 0xff, 0xff, 0xff, 0xf}},
     {.decoded = std::numeric_limits<uint64_t>::max(),
      .encoded = {
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x1}}}));

INSTANTIATE_TEST_SUITE_P(
  InterestingSignedLongs,
  SignedInt64RoundTripTest,
  testing::ValuesIn<std::vector<test_data<int64_t>>>({
    {.decoded = -std::numeric_limits<int32_t>::max(),
     .encoded = {0x81, 0x80, 0x80, 0x80, 0x78}},
    {.decoded = -165675008, .encoded = {0x80, 0x80, 0x80, 0xb1, 0x7f}},
    {.decoded = -624485, .encoded = {0x9b, 0xf1, 0x59}},
    {.decoded = -16256, .encoded = {0x80, 0x81, 0x7f}},
    {.decoded = -4, .encoded = {0x7c}},
    {.decoded = -1, .encoded = {0x7f}},
    {.decoded = 0, .encoded = {0x00}},
    {.decoded = 1, .encoded = {0x01}},
    {.decoded = 4, .encoded = {0x04}},
    {.decoded = 16256, .encoded = {0x80, 0xff, 0x0}},
    {.decoded = 624485, .encoded = {0xe5, 0x8e, 0x26}},
    {.decoded = 165675008, .encoded = {0x80, 0x80, 0x80, 0xcf, 0x0}},
    {.decoded = std::numeric_limits<int32_t>::max(),
     .encoded = {0xff, 0xff, 0xff, 0xff, 0x7}},
    {.decoded = std::numeric_limits<int64_t>::max(),
     .encoded = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0}},
  }));

TEST(Overflow, Long) {
    bytes encoded(bytes::initialized_later{}, size_t(11));
    std::fill(encoded.begin(), encoded.end(), 0xff);
    auto p = iobuf_parser(bytes_to_iobuf(encoded));
    EXPECT_THROW(decode<int64_t>(&p), decode_exception);
    p = iobuf_parser(bytes_to_iobuf(encoded));
    EXPECT_THROW(decode<uint64_t>(&p), decode_exception);
}
TEST(Overflow, Int) {
    bytes encoded = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
    auto p = iobuf_parser(bytes_to_iobuf(encoded));
    EXPECT_THROW(decode<int32_t>(&p), decode_exception);
    p = iobuf_parser(bytes_to_iobuf(encoded));
    EXPECT_THROW(decode<uint32_t>(&p), decode_exception);
}
// NOLINTEND(cppcoreguidelines-avoid-magic-numbers)
} // namespace wasm::parser::leb128
