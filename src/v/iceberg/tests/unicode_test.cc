/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/field_name_comparison.h"
#include "iceberg/unicode.h"

#include <gtest/gtest.h>

#include <string_view>

using namespace iceberg;
using fnn = field_name_comparison;

struct NamesEqualCase {
    std::string_view a;
    std::string_view b;
    fnn norm;
    bool expected;
};

class NamesEqualTest : public ::testing::TestWithParam<NamesEqualCase> {};

TEST_P(NamesEqualTest, Equal) {
    const auto& p = GetParam();
    EXPECT_EQ(names_equal(p.a, p.b, p.norm), p.expected);
}

// field_name_comparison::verbatim — exact (case-sensitive) comparison.
INSTANTIATE_TEST_SUITE_P(
  NoNorm,
  NamesEqualTest,
  ::testing::Values(
    NamesEqualCase{"foo", "foo", fnn::verbatim, true},
    NamesEqualCase{"Foo", "foo", fnn::verbatim, false},
    NamesEqualCase{"FOO", "foo", fnn::verbatim, false},
    NamesEqualCase{"eventTimestamp", "eventTimestamp", fnn::verbatim, true},
    NamesEqualCase{"eventTimestamp", "eventtimestamp", fnn::verbatim, false}));

// field_name_comparison::lower_case — case-insensitive via Unicode case
// folding.
INSTANTIATE_TEST_SUITE_P(
  YesNormASCII,
  NamesEqualTest,
  ::testing::Values(
    NamesEqualCase{"foo", "foo", fnn::lower_case, true},
    NamesEqualCase{"Foo", "foo", fnn::lower_case, true},
    NamesEqualCase{"FOO", "foo", fnn::lower_case, true},
    NamesEqualCase{"userId", "userid", fnn::lower_case, true},
    NamesEqualCase{"eventTimestamp", "eventtimestamp", fnn::lower_case, true},
    NamesEqualCase{"FOO", "bar", fnn::lower_case, false}));

// Non-ASCII cases. utf8proc handles all Unicode case mappings including
// 1:many, and is fully deterministic (no system locale dependency).
TEST(NamesEqualNonASCII, Unicode) {
    // ẞ (U+1E9E) → ß (U+00DF): 1:1 mapping.
    EXPECT_TRUE(names_equal("\xE1\xBA\x9E", "\xC3\x9F", fnn::lower_case));

    // İ (U+0130 LATIN CAPITAL LETTER I WITH DOT ABOVE): full Unicode case
    // folding produces i + U+0307 COMBINING DOT ABOVE (a 1:many mapping).
    EXPECT_TRUE(names_equal("\xC4\xB0", "\x69\xCC\x87", fnn::lower_case));
    EXPECT_FALSE(names_equal("\xC4\xB0", "\x69", fnn::lower_case));
}
