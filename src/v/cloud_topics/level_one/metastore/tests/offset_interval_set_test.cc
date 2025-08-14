/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "bytes/iobuf.h"
#include "cloud_topics/level_one/metastore/offset_interval_set.h"
#include "gmock/gmock.h"

#include <gtest/gtest.h>

using namespace experimental::cloud_topics::l1;
using o = kafka::offset;
namespace {
MATCHER_P2(MatchesRange, base, last, "") {
    return arg.base_offset == base && arg.last_offset == last;
}
} // namespace

TEST(OffsetIntervalSetTest, TestEmpty) {
    offset_interval_set s;
    ASSERT_TRUE(s.empty());
    ASSERT_TRUE(s.insert(o{0}, o{1}));
    ASSERT_FALSE(s.empty());
}
TEST(OffsetIntervalSetTest, TestInsertContains) {
    offset_interval_set s;
    ASSERT_TRUE(s.insert(o{1}, o{2}));
    ASSERT_TRUE(s.contains(o{1}));
    ASSERT_TRUE(s.contains(o{2}));
    ASSERT_FALSE(s.contains(o{0}));
    ASSERT_FALSE(s.contains(o{3}));

    ASSERT_TRUE(s.insert(o{2}, o{3}));
    ASSERT_TRUE(s.contains(o{1}));
    ASSERT_TRUE(s.contains(o{2}));
    ASSERT_TRUE(s.contains(o{3}));
    ASSERT_FALSE(s.contains(o{0}));
}
TEST(OffsetIntervalSetTest, TestStream) {
    offset_interval_set s;
    EXPECT_THAT(s.to_vec(), testing::ElementsAre());

    ASSERT_TRUE(s.insert(o{1}, o{2}));
    ASSERT_TRUE(s.insert(o{2}, o{3}));
    EXPECT_THAT(s.to_vec(), testing::ElementsAre(MatchesRange(o{1}, o{3})));

    ASSERT_TRUE(s.insert(o{4}, o{4}));
    EXPECT_THAT(s.to_vec(), testing::ElementsAre(MatchesRange(o{1}, o{4})));

    ASSERT_TRUE(s.insert(o{10}, o{10}));
    EXPECT_THAT(
      s.to_vec(),
      testing::ElementsAre(
        MatchesRange(o{1}, o{4}), MatchesRange(o{10}, o{10})));
}

TEST(OffsetIntervalSetTest, TestSerdeEmpty) {
    offset_interval_set s;
    iobuf b = serde::to_iobuf(s);
    auto roundtrip_s = serde::from_iobuf<offset_interval_set>(std::move(b));
    ASSERT_TRUE(s == roundtrip_s);
}

TEST(OffsetIntervalSetTest, TestSerde) {
    offset_interval_set s;
    ASSERT_TRUE(s.insert(o{3}, o{4}));
    ASSERT_TRUE(s.insert(o{0}, o{0}));
    ASSERT_TRUE(s.insert(o{2}, o{3}));
    iobuf b = serde::to_iobuf(s);
    auto roundtrip_s = serde::from_iobuf<offset_interval_set>(std::move(b));
    ASSERT_TRUE(s == roundtrip_s);
}
