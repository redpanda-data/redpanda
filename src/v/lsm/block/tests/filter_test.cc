// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "base/seastarx.h"
#include "lsm/block/contents.h"
#include "lsm/block/filter.h"
#include "lsm/core/internal/keys.h"

#include <seastar/core/file.hh>

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

namespace {

using key_vector = std::vector<std::string_view>;
using keys_by_block = std::map<uint64_t, key_vector>;

lsm::block::filter_reader make_filter(const keys_by_block& keys) {
    lsm::block::filter_builder builder({});
    for (const auto& [block, keys_in_block] : keys) {
        builder.start_block(block);
        for (const auto& key : keys_in_block) {
            builder.add_key(
              lsm::internal::key::encode({.key = lsm::user_key_view(key)}));
        }
    }
    auto c = lsm::block::contents::copy_from(builder.finish());
    return lsm::block::filter_reader(std::move(c));
}

using lsm::internal::operator""_key;

} // namespace

TEST(Filter, Empty) {
    auto filter = make_filter({});
    ASSERT_TRUE(filter.key_may_match(0, "foo"_key));
    ASSERT_TRUE(filter.key_may_match(100000, "foo"_key));
}

TEST(Filter, Bloom) {
    auto reader = make_filter({
      {0, {"hello", "world"}},
    });
    EXPECT_TRUE(reader.key_may_match(0, "hello"_key));
    EXPECT_TRUE(reader.key_may_match(0, "world"_key));
    EXPECT_FALSE(reader.key_may_match(0, "x"_key));
    EXPECT_FALSE(reader.key_may_match(0, "foo"_key));
}

TEST(Filter, SingleBlock) {
    auto reader = make_filter({
      {100, {"foo", "bar", "box"}},
      {200, {"box"}},
      {300, {"hello"}},
    });
    EXPECT_TRUE(reader.key_may_match(100, "foo"_key));
    EXPECT_TRUE(reader.key_may_match(100, "bar"_key));
    EXPECT_TRUE(reader.key_may_match(100, "box"_key));
    EXPECT_TRUE(reader.key_may_match(100, "hello"_key));
    EXPECT_TRUE(reader.key_may_match(100, "foo"_key));
    EXPECT_FALSE(reader.key_may_match(100, "missing"_key));
    EXPECT_FALSE(reader.key_may_match(100, "other"_key));
}

TEST(FilterBlockTest, MultipleBlocks) {
    auto reader = make_filter({
      {0, {"foo"}},
      {2000, {"bar"}},
      {3100, {"box"}},
      {9000, {"box", "hello"}},
    });

    // Check first filter
    EXPECT_TRUE(reader.key_may_match(0, "foo"_key));
    EXPECT_TRUE(reader.key_may_match(2000, "bar"_key));
    EXPECT_FALSE(reader.key_may_match(0, "box"_key));
    EXPECT_FALSE(reader.key_may_match(0, "hello"_key));
    EXPECT_FALSE(reader.key_may_match(0, "world"_key));

    // Check second filter
    EXPECT_TRUE(reader.key_may_match(3100, "box"_key));
    EXPECT_FALSE(reader.key_may_match(3100, "foo"_key));
    EXPECT_FALSE(reader.key_may_match(3100, "bar"_key));
    EXPECT_FALSE(reader.key_may_match(3100, "hello"_key));
    EXPECT_FALSE(reader.key_may_match(3100, "world"_key));

    // Check third filter (empty)
    EXPECT_FALSE(reader.key_may_match(4100, "foo"_key));
    EXPECT_FALSE(reader.key_may_match(4100, "bar"_key));
    EXPECT_FALSE(reader.key_may_match(4100, "box"_key));
    EXPECT_FALSE(reader.key_may_match(4100, "hello"_key));

    // Check last filter
    EXPECT_TRUE(reader.key_may_match(9000, "box"_key));
    EXPECT_TRUE(reader.key_may_match(9000, "hello"_key));
    EXPECT_FALSE(reader.key_may_match(9000, "foo"_key));
    EXPECT_FALSE(reader.key_may_match(9000, "bar"_key));
}
