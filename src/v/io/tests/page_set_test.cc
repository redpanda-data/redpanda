/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "io/page_set.h"

#include <seastar/util/later.hh>

#include <gtest/gtest.h>

#include <random>

namespace io = experimental::io;

namespace {
auto make_page(uint64_t offset, uint64_t size) {
    return seastar::make_lw_shared<io::page>(
      offset, seastar::temporary_buffer<char>(size));
}
} // namespace

TEST(PageSet, EmptyBeginEnd) {
    const io::page_set set;
    EXPECT_EQ(set.begin(), set.end());
}

TEST(PageSet, EmptyFind) {
    const io::page_set set;
    EXPECT_EQ(set.find(0), set.end());
}

TEST(PageSet, InsertOne) {
    io::page_set set;

    auto res = set.insert(make_page(0, 10));
    EXPECT_TRUE(res.second);
    EXPECT_EQ(set.find(0), res.first);

    EXPECT_NE(set.begin(), set.end());
    for (int i = 0; i < 10; ++i) {
        auto it = set.find(i);
        EXPECT_EQ((*it)->offset(), 0);
        EXPECT_EQ((*it)->size(), 10);
        EXPECT_EQ((*it)->data().size(), 10);
    }
    EXPECT_EQ(set.find(10), set.end());
}

TEST(PageSet, InsertOverlap) {
    io::page_set set;

    auto res = set.insert(make_page(0, 10));
    EXPECT_TRUE(res.second);
    EXPECT_EQ(set.find(0), res.first);

    auto res2 = set.insert(make_page(9, 10));
    EXPECT_FALSE(res2.second);
    EXPECT_EQ(res2.first, res.first);
}

TEST(PageSet, InsertEmpty) {
    io::page_set set;

    auto res = set.insert(make_page(0, 0));
    EXPECT_FALSE(res.second);
    EXPECT_EQ(res.first, set.end());
    EXPECT_EQ(set.begin(), set.end());

    res = set.insert(make_page(0, 10));
    EXPECT_TRUE(res.second);
    EXPECT_EQ(set.find(0), res.first);

    res = set.insert(make_page(0, 0));
    EXPECT_FALSE(res.second);
    EXPECT_EQ(res.first, set.end());
}

TEST(PageSet, Erase) {
    io::page_set set;

    // add two pages
    auto res = set.insert(make_page(0, 10));
    EXPECT_TRUE(res.second);
    res = set.insert(make_page(20, 10));
    EXPECT_TRUE(res.second);
    EXPECT_NE(set.begin(), set.end());

    // remove one
    set.erase(set.find(5));
    EXPECT_NE(set.begin(), set.end());

    // remove the other
    set.erase(set.find(25));

    // now its empty
    EXPECT_EQ(set.begin(), set.end());
}
