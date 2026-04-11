// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "gtest/gtest.h"
#include "lsm/db/tests/immutable_tree.h"

#include <string>
#include <vector>

namespace {

using tree = lsm::db::immutable_tree<int, std::string>;

std::vector<std::pair<int, std::string>> collect(const tree& t) {
    std::vector<std::pair<int, std::string>> result;
    t.for_each(
      [&](const int& k, const std::string& v) { result.emplace_back(k, v); });
    return result;
}

} // namespace

TEST(ImmutableTree, EmptyTree) {
    tree t;
    EXPECT_TRUE(t.empty());
    EXPECT_EQ(t.size(), 0);
    EXPECT_EQ(t.get(1), nullptr);
    EXPECT_TRUE(collect(t).empty());
}

TEST(ImmutableTree, InsertSingle) {
    tree t;
    auto t2 = t.insert(1, "one");

    // Original tree is unchanged.
    EXPECT_TRUE(t.empty());

    EXPECT_EQ(t2.size(), 1);
    ASSERT_NE(t2.get(1), nullptr);
    EXPECT_EQ(*t2.get(1), "one");
}

TEST(ImmutableTree, InsertMultiple) {
    auto t = tree{}.insert(3, "three").insert(1, "one").insert(2, "two");

    EXPECT_EQ(t.size(), 3);

    auto entries = collect(t);
    ASSERT_EQ(entries.size(), 3);
    EXPECT_EQ(entries[0], std::make_pair(1, std::string("one")));
    EXPECT_EQ(entries[1], std::make_pair(2, std::string("two")));
    EXPECT_EQ(entries[2], std::make_pair(3, std::string("three")));
}

TEST(ImmutableTree, InsertDuplicateReplacesValue) {
    auto t1 = tree{}.insert(1, "one");
    auto t2 = t1.insert(1, "ONE");

    EXPECT_EQ(t2.size(), 1);
    ASSERT_NE(t2.get(1), nullptr);
    EXPECT_EQ(*t2.get(1), "ONE");

    // Original is unchanged.
    ASSERT_NE(t1.get(1), nullptr);
    EXPECT_EQ(*t1.get(1), "one");
}

TEST(ImmutableTree, GetNonExistent) {
    auto t = tree{}.insert(1, "one").insert(3, "three");
    EXPECT_EQ(t.get(2), nullptr);
    EXPECT_EQ(t.get(0), nullptr);
    EXPECT_EQ(t.get(4), nullptr);
}

TEST(ImmutableTree, RemoveExisting) {
    auto t1 = tree{}.insert(1, "one").insert(2, "two").insert(3, "three");
    auto t2 = t1.remove(2);

    EXPECT_EQ(t2.size(), 2);
    EXPECT_EQ(t2.get(2), nullptr);
    ASSERT_NE(t2.get(1), nullptr);
    ASSERT_NE(t2.get(3), nullptr);

    // Original is unchanged.
    EXPECT_EQ(t1.size(), 3);
    ASSERT_NE(t1.get(2), nullptr);
}

TEST(ImmutableTree, RemoveNonExistent) {
    auto t1 = tree{}.insert(1, "one");
    auto t2 = t1.remove(99);

    EXPECT_EQ(t2.size(), 1);
    ASSERT_NE(t2.get(1), nullptr);
}

TEST(ImmutableTree, RemoveFromEmpty) {
    tree t;
    auto t2 = t.remove(1);
    EXPECT_TRUE(t2.empty());
}

TEST(ImmutableTree, RemoveOnlyElement) {
    auto t1 = tree{}.insert(1, "one");
    auto t2 = t1.remove(1);
    EXPECT_TRUE(t2.empty());
    EXPECT_EQ(t2.get(1), nullptr);
}

TEST(ImmutableTree, RemoveWithTwoChildren) {
    // Build a tree where the removed node has two children.
    auto t = tree{}.insert(2, "two").insert(1, "one").insert(3, "three");
    auto t2 = t.remove(2);

    EXPECT_EQ(t2.size(), 2);
    auto entries = collect(t2);
    ASSERT_EQ(entries.size(), 2);
    EXPECT_EQ(entries[0].first, 1);
    EXPECT_EQ(entries[1].first, 3);
}

TEST(ImmutableTree, ImmutabilityAcrossOperations) {
    auto t0 = tree{};
    auto t1 = t0.insert(5, "five");
    auto t2 = t1.insert(3, "three");
    auto t3 = t2.insert(7, "seven");
    auto t4 = t3.remove(5);

    // Each version should be independently valid.
    EXPECT_EQ(t0.size(), 0);
    EXPECT_EQ(t1.size(), 1);
    EXPECT_EQ(t2.size(), 2);
    EXPECT_EQ(t3.size(), 3);
    EXPECT_EQ(t4.size(), 2);

    EXPECT_EQ(t4.get(5), nullptr);
    ASSERT_NE(t4.get(3), nullptr);
    ASSERT_NE(t4.get(7), nullptr);

    // t3 still has key 5.
    ASSERT_NE(t3.get(5), nullptr);
    EXPECT_EQ(*t3.get(5), "five");
}

TEST(ImmutableTree, SortedInsertionStaysBalanced) {
    // Insert keys in ascending order (worst case for a naive BST).
    // If the balancing is wrong this will degenerate into a linked list.
    tree t;
    constexpr int n = 1000;
    for (int i = 0; i < n; ++i) {
        t = t.insert(i, std::to_string(i));
    }
    EXPECT_EQ(t.size(), n);

    // Verify sorted order via for_each.
    int prev = -1;
    int count = 0;
    t.for_each([&](const int& k, const std::string&) {
        EXPECT_GT(k, prev);
        prev = k;
        ++count;
    });
    EXPECT_EQ(count, n);
}

TEST(ImmutableTree, ReverseInsertionStaysBalanced) {
    tree t;
    constexpr int n = 1000;
    for (int i = n - 1; i >= 0; --i) {
        t = t.insert(i, std::to_string(i));
    }
    EXPECT_EQ(t.size(), n);

    auto entries = collect(t);
    for (int i = 0; i < n; ++i) {
        EXPECT_EQ(entries[i].first, i);
    }
}

TEST(ImmutableTree, InsertAndRemoveMany) {
    tree t;
    constexpr int n = 500;
    for (int i = 0; i < n; ++i) {
        t = t.insert(i, std::to_string(i));
    }
    // Remove even keys.
    for (int i = 0; i < n; i += 2) {
        t = t.remove(i);
    }
    EXPECT_EQ(t.size(), n / 2);

    // Only odd keys remain.
    for (int i = 0; i < n; ++i) {
        if (i % 2 == 0) {
            EXPECT_EQ(t.get(i), nullptr);
        } else {
            ASSERT_NE(t.get(i), nullptr);
            EXPECT_EQ(*t.get(i), std::to_string(i));
        }
    }

    // Verify sorted order.
    int prev = -1;
    t.for_each([&](const int& k, const std::string&) {
        EXPECT_GT(k, prev);
        prev = k;
    });
}

TEST(ImmutableTree, ForEachOnSingleElement) {
    auto t = tree{}.insert(42, "answer");
    int count = 0;
    t.for_each([&](const int& k, const std::string& v) {
        EXPECT_EQ(k, 42);
        EXPECT_EQ(v, "answer");
        ++count;
    });
    EXPECT_EQ(count, 1);
}
