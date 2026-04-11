// Copyright (c) 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found at https://github.com/google/leveldb/blob/main/LICENSE. See
// https://github.com/google/leveldb/blob/main/AUTHORS for names of
// contributors.
//
// Modifications copyright 2025 Redpanda Data, Inc.

#include "base/format_to.h"
#include "gmock/gmock-matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "lsm/db/weak_intrusive_list.h"

#include <seastar/core/shared_ptr.hh>

namespace {
struct foo;

using list = lsm::db::weak_intrusive_list<foo>;

struct foo : list {
    explicit foo(int v)
      : value(v) {}
    int value;
    ~foo() = default;

    bool operator==(const foo& other) const { return value == other.value; };
    fmt::iterator format_to(fmt::iterator it) const {
        return fmt::format_to(it, "{{value:{}}}", value);
    }
};

std::vector<int> collect_tail(ss::lw_shared_ptr<foo> head) {
    std::vector<int> v;
    for (auto curr = head; curr != nullptr; curr = *curr->next()) {
        v.push_back(curr->value);
    }
    return v;
}
} // namespace

TEST(WeakIntrusiveList, OnlyKeepsHeadReference) {
    ss::lw_shared_ptr<foo> head;
    list::push_front(&head, ss::make_lw_shared<foo>(1));
    ASSERT_TRUE(head);
    EXPECT_EQ(head->value, 1);
    EXPECT_FALSE(head->next());
    list::push_front(&head, ss::make_lw_shared<foo>(2));
    ASSERT_TRUE(head);
    EXPECT_EQ(head->value, 2);
    EXPECT_FALSE(head->next());
}

using ::testing::ElementsAre;
using ::testing::IsEmpty;

TEST(WeakIntrusiveList, LiveAreReachableViaIteration) {
    auto a = ss::make_lw_shared<foo>(1);
    auto b = ss::make_lw_shared<foo>(2);
    auto c = ss::make_lw_shared<foo>(3);
    ss::lw_shared_ptr<foo> head;
    EXPECT_THAT(collect_tail(head), IsEmpty()) << head;
    list::push_front(&head, a);
    EXPECT_THAT(collect_tail(head), ElementsAre(1)) << head;
    list::push_front(&head, b);
    EXPECT_THAT(collect_tail(head), ElementsAre(2, 1)) << head;
    list::push_front(&head, c);
    EXPECT_THAT(collect_tail(head), ElementsAre(3, 2, 1)) << head;
    b = nullptr;
    EXPECT_THAT(collect_tail(head), ElementsAre(3, 1)) << head;
    c = nullptr;
    EXPECT_THAT(collect_tail(head), ElementsAre(3, 1)) << head;
    a = nullptr;
    EXPECT_THAT(collect_tail(head), ElementsAre(3)) << head;
    head = nullptr;
    EXPECT_THAT(collect_tail(head), IsEmpty()) << head;
}
