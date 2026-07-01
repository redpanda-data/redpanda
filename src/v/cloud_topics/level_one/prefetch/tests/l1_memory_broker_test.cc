/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/prefetch/l1_memory_broker.h"
#include "config/property.h"

#include <gtest/gtest.h>

namespace cloud_topics::prefetch {

TEST(l1_memory_broker, reserve_release_and_limits) {
    auto b = config::mock_binding<size_t>(1000);
    l1_memory_broker br(b);
    EXPECT_EQ(br.available(), 1000u);
    EXPECT_TRUE(br.try_reserve(600));
    EXPECT_EQ(br.available(), 400u);
    EXPECT_FALSE(br.try_reserve(500)); // would exceed
    EXPECT_EQ(br.reserved(), 600u);
    br.borrow(700); // borrow may exceed budget
    EXPECT_TRUE(br.over_budget());
    br.release(700);
    br.release(600);
    EXPECT_EQ(br.reserved(), 0u);
}

TEST(l1_memory_broker, budget_accessor) {
    auto b = config::mock_binding<size_t>(4096);
    l1_memory_broker br(b);
    EXPECT_EQ(br.budget(), 4096u);
}

TEST(l1_memory_broker, available_saturates_at_zero) {
    auto b = config::mock_binding<size_t>(100);
    l1_memory_broker br(b);
    br.borrow(200); // borrow beyond budget
    EXPECT_EQ(br.available(), 0u);
    EXPECT_TRUE(br.over_budget());
    br.release(200);
    EXPECT_EQ(br.available(), 100u);
    EXPECT_FALSE(br.over_budget());
}

TEST(l1_memory_broker, try_reserve_exact_budget) {
    auto b = config::mock_binding<size_t>(500);
    l1_memory_broker br(b);
    EXPECT_TRUE(br.try_reserve(500));
    EXPECT_EQ(br.available(), 0u);
    EXPECT_FALSE(br.over_budget());
    EXPECT_FALSE(br.try_reserve(1));
    br.release(500);
    EXPECT_EQ(br.reserved(), 0u);
}

} // namespace cloud_topics::prefetch
