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
#include "common.h"

#include <gtest/gtest.h>

TEST(Common, MakeRandomData) {
    auto d0 = make_random_data(100).get();
    auto d1 = make_random_data(100).get();
    EXPECT_EQ(d0.size(), 100);
    EXPECT_EQ(d1.size(), 100);
    EXPECT_NE(d0, d1);
}

TEST(Common, MakeRandomDataSeed) {
    auto d0 = make_random_data(100, std::nullopt, 10).get();
    auto d1 = make_random_data(100, std::nullopt, 11).get();
    auto d2 = make_random_data(100, std::nullopt, 10).get();
    EXPECT_EQ(d0.size(), 100);
    EXPECT_EQ(d1.size(), 100);
    EXPECT_EQ(d2.size(), 100);
    EXPECT_NE(d0, d1);
    EXPECT_EQ(d0, d2);
}

TEST(Common, MakeRandomDataAlignment) {
    auto d0 = make_random_data(4096, 4096).get();
    auto d1 = make_random_data(16384, 4096).get();
    auto d2 = make_random_data(32768, 8192).get();
    EXPECT_EQ(d0.size(), 4096);
    EXPECT_EQ(d1.size(), 16384);
    EXPECT_EQ(d2.size(), 32768);
    // NOLINTBEGIN(cppcoreguidelines-pro-type-reinterpret-cast)
    EXPECT_EQ(reinterpret_cast<uintptr_t>(d0.get()) % 4096, 0);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(d1.get()) % 4096, 0);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(d2.get()) % 8192, 0);
    // NOLINTEND(cppcoreguidelines-pro-type-reinterpret-cast)
}
