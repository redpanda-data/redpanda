/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "kafka/protocol/api_key_indexed_array.h"
#include "kafka/protocol/types.h"

#include <gtest/gtest.h>

#include <stdexcept>
#include <vector>

namespace {

// Standard keys 0..3, reserved keys 100..101 (base 100, count 2).
using test_array = kafka::api_key_indexed_array<int, 4, 2, 100>;

kafka::api_key k(int16_t v) { return kafka::api_key(v); }

TEST(ApiKeyIndexedArray, routes_both_regions) {
    test_array a;
    a[k(0)] = 10;
    a[k(3)] = 13;
    a[k(100)] = 200;
    a[k(101)] = 201;

    EXPECT_EQ(a[k(0)], 10);
    EXPECT_EQ(a[k(3)], 13);
    EXPECT_EQ(a[k(100)], 200);
    EXPECT_EQ(a[k(101)], 201);
}

TEST(ApiKeyIndexedArray, at_returns_valid_indices) {
    test_array a{7};
    EXPECT_EQ(a.at(k(0)), 7);
    EXPECT_EQ(a.at(k(3)), 7);
    EXPECT_EQ(a.at(k(100)), 7);
    EXPECT_EQ(a.at(k(101)), 7);
}

TEST(ApiKeyIndexedArray, at_throws_for_out_of_range) {
    test_array a;
    EXPECT_THROW(a.at(k(-1)), std::out_of_range);  // negative
    EXPECT_THROW(a.at(k(4)), std::out_of_range);   // just past standard
    EXPECT_THROW(a.at(k(99)), std::out_of_range);  // gap below reserved
    EXPECT_THROW(a.at(k(102)), std::out_of_range); // just past reserved
}

TEST(ApiKeyIndexedArray, contains_and_find) {
    test_array a;
    for (auto key : {0, 3, 100, 101}) {
        EXPECT_TRUE(a.contains(k(key))) << key;
        EXPECT_NE(a.find(k(key)), nullptr) << key;
    }
    for (auto key : {-1, 4, 99, 102}) {
        EXPECT_FALSE(a.contains(k(key))) << key;
        EXPECT_EQ(a.find(k(key)), nullptr) << key;
    }
}

TEST(ApiKeyIndexedArray, for_each_yields_real_keys) {
    test_array a;
    std::vector<int16_t> seen;
    a.for_each([&](kafka::api_key key, int&) { seen.push_back(key()); });
    EXPECT_EQ(seen, (std::vector<int16_t>{0, 1, 2, 3, 100, 101}));
}

TEST(ApiKeyIndexedArray, equality) {
    test_array a;
    test_array b;
    EXPECT_EQ(a, b);
    b[k(100)] = 1;
    EXPECT_NE(a, b);
}

TEST(ApiKeyIndexedArray, sizes_and_base) {
    EXPECT_EQ(test_array::standard_size(), 4u);
    EXPECT_EQ(test_array::reserved_size(), 2u);
    EXPECT_EQ(test_array::reserved_base(), k(100));
}

// Proves operator[] works under constant evaluation in both regions.
constexpr int constexpr_sum() {
    kafka::api_key_indexed_array<int, 4, 2, 100> a;
    a[kafka::api_key(0)] = 5;
    a[kafka::api_key(101)] = 9;
    return a[kafka::api_key(0)] + a[kafka::api_key(101)];
}
static_assert(constexpr_sum() == 14);

// Proves the container holds a non-movable, non-copyable element type
// (mirrors handler_probe).
struct non_movable {
    non_movable() = default;
    non_movable(const non_movable&) = delete;
    non_movable& operator=(const non_movable&) = delete;
    non_movable(non_movable&&) = delete;
    non_movable& operator=(non_movable&&) = delete;
    int value{0};
};

TEST(ApiKeyIndexedArray, holds_non_movable_type) {
    kafka::api_key_indexed_array<non_movable, 4, 2, 100> a;
    a[k(2)].value = 42;
    a[k(100)].value = 7;
    EXPECT_EQ(a[k(2)].value, 42);
    EXPECT_EQ(a[k(100)].value, 7);

    int total = 0;
    a.for_each([&](kafka::api_key, non_movable& n) { total += n.value; });
    EXPECT_EQ(total, 49);
}

} // namespace
