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

#include "utils/bounds.h"

#include <gtest/gtest.h>

#include <limits>

namespace {

template<std::integral I>
struct integral_range {
    I min = std::numeric_limits<I>::min();
    I max = std::numeric_limits<I>::max();
};
template<std::integral I>
std::ostream& operator<<(std::ostream& os, const integral_range<I> ir) {
    return os << "[" << ir.min << ", " << ir.max << "]";
}

template<std::integral To, std::integral From>
testing::AssertionResult within_limits(From i) {
    const integral_range<To> rng;
    const auto b = bounds::within_limits<To>(i);
    if (b) {
        return testing::AssertionSuccess()
               << "value " << i << " reported within range " << rng;
    } else {
        return testing::AssertionFailure()
               << "value " << i << " reported out of range " << rng;
    }
}

template<std::integral To, std::integral From>
testing::AssertionResult outside_limits(From i) {
    const integral_range<To> rng;
    const auto b = bounds::outside_limits<To>(i);
    if (b) {
        return testing::AssertionSuccess()
               << "value " << i << " reported out of range " << rng;
    } else {
        return testing::AssertionFailure()
               << "value " << i << " reported within range " << rng;
    }
}
using ushort = unsigned short;
using uint = unsigned int;
} // namespace

TEST(bounds_check, signed_to_signed) {
    const integral_range<short> shorts{};
    const integral_range<int> ints{};
    const std::vector<int> within_bounds{shorts.min, -1, 0, 1, shorts.max};
    for (const int i : within_bounds) {
        EXPECT_TRUE(::within_limits<short>(i));
        EXPECT_FALSE(::outside_limits<short>(i));
    }
    const std::vector<int> out_of_bounds{
      ints.min, shorts.min - 1, shorts.max + 1, ints.max};
    for (const int i : out_of_bounds) {
        EXPECT_FALSE(::within_limits<short>(i));
        EXPECT_TRUE(::outside_limits<short>(i));
    }
}

TEST(bounds_check, unsigned_to_unsigned) {
    const integral_range<ushort> ushorts{};
    const integral_range<uint> uints{};
    const std::vector<uint> within_bounds{0, 1, ushorts.max};
    for (const uint i : within_bounds) {
        EXPECT_TRUE(::within_limits<ushort>(i));
        EXPECT_FALSE(::outside_limits<ushort>(i));
    }
    const std::vector<uint> out_of_bounds{
      static_cast<uint>(ushorts.max + 1), uints.max};
    for (const uint i : out_of_bounds) {
        EXPECT_FALSE(::within_limits<ushort>(i));
        EXPECT_TRUE(::outside_limits<ushort>(i));
    }
}

TEST(bounds_check, unsigned_to_signed) {
    const integral_range<short> shorts{};
    const integral_range<uint> uints{};
    const std::vector<uint> within_bounds{0, 1, static_cast<uint>(shorts.max)};
    for (const uint i : within_bounds) {
        EXPECT_TRUE(::within_limits<short>(i));
        EXPECT_FALSE(::outside_limits<short>(i));
    }
    const std::vector<uint> out_of_bounds{
      static_cast<uint>(shorts.max + 1), uints.max};
    for (const uint i : out_of_bounds) {
        EXPECT_FALSE(::within_limits<short>(i));
        EXPECT_TRUE(::outside_limits<short>(i));
    }
}

TEST(bounds_check, signed_to_unsigned) {
    const integral_range<ushort> ushorts{};
    const integral_range<int> ints{};
    const std::vector<int> within_bounds{0, 1, ushorts.max};
    for (const int i : within_bounds) {
        EXPECT_TRUE(::within_limits<ushort>(i));
        EXPECT_FALSE(::outside_limits<ushort>(i));
    }
    const std::vector<int> out_of_bounds{
      ints.min, -1, ushorts.max + 1, ints.max};
    for (const int i : out_of_bounds) {
        EXPECT_FALSE(::within_limits<ushort>(i));
        EXPECT_TRUE(::outside_limits<ushort>(i));
    }
}
