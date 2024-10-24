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
#include "base/seastarx.h"
#include "container/interval_set.h"
#include "random/generators.h"

#include <seastar/core/sstring.hh>

#include <gtest/gtest.h>

#include <random>

using set_t = interval_set<uint64_t>;

namespace {

// Iterates through the set, ensuring that they are monotonically increasing
// and that there are no overlapping intervals.
void check_no_overlap(const set_t& s) {
    std::optional<uint64_t> prev_last;
    for (const auto& interval : s) {
        auto start = interval.first;
        auto end = interval.second;
        EXPECT_GT(end, start);
        if (prev_last) {
            EXPECT_GT(start, prev_last.value());
        }
        prev_last = end - 1;
    }
}

} // anonymous namespace

TEST(IntervalSet, InsertZeroLength) {
    set_t set;
    for (unsigned int i = 0; i < 10; ++i) {
        const auto res = set.insert({i, 0});
        EXPECT_EQ(res, std::make_pair(set.end(), false));
    }
}

TEST(IntervalSet, InsertWhenEmpty) {
    for (unsigned int i = 0; i < 10; ++i) {
        set_t set;
        const auto res = set.insert({i, 10});
        EXPECT_NE(res.first, set.end());
        EXPECT_TRUE(res.second);
    }
}

TEST(IntervalSet, InsertMergeOverlappingIntervals) {
    for (unsigned int i = 0; i < 10; ++i) {
        set_t set;
        // [0, 10)
        //    [i, i + 10)
        const auto ret1 = set.insert({0, 10});
        EXPECT_TRUE(ret1.second);
        EXPECT_EQ(ret1.first->first, 0);
        EXPECT_EQ(ret1.first->second, 10);

        // Insertion of a sub-interval results in the first interval being
        // extended. Note that because of this, the iterators are safe (i.e.
        // there is only one element in the underlying container).
        const auto ret2 = set.insert({i, 10});
        auto expected_end = 10 + i;
        EXPECT_TRUE(ret2.second);
        EXPECT_EQ(ret1, ret2);
        EXPECT_EQ(ret1.first->first, 0);
        EXPECT_EQ(ret1.first->second, expected_end);

        // Confirm we can still find the expanded intervals.
        const auto found_iter_first = set.find(0);
        EXPECT_EQ(ret1.first, found_iter_first);

        const auto found_iter_last = set.find(expected_end - 1);
        EXPECT_EQ(ret1.first, found_iter_last);

        const auto found_iter_end = set.find(expected_end);
        EXPECT_EQ(set.end(), found_iter_end);
        check_no_overlap(set);
    }
}

TEST(IntervalSet, InsertMergesAdjacentIntervals) {
    set_t set;
    // [0, 10) [10, 20)
    const auto ret1 = set.insert({0, 10});
    EXPECT_TRUE(ret1.second);
    const auto ret2 = set.insert({10, 10});
    EXPECT_TRUE(ret2.second);

    // Since the intervals were exactly adjacent, they should be merged.
    EXPECT_EQ(ret1, ret2);
    EXPECT_EQ(ret1.first->first, 0);
    EXPECT_EQ(ret1.first->second, 20);
    check_no_overlap(set);
}

TEST(IntervalSet, InsertWithGap) {
    set_t set;
    // [0, 10) [11, 21): a gap at [10, 11).
    EXPECT_TRUE(set.insert({0, 10}).second);
    EXPECT_TRUE(set.insert({11, 10}).second);
    EXPECT_EQ(2, set.size());

    // We can't find the gap.
    auto found = set.find(10);
    EXPECT_EQ(set.end(), found);

    // But the surrounding intervals we can find.
    for (unsigned int i = 0; i < 10; i++) {
        auto found = set.find(i);
        EXPECT_NE(set.end(), found);
    }
    for (unsigned int i = 0; i < 10; i++) {
        auto found = set.find(11 + i);
        EXPECT_NE(set.end(), found);
    }
    check_no_overlap(set);
}

TEST(IntervalSet, InsertOverlapFront) {
    set_t set;

    // Overlap such that the beginning of the intervals overlap.
    auto ret1 = set.insert({0, 10});
    EXPECT_TRUE(ret1.second);
    auto ret2 = set.insert({0, 100});
    EXPECT_TRUE(ret2.second);
    auto ret3 = set.insert({0, 10});
    EXPECT_TRUE(ret3.second);
    EXPECT_EQ(ret1, ret2);
    EXPECT_EQ(ret1, ret3);
    EXPECT_EQ(ret1.first->first, 0);
    EXPECT_EQ(ret1.first->second, 100);
    EXPECT_EQ(1, set.size());

    check_no_overlap(set);
}

TEST(IntervalSet, InsertOverlapCompletely) {
    set_t set;
    EXPECT_TRUE(set.insert({10, 20}).second);
    EXPECT_TRUE(set.insert({0, 100}).second);
    EXPECT_EQ(set.begin()->first, 0);
    EXPECT_EQ(set.begin()->second, 100);
    check_no_overlap(set);
}

TEST(IntervalSet, InsertFillGap) {
    set_t set;
    // [0, 10) [11, 21)
    EXPECT_TRUE(set.insert({0, 10}).second);
    EXPECT_TRUE(set.insert({11, 10}).second);

    // Fill the gap and ensure we can find through the entire interval.
    EXPECT_TRUE(set.insert({10, 1}).second);
    for (unsigned int i = 0; i < 21; i++) {
        auto found = set.find(i);
        EXPECT_NE(set.end(), found);
    }
    check_no_overlap(set);
}

TEST(IntervalSet, InsertOverlapPastFront) {
    set_t set;
    // Insert an interval, and then insert again before the start of that
    // interval in a way that overlaps.
    // [10, 20)[0, 11)
    EXPECT_TRUE(set.insert({10, 10}).second);
    EXPECT_TRUE(set.insert({0, 11}).second);
    for (unsigned int i = 0; i < 20; i++) {
        auto found = set.find(i);
        EXPECT_NE(set.end(), found);
    }
    check_no_overlap(set);
}

TEST(IntervalSet, FindEmpty) {
    const set_t set;
    EXPECT_EQ(set.find(0), set.end());
}

TEST(IntervalSet, FindPastLast) {
    set_t set;
    const auto res = set.insert({0, 10});
    EXPECT_EQ(set.find(8), res.first);
    EXPECT_EQ(set.find(9), res.first);
    EXPECT_EQ(set.find(10), set.end());
    EXPECT_EQ(set.find(11), set.end());
}

TEST(IntervalSet, FindBeforeFirst) {
    set_t set;
    EXPECT_TRUE(set.insert({2, 10}).second);
    EXPECT_EQ(set.find(0), set.end());
    EXPECT_EQ(set.find(1), set.end());
}

TEST(IntervalSet, BeginEndEmpty) {
    const set_t set;
    EXPECT_EQ(set.begin(), set.end());
}

TEST(IntervalSet, Erase) {
    set_t set;
    auto res = set.insert({0, 10});
    EXPECT_FALSE(set.empty());
    EXPECT_EQ(1, set.size());
    set.erase(res.first);
    EXPECT_TRUE(set.empty());
}

TEST(IntervalSet, EraseMerged) {
    set_t set;
    EXPECT_TRUE(set.insert({0, 10}).second);
    EXPECT_TRUE(set.insert({20, 10}).second);
    EXPECT_TRUE(set.insert({10, 10}).second);
    EXPECT_FALSE(set.empty());
    EXPECT_EQ(1, set.size());
    auto found = set.find(0);
    EXPECT_NE(found, set.end());
    set.erase(found);
    EXPECT_EQ(0, set.size());
    EXPECT_TRUE(set.empty());
}

TEST(IntervalSet, EraseBeginToEnd) {
    set_t set;
    EXPECT_TRUE(set.insert({0, 10}).second);
    EXPECT_TRUE(set.insert({20, 10}).second);
    EXPECT_TRUE(set.insert({40, 10}).second);
    EXPECT_EQ(3, set.size());
    EXPECT_TRUE(set.begin()->first == 0);
    EXPECT_TRUE(set.begin()->second == 10);

    set.erase(set.begin());
    EXPECT_EQ(set.begin()->first, 20);
    EXPECT_EQ(set.begin()->second, 30);

    set.erase(set.begin());
    EXPECT_EQ(set.begin()->first, 40);
    EXPECT_EQ(set.begin()->second, 50);

    set.erase(set.begin());
    EXPECT_EQ(set.begin(), set.end());
    EXPECT_TRUE(set.empty());
}

struct randomized_test_params {
    size_t interval_max_size;
    double target_fill_percent;
};
class RandomizedIntervalSetTest
  : public ::testing::TestWithParam<randomized_test_params> {};

// Test that generates an interval set randomly, filling the corresponding
// intervals in a buffer with 'X's. It then iterates through the intervals in
// the set, ensuring that the interval_set actually refers to just the 'X's.
TEST_P(RandomizedIntervalSetTest, RandomInsertsSequentialErase) {
    const auto params = GetParam();
    const auto interval_max_size = params.interval_max_size;
    const double target_fill_percent = params.target_fill_percent;
    const size_t buf_size = 5000;

    const size_t target_fill_size = target_fill_percent * buf_size;

    ss::sstring buf(buf_size, 'O');
    auto current_filled = [&buf] {
        size_t count = 0;
        for (char c : buf) {
            if (c == 'X') {
                count++;
            }
        }
        return count;
    };
    // Come up with a series of intervals until the target is filled.
    set_t filled_intervals;
    while (current_filled() < target_fill_size) {
        const uint64_t rand_size = random_generators::get_int(
          1, static_cast<int>(interval_max_size));
        const int start_max = buf_size - rand_size;
        const uint64_t rand_start = random_generators::get_int(0, start_max);
        EXPECT_TRUE(filled_intervals.insert({rand_start, rand_size}).second);
        for (size_t i = 0; i < rand_size; i++) {
            buf[rand_start + i] = 'X';
        }
    }
    auto exact_filled_size = current_filled();
    ASSERT_FALSE(filled_intervals.empty());
    check_no_overlap(filled_intervals);

    // Iterate through our intervals, validating that they refer to the filled
    // intervals.
    size_t interval_filled_size = 0;
    std::optional<uint64_t> highest_so_far;
    while (!filled_intervals.empty()) {
        auto it = filled_intervals.begin();
        if (highest_so_far) {
            EXPECT_GT(it->first, *highest_so_far);
        }
        highest_so_far = it->second;
        interval_filled_size += it->second - it->first;
        for (auto i = it->first; i < it->second; i++) {
            EXPECT_EQ(buf[i], 'X');
        }
        filled_intervals.erase(it);
        check_no_overlap(filled_intervals);
    }
    ASSERT_EQ(interval_filled_size, exact_filled_size);
}

INSTANTIATE_TEST_SUITE_P(
  RandomizedFilledIntervals,
  RandomizedIntervalSetTest,
  ::testing::Values(
    // Moderately filled.
    randomized_test_params{
      .interval_max_size = 100,
      .target_fill_percent = 0.5,
    },
    // Mostly filled.
    randomized_test_params{
      .interval_max_size = 100,
      .target_fill_percent = 0.9,
    },
    // Completely filled.
    randomized_test_params{
      .interval_max_size = 100,
      .target_fill_percent = 1.0,
    },
    // Low fill.
    randomized_test_params{
      .interval_max_size = 2,
      .target_fill_percent = 0.1,
    }));
