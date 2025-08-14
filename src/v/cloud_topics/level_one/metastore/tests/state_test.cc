/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "bytes/iobuf.h"
#include "cloud_topics/level_one/metastore/state.h"

#include <gtest/gtest.h>

using namespace experimental::cloud_topics::l1;
using o = kafka::offset;
using ts = model::timestamp;

// Simple test that makes sure we can build serde serialization.
TEST(StateTest, TestSerde) {
    state s;
    iobuf b = serde::to_iobuf(s.copy());
    auto roundtrip_s = serde::from_iobuf<state>(std::move(b));
    ASSERT_TRUE(s == roundtrip_s);
}

TEST(CompactionStateTest, TestContiguousTombstoneRangeEmpty) {
    compaction_state s;
    s.cleaned_ranges_with_tombstones = {};
    ASSERT_FALSE(s.has_contiguous_range_with_tombstones(o{0}, o{10}));
}

TEST(CompactionStateTest, TestContiguousTombstoneRangeSingle) {
    compaction_state s;
    s.cleaned_ranges_with_tombstones = {
      {.base_offset = o{0},
       .last_offset = o{99},
       .cleaned_with_tombstones_at = ts{1000}},
    };
    ASSERT_TRUE(s.has_contiguous_range_with_tombstones(o{0}, o{99}));

    // Remove from the beginning.
    ASSERT_TRUE(s.has_contiguous_range_with_tombstones(o{0}, o{10}));
    ASSERT_TRUE(s.erase_contiguous_range_with_tombstones(o{0}, o{10}));
    {
        compaction_state::tombstone_range_set_t expected = {
          {.base_offset = o{11},
           .last_offset = o{99},
           .cleaned_with_tombstones_at = ts{1000}},
        };
        EXPECT_EQ(s.cleaned_ranges_with_tombstones, expected);
    }
    ASSERT_TRUE(s.has_contiguous_range_with_tombstones(o{11}, o{99}));

    // Remove from the end.
    ASSERT_TRUE(s.has_contiguous_range_with_tombstones(o{89}, o{99}));
    ASSERT_TRUE(s.erase_contiguous_range_with_tombstones(o{89}, o{99}));
    {
        compaction_state::tombstone_range_set_t expected = {
          {.base_offset = o{11},
           .last_offset = o{88},
           .cleaned_with_tombstones_at = ts{1000}},
        };
        EXPECT_EQ(s.cleaned_ranges_with_tombstones, expected);
    }
    ASSERT_TRUE(s.has_contiguous_range_with_tombstones(o{11}, o{88}));

    // Remove in the middle, creating two ranges.
    ASSERT_TRUE(s.has_contiguous_range_with_tombstones(o{12}, o{87}));
    ASSERT_TRUE(s.erase_contiguous_range_with_tombstones(o{12}, o{87}));
    {
        compaction_state::tombstone_range_set_t expected = {
          {.base_offset = o{11},
           .last_offset = o{11},
           .cleaned_with_tombstones_at = ts{1000}},
          {.base_offset = o{88},
           .last_offset = o{88},
           .cleaned_with_tombstones_at = ts{1000}},
        };
        EXPECT_EQ(s.cleaned_ranges_with_tombstones, expected);
    }
    ASSERT_TRUE(s.has_contiguous_range_with_tombstones(o{11}, o{11}));

    // Remove an entire range.
    ASSERT_TRUE(s.has_contiguous_range_with_tombstones(o{88}, o{88}));
    ASSERT_TRUE(s.erase_contiguous_range_with_tombstones(o{88}, o{88}));
    {
        compaction_state::tombstone_range_set_t expected = {
          {.base_offset = o{11},
           .last_offset = o{11},
           .cleaned_with_tombstones_at = ts{1000}},
        };
        EXPECT_EQ(s.cleaned_ranges_with_tombstones, expected);
    }

    // Remove the last range.
    ASSERT_TRUE(s.has_contiguous_range_with_tombstones(o{11}, o{11}));
    ASSERT_TRUE(s.erase_contiguous_range_with_tombstones(o{11}, o{11}));
    EXPECT_EQ(
      s.cleaned_ranges_with_tombstones,
      compaction_state::tombstone_range_set_t{});
}

TEST(CompactionStateTest, TestContiguousTombstoneRangeTwo) {
    compaction_state s;
    s.cleaned_ranges_with_tombstones = {
      {.base_offset = o{0},
       .last_offset = o{49},
       .cleaned_with_tombstones_at = ts{1000}},
      {.base_offset = o{50},
       .last_offset = o{99},
       .cleaned_with_tombstones_at = ts{2000}},
    };
    ASSERT_TRUE(s.has_contiguous_range_with_tombstones(o{0}, o{99}));

    // Remove a single offset.
    ASSERT_TRUE(s.erase_contiguous_range_with_tombstones(o{10}, o{10}));
    {
        compaction_state::tombstone_range_set_t expected = {
          {.base_offset = o{0},
           .last_offset = o{9},
           .cleaned_with_tombstones_at = ts{1000}},
          {.base_offset = o{11},
           .last_offset = o{49},
           .cleaned_with_tombstones_at = ts{1000}},
          {.base_offset = o{50},
           .last_offset = o{99},
           .cleaned_with_tombstones_at = ts{2000}},
        };
        EXPECT_EQ(s.cleaned_ranges_with_tombstones, expected);
    }
    ASSERT_TRUE(s.has_contiguous_range_with_tombstones(o{0}, o{9}));
    ASSERT_TRUE(s.has_contiguous_range_with_tombstones(o{11}, o{99}));

    // Remove a range that spans two ranges.
    ASSERT_TRUE(s.erase_contiguous_range_with_tombstones(o{11}, o{89}));
    {
        compaction_state::tombstone_range_set_t expected = {
          {.base_offset = o{0},
           .last_offset = o{9},
           .cleaned_with_tombstones_at = ts{1000}},
          {.base_offset = o{90},
           .last_offset = o{99},
           .cleaned_with_tombstones_at = ts{2000}},
        };
        EXPECT_EQ(s.cleaned_ranges_with_tombstones, expected);
    }
    ASSERT_TRUE(s.has_contiguous_range_with_tombstones(o{0}, o{9}));
    ASSERT_TRUE(s.has_contiguous_range_with_tombstones(o{90}, o{99}));
}

TEST(CompactionStateTest, TestContiguousTombstoneRangesBaseTooLow) {
    compaction_state s;
    s.cleaned_ranges_with_tombstones = {
      {.base_offset = o{5},
       .last_offset = o{10},
       .cleaned_with_tombstones_at = ts{2000}},
    };
    // Look for ranges that start below the bottom. Even if there is overlap
    // with the target range, it won't be fully covered and should return
    // false.
    for (int base = 0; base < 5; ++base) {
        for (int last = base; last < 15; ++last) {
            EXPECT_FALSE(
              s.has_contiguous_range_with_tombstones(o{base}, o{last}));
        }
    }
}

TEST(CompactionStateTest, TestContiguousTombstoneRangesLastTooHigh) {
    compaction_state s;
    s.cleaned_ranges_with_tombstones = {
      {.base_offset = o{5},
       .last_offset = o{10},
       .cleaned_with_tombstones_at = ts{2000}},
    };
    // Look for ranges that end above the top. Even if there is overlap with
    // the target range, it won't be fully covered and should return false.
    for (int base = 5; base <= 10; ++base) {
        for (int last = 11; last < 15; ++last) {
            EXPECT_FALSE(
              s.has_contiguous_range_with_tombstones(o{base}, o{last}));
        }
    }
}

TEST(CompactionStateTest, TestContiguousTombstoneRangesNotContiguous) {
    compaction_state s;
    s.cleaned_ranges_with_tombstones = {
      {.base_offset = o{5},
       .last_offset = o{10},
       .cleaned_with_tombstones_at = ts{2000}},
      {.base_offset = o{12},
       .last_offset = o{20},
       .cleaned_with_tombstones_at = ts{2000}},
    };
    // Look for ranges that span a gap.
    for (int base = 0; base <= 10; ++base) {
        for (int last = 12; last < 25; ++last) {
            EXPECT_FALSE(
              s.has_contiguous_range_with_tombstones(o{base}, o{last}));
        }
    }
}

TEST(CompactionStateTest, TestAddContiguousTombstoneRanges) {
    auto range = [](int base, int last) {
        return compaction_state::cleaned_range_with_tombstones{
          .base_offset = o{base},
          .last_offset = o{last},
          .cleaned_with_tombstones_at = ts{1000},
        };
    };
    compaction_state s;
    ASSERT_TRUE(s.may_add(range(10, 20)));
    ASSERT_TRUE(s.add(range(10, 20)));

    ASSERT_TRUE(s.may_add(range(0, 9)));
    ASSERT_TRUE(s.may_add(range(21, 30)));

    // Overlap at the edges.
    ASSERT_FALSE(s.may_add(range(0, 10)));
    ASSERT_FALSE(s.may_add(range(20, 25)));

    // Partial overlap.
    ASSERT_FALSE(s.may_add(range(5, 15)));
    ASSERT_FALSE(s.may_add(range(15, 25)));
    ASSERT_FALSE(s.may_add(range(11, 19)));

    // Full overlap.
    ASSERT_FALSE(s.may_add(range(10, 20)));
    ASSERT_FALSE(s.may_add(range(5, 25)));

    // Add another range.
    ASSERT_TRUE(s.may_add(range(30, 40)));
    ASSERT_TRUE(s.add(range(30, 40)));

    ASSERT_TRUE(s.may_add(range(21, 29)));
    ASSERT_TRUE(s.may_add(range(41, 45)));

    // Overlap at the edges.
    ASSERT_FALSE(s.may_add(range(0, 10)));
    ASSERT_FALSE(s.may_add(range(20, 30)));
    ASSERT_FALSE(s.may_add(range(40, 45)));

    // Partial overlap.
    ASSERT_FALSE(s.may_add(range(15, 25)));
    ASSERT_FALSE(s.may_add(range(25, 35)));

    // Fill the gap.
    ASSERT_TRUE(s.may_add(range(21, 29)));
    ASSERT_TRUE(s.add(range(21, 29)));

    // Exhaustively check. At this point, the range contains [10, 40].
    for (int base = 9; base < 10; ++base) {
        for (int last = 10; last <= 40; ++last) {
            ASSERT_FALSE(s.may_add(range(base, last)));
        }
    }
    for (int base = 10; base <= 40; ++base) {
        for (int last = base; last <= 40; ++last) {
            ASSERT_FALSE(s.may_add(range(base, last)));
        }
    }
}
