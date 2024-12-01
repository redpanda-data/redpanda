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

#include "gmock/gmock.h"
#include "serde/parquet/column_stats_collector.h"
#include "serde/parquet/value.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <optional>

using namespace serde::parquet;
using testing::DoubleEq;
using testing::Field;
using testing::Optional;

namespace {
MATCHER(IsNegativeZero, "") { return arg == 0.0 && std::signbit(arg); }
MATCHER(IsPositiveZero, "") { return arg == 0.0 && !std::signbit(arg); }
auto DoubleBound(auto m) {
    return Optional(Field("val", &float64_value::val, m));
}
} // namespace

// NOLINTBEGIN(*magic-number*)
TEST(ColumnStatsCollector, FloatingPoint) {
    column_stats_collector<float64_value, ordering::float64> collector;
    collector.record_value(float64_value{NAN});
    EXPECT_EQ(collector.min(), std::nullopt);
    EXPECT_EQ(collector.max(), std::nullopt);
    collector.record_value(float64_value{3.14});
    collector.record_value(float64_value{1});
    EXPECT_THAT(collector.min(), DoubleBound(DoubleEq(1)));
    EXPECT_THAT(collector.max(), DoubleBound(DoubleEq(3.14)));
    collector.record_value(float64_value{+0.0});
    collector.record_value(float64_value{NAN});
    collector.record_value(
      float64_value{-std::numeric_limits<double>::infinity()});
    collector.record_value(
      float64_value{std::numeric_limits<double>::infinity()});
    EXPECT_THAT(
      collector.min(),
      DoubleBound(DoubleEq(-std::numeric_limits<double>::infinity())));
    EXPECT_THAT(
      collector.max(),
      DoubleBound(DoubleEq(std::numeric_limits<double>::infinity())));
    collector.reset();
    collector.record_value(float64_value{+0.0});
    EXPECT_THAT(collector.min(), DoubleBound(IsNegativeZero()));
    EXPECT_THAT(collector.max(), DoubleBound(IsPositiveZero()));
    collector.reset();
    collector.record_value(float64_value{-0.0});
    EXPECT_THAT(collector.min(), DoubleBound(IsNegativeZero()));
    EXPECT_THAT(collector.max(), DoubleBound(IsPositiveZero()));
}

TEST(ColumnStatsCollector, Binary) {
    column_stats_collector<byte_array_value, ordering::byte_array> collector;
    auto empty = byte_array_value{iobuf::from("")};
    auto bat = byte_array_value{iobuf::from("bat")};
    auto cat = byte_array_value{iobuf::from("cat")};
    auto zzzz = byte_array_value{iobuf::from("zzzz")};
    collector.record_value(bat);
    EXPECT_THAT(collector.min(), Optional(std::ref(bat)));
    EXPECT_THAT(collector.max(), Optional(std::ref(bat)));
    collector.record_value(cat);
    EXPECT_THAT(collector.min(), Optional(std::ref(bat)));
    EXPECT_THAT(collector.max(), Optional(std::ref(cat)));
    collector.record_value(empty);
    collector.record_value(zzzz);
    EXPECT_THAT(collector.min(), Optional(std::ref(empty)));
    EXPECT_THAT(collector.max(), Optional(std::ref(zzzz)));
}

TEST(ColumnStatsCollector, Decimal128) {
    column_stats_collector<fixed_byte_array_value, ordering::int128_be>
      collector;
    auto negative_five = fixed_byte_array_value{iobuf::from(
      {"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFE",
       16})};
    auto one = fixed_byte_array_value{
      iobuf::from({"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\1", 16})};
    auto fourty_two = fixed_byte_array_value{
      iobuf::from({"\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\x2A", 16})};
    collector.record_value(one);
    EXPECT_THAT(collector.min(), Optional(std::ref(one)));
    EXPECT_THAT(collector.max(), Optional(std::ref(one)));
    collector.record_value(fourty_two);
    EXPECT_THAT(collector.min(), Optional(std::ref(one)));
    EXPECT_THAT(collector.max(), Optional(std::ref(fourty_two)));
    collector.record_value(negative_five);
    EXPECT_THAT(collector.min(), Optional(std::ref(negative_five)));
    EXPECT_THAT(collector.max(), Optional(std::ref(fourty_two)));
}

TEST(ColumnStatsCollector, Signed) {
    column_stats_collector<int32_value, ordering::int32> collector;

    collector.record_value({-5});
    collector.record_value({1});
    collector.record_value({42});

    EXPECT_THAT(collector.min(), Optional(int32_value{-5}));
    EXPECT_THAT(collector.max(), Optional(int32_value{42}));
}

TEST(ColumnStatsCollector, Unsigned) {
    column_stats_collector<int32_value, ordering::uint32> collector;

    collector.record_value({-5});
    collector.record_value({1});
    collector.record_value({42});

    EXPECT_THAT(collector.min(), Optional(int32_value{1}));
    EXPECT_THAT(collector.max(), Optional(int32_value{-5}));
}

TEST(ColumnStatsCollector, NullCount) {
    column_stats_collector<boolean_value, ordering::boolean> collector;

    EXPECT_EQ(collector.null_count(), 0);

    collector.record_null();
    EXPECT_EQ(collector.null_count(), 1);

    collector.record_null();
    EXPECT_EQ(collector.null_count(), 2);

    collector.record_null();
    EXPECT_EQ(collector.null_count(), 3);
    EXPECT_EQ(collector.min(), std::nullopt);
    EXPECT_EQ(collector.max(), std::nullopt);
}

TEST(ColumnStatsCollector, Merge) {
    column_stats_collector<int64_value, ordering::int64> collector_a;
    collector_a.record_value({-5});
    collector_a.record_value({1});
    collector_a.record_null();
    collector_a.record_value({42});

    EXPECT_THAT(collector_a.null_count(), 1);
    EXPECT_THAT(collector_a.min(), Optional(int64_value{-5}));
    EXPECT_THAT(collector_a.max(), Optional(int64_value{42}));

    column_stats_collector<int64_value, ordering::int64> collector_b;
    collector_b.record_value({-2});
    collector_b.record_value({2});
    collector_b.record_null();
    collector_b.record_value({99});
    collector_a.merge(collector_b);

    EXPECT_THAT(collector_b.null_count(), 1);
    EXPECT_THAT(collector_b.min(), Optional(int64_value{-2}));
    EXPECT_THAT(collector_b.max(), Optional(int64_value{99}));

    EXPECT_THAT(collector_a.null_count(), 2);
    EXPECT_THAT(collector_a.min(), Optional(int64_value{-5}));
    EXPECT_THAT(collector_a.max(), Optional(int64_value{99}));

    column_stats_collector<int64_value, ordering::int64> collector_c;
    collector_c.record_value({-99});
    collector_c.record_null();
    collector_c.record_null();
    collector_c.record_null();
    collector_c.record_value({64});
    collector_a.merge(collector_c);

    EXPECT_THAT(collector_c.null_count(), 3);
    EXPECT_THAT(collector_c.min(), Optional(int64_value{-99}));
    EXPECT_THAT(collector_c.max(), Optional(int64_value{64}));

    EXPECT_THAT(collector_a.null_count(), 5);
    EXPECT_THAT(collector_a.min(), Optional(int64_value{-99}));
    EXPECT_THAT(collector_a.max(), Optional(int64_value{99}));
}

// NOLINTEND(*magic-number*)
