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
#include "strings/utf8.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cmath>
#include <compare>
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

TEST(NoopTrunactor, Basic) {
    serde::parquet::internal::noop_bound_truncator t{0};
    iobuf b;
    EXPECT_EQ(t.get_max_bound(b), std::nullopt);
    EXPECT_EQ(t.get_min_bound(b), std::nullopt);
}

namespace {
bool is_utf8(iobuf& b, size_t bound_size) {
    auto begin = iobuf::byte_iterator(b.cbegin(), b.cend());
    auto end = iobuf::byte_iterator(b.cend(), b.cend());
    return utf::is_valid_utf8(begin, end, bound_size);
}
} // namespace

TEST(BinaryTrunactor, Bytes) {
    struct test_params {
        size_t max_bound_size;
        bool is_min_bound;
        std::vector<uint8_t> bound;
    };

    std::vector<std::pair<test_params, std::optional<std::vector<uint8_t>>>>
      tests{
        {{.max_bound_size = 2,
          .is_min_bound = true,
          .bound = {0xFF, 0xFF, 0xFF}},
         {{0xFF, 0xFF}}},
        {{.max_bound_size = 2,
          .is_min_bound = false,
          .bound = {0xFF, 0xFF, 0xFF}},
         {}},
        {{.max_bound_size = 2,
          .is_min_bound = false,
          .bound = {0xFF, 0x1, 0xFF}},
         {{0xFF, 0x2}}},
        {{.max_bound_size = 2,
          .is_min_bound = false,
          .bound = {0xFE, 0xFF, 0xFF}},
         {{0xFF, 0x0}}},
      };

    for (const auto& [params, expec] : tests) {
        iobuf buf;
        buf.append(&params.bound.front(), params.bound.size());
        auto param_bound_str = buf.linearize_to_string();
        auto res = expec.transform([](auto& e) {
            iobuf b;
            b.append(&e.front(), e.size());
            return b;
        });

        EXPECT_FALSE(is_utf8(buf, params.max_bound_size));

        serde::parquet::internal::binary_bound_truncator t{
          params.max_bound_size};
        std::optional<iobuf> bound;
        if (params.is_min_bound) {
            bound = t.get_min_bound(buf);
        } else {
            bound = t.get_max_bound(buf);
        }
        EXPECT_EQ(bound, res);

        // Ensure the original buffer remains un-modified.
        EXPECT_EQ(buf.linearize_to_string(), param_bound_str);

        if (bound) {
            if (params.is_min_bound) {
                EXPECT_LE(
                  bound->linearize_to_string(), buf.linearize_to_string());
            } else {
                EXPECT_GE(
                  bound->linearize_to_string(), buf.linearize_to_string());
            }
        }
    }
}

TEST(BinaryTrunactor, MinMax) {
    auto b = iobuf::from(
      "vkNOQZeDacDujKTSpi3tqFjam5Q7I0PaBS8uXvMeSYsNm8Q2yegdvbTOkjzo2bRSGDSSMjBJ"
      "esftbKb7RmIjMh");
    auto b_min
      = serde::parquet::internal::binary_bound_truncator{64}.get_min_bound(b);
    auto b_max
      = serde::parquet::internal::binary_bound_truncator{64}.get_max_bound(b);
    EXPECT_TRUE((b_min <=> b_max) == std::strong_ordering::less);
}

TEST(BinaryTrunactor, UTF8) {
    struct test_params {
        size_t max_bound_size;
        bool is_min_bound;
        std::string bound;
    };

    const auto max_code_point = utf::utf32_code_point{0x10FFFF};
    const std::string max_code_point_s = {
      max_code_point.utf8_encoding().data(),
      max_code_point.utf8_encoding_length()};

    std::vector<std::pair<test_params, std::optional<std::string>>> tests{
      {{.max_bound_size = 2, .is_min_bound = true, .bound = ""}, {}},
      {{.max_bound_size = 2, .is_min_bound = false, .bound = ""}, {}},
      {{.max_bound_size = 2, .is_min_bound = true, .bound = "hello"}, {"he"}},
      {{.max_bound_size = 2, .is_min_bound = false, .bound = "hello"}, {"hf"}},
      {{.max_bound_size = 4, .is_min_bound = false, .bound = "hello"},
       {"helm"}},
      {{.max_bound_size = 5, .is_min_bound = false, .bound = "hello"}, {}},
      {{.max_bound_size = 5, .is_min_bound = true, .bound = "hello"}, {}},
      {{.max_bound_size = 2, .is_min_bound = true, .bound = max_code_point_s},
       {}},
      {{.max_bound_size = 8,
        .is_min_bound = false,
        .bound = max_code_point_s + max_code_point_s + max_code_point_s},
       {}},
      {{.max_bound_size = 8,
        .is_min_bound = true,
        .bound = max_code_point_s + max_code_point_s + max_code_point_s},
       {max_code_point_s + max_code_point_s}},
      {{.max_bound_size = 2,
        .is_min_bound = false,
        .bound = "h" + max_code_point_s + max_code_point_s},
       {"i"}},
      {{.max_bound_size = 4,
        .is_min_bound = false,
        .bound = "h" + max_code_point_s + max_code_point_s},
       {"i"}},
      {{.max_bound_size = 5,
        .is_min_bound = false,
        .bound = "h" + max_code_point_s + max_code_point_s},
       {std::string{"i"} + '\0'}},
    };

    for (auto& [params, expec] : tests) {
        serde::parquet::internal::binary_bound_truncator t{
          params.max_bound_size};
        auto buf = iobuf::from(params.bound);
        auto res = expec.transform([](auto& r) { return iobuf::from(r); });

        EXPECT_TRUE(is_utf8(buf, params.max_bound_size));

        std::optional<iobuf> bound;
        if (params.is_min_bound) {
            bound = t.get_min_bound(buf);
        } else {
            bound = t.get_max_bound(buf);
        }
        EXPECT_EQ(bound, res);

        // Ensure the original buffer remains un-modified.
        EXPECT_EQ(buf.linearize_to_string(), params.bound);

        if (bound) {
            EXPECT_TRUE(is_utf8(*bound, params.max_bound_size));

            if (params.is_min_bound) {
                EXPECT_LE(
                  bound->linearize_to_string(), buf.linearize_to_string());
            } else {
                EXPECT_GE(
                  bound->linearize_to_string(), buf.linearize_to_string());
            }
        }
    }
}

// NOLINTEND(*magic-number*)
