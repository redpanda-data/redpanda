/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "absl/numeric/int128.h"
#include "bytes/iobuf.h"
#include "iceberg/bucket_transform_hashing_visitor.h"
#include "iceberg/transform.h"
#include "iceberg/transform_utils.h"
#include "iceberg/values.h"
#include "model/timestamp.h"
#include "random/generators.h"
#include "test_utils/random_bytes.h"

#include <fmt/chrono.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <optional>
#include <ranges>
#include <utility>
#include <variant>

using namespace iceberg;
using namespace std::chrono_literals;
using t_clock_t = std::chrono::system_clock;
namespace {
value make_timestamp_val(std::chrono::time_point<t_clock_t> tp) {
    return timestamp_value{tp.time_since_epoch() / 1us};
}
value make_timestamp_val(std::chrono::microseconds time_shift) {
    return timestamp_value{time_shift / 1us};
}

value make_date_val(std::chrono::microseconds time_shift) {
    auto days = std::chrono::floor<std::chrono::days>(time_shift);
    return date_value{static_cast<int32_t>(days.count())};
}
} // namespace

TEST(TestTransforms, TestHourlyTransform) {
    auto start_time = std::chrono::system_clock::now();

    auto start_transformed = apply_transform(
                               make_timestamp_val(start_time), hour_transform{})
                               .value();

    ASSERT_TRUE(std::holds_alternative<primitive_value>(start_transformed));
    ASSERT_TRUE(
      std::holds_alternative<int_value>(
        std::get<primitive_value>(start_transformed)));
    auto start_val = std::get<int_value>(
      std::get<primitive_value>(start_transformed));

    ASSERT_EQ(
      start_val.val,
      std::chrono::floor<std::chrono::hours>(start_time)
        .time_since_epoch()
        .count());

    auto plus_1hr = start_time + 1h;
    auto plus_1hr_transformed
      = apply_transform(make_timestamp_val(plus_1hr), hour_transform{}).value();
    ASSERT_NE(start_transformed, plus_1hr_transformed);
    ASSERT_TRUE(std::holds_alternative<primitive_value>(plus_1hr_transformed));
    ASSERT_TRUE(
      std::holds_alternative<int_value>(
        std::get<primitive_value>(plus_1hr_transformed)));
    auto plus_1hr_val = std::get<int_value>(
      std::get<primitive_value>(plus_1hr_transformed));
    ASSERT_EQ(start_val.val + 1, plus_1hr_val.val);

    auto minus_1hr = start_time - 1h;
    auto minus_1hr_transformed
      = apply_transform(make_timestamp_val(minus_1hr), hour_transform{})
          .value();
    ASSERT_NE(start_transformed, minus_1hr_transformed);
    ASSERT_TRUE(std::holds_alternative<primitive_value>(minus_1hr_transformed));
    ASSERT_TRUE(
      std::holds_alternative<int_value>(
        std::get<primitive_value>(minus_1hr_transformed)));
    auto minus_1hr_val = std::get<int_value>(
      std::get<primitive_value>(minus_1hr_transformed));
    ASSERT_EQ(start_val.val - 1, minus_1hr_val.val);
}

struct time_transform_test_case {
    std::chrono::microseconds time_shift;
    transform tr;
    int32_t expected_result;
};

const std::vector<time_transform_test_case> time_transform_test_cases{
  time_transform_test_case{
    .time_shift = 0s, .tr = hour_transform{}, .expected_result = 0},
  time_transform_test_case{
    .time_shift = 0s, .tr = day_transform{}, .expected_result = 0},
  time_transform_test_case{
    .time_shift = 0s, .tr = month_transform{}, .expected_result = 0},
  time_transform_test_case{
    .time_shift = 0s, .tr = year_transform{}, .expected_result = 0},
  time_transform_test_case{
    .time_shift = 10s, .tr = hour_transform{}, .expected_result = 0},
  time_transform_test_case{
    .time_shift = -10s, .tr = hour_transform{}, .expected_result = -1},
  time_transform_test_case{
    .time_shift = -10h, .tr = hour_transform{}, .expected_result = -10},
  time_transform_test_case{
    .time_shift = -(10h + 1us), .tr = hour_transform{}, .expected_result = -11},
  time_transform_test_case{
    .time_shift = std::chrono::days(-1) - 1us,
    .tr = hour_transform{},
    .expected_result = -25},
  time_transform_test_case{
    .time_shift = std::chrono::days(-1) - 1us,
    .tr = day_transform{},
    .expected_result = -2},
  time_transform_test_case{
    .time_shift = std::chrono::days(-1) - 1us,
    .tr = month_transform{},
    .expected_result = -1},
  time_transform_test_case{
    .time_shift = std::chrono::days(-1) - 1us,
    .tr = year_transform{},
    .expected_result = -1},
  time_transform_test_case{
    .time_shift = std::chrono::years(100),
    .tr = month_transform{},
    .expected_result = 1199},
  time_transform_test_case{
    .time_shift = std::chrono::years(100),
    .tr = year_transform{},
    .expected_result = 99},
  time_transform_test_case{
    .time_shift = std::chrono::years(100),
    .tr = day_transform{},
    .expected_result = 36524,
  },
  time_transform_test_case{
    .time_shift = std::chrono::years(100),
    .tr = hour_transform{},
    .expected_result = 876582},
};

std::vector<time_transform_test_case> date_transform_test_cases() {
    std::vector<time_transform_test_case> ret;

    for (const auto& tc : time_transform_test_cases) {
        if (std::holds_alternative<hour_transform>(tc.tr)) {
            continue;
        }
        ret.push_back(tc);
    }
    return ret;
};

struct TestTimeTransforms
  : public testing::TestWithParam<time_transform_test_case> {};
struct TestDateTransforms
  : public testing::TestWithParam<time_transform_test_case> {};

TEST_P(TestTimeTransforms, TestConversion) {
    auto test_case = GetParam();

    auto transformed = apply_transform(
                         make_timestamp_val(test_case.time_shift), test_case.tr)
                         .value();
    ASSERT_TRUE(std::holds_alternative<primitive_value>(transformed));
    ASSERT_TRUE(
      std::holds_alternative<int_value>(
        std::get<primitive_value>(transformed)));
    auto transformed_val = std::get<int_value>(
      std::get<primitive_value>(transformed));

    ASSERT_EQ(transformed_val.val, test_case.expected_result);
}

TEST_P(TestDateTransforms, TestConversion) {
    auto test_case = GetParam();

    auto transformed = apply_transform(
                         make_date_val(test_case.time_shift), test_case.tr)
                         .value();
    ASSERT_TRUE(std::holds_alternative<primitive_value>(transformed));
    ASSERT_TRUE(
      std::holds_alternative<int_value>(
        std::get<primitive_value>(transformed)));
    auto transformed_val = std::get<int_value>(
      std::get<primitive_value>(transformed));

    ASSERT_EQ(transformed_val.val, test_case.expected_result);
}

INSTANTIATE_TEST_SUITE_P(
  TestAllTimeTimeTransforms,
  TestTimeTransforms,
  ::testing::ValuesIn(time_transform_test_cases));
INSTANTIATE_TEST_SUITE_P(
  TestAllTimeTimeTransforms,
  TestDateTransforms,
  ::testing::ValuesIn(date_transform_test_cases()));

struct primitive_test_values {
    value boolean_val = primitive_value{boolean_value{true}};
    value int_val = primitive_value{int_value{321123}};
    value long_val = primitive_value{long_value{321123321123}};
    value float_val = primitive_value{float_value{3.14}};
    value double_val = primitive_value{double_value{6.23}};
    value date_val = primitive_value{date_value{1000}};
    value time_val = primitive_value{time_value{std::chrono::hours(10) / 1us}};
    value timestamp_val = primitive_value{timestamp_value{1741177530000000}};
    value timestamptz_val = primitive_value{
      timestamptz_value{1741177530000000}};
    value string_val = primitive_value{
      string_value{iobuf::from("non-latin to test UTF-8: Алексей")}};
    value uuid_val = primitive_value{
      uuid_value{uuid_t::from_string("ab4ed576-b638-424f-89d8-4ea602393772")}};
    value fixed_val = primitive_value{
      fixed_value{iobuf::from("\xDE\xAD\xBE\xEF")}};
    value binary_val = primitive_value{
      binary_value{iobuf::from("\xDE\xAD\xBE\xEF")}};
    value decimal_val = primitive_value{
      decimal_value{absl::MakeInt128(0, 123)}};
    value decimal_val_2 = primitive_value{
      decimal_value{absl::MakeInt128(1, 123)}};
    value decimal_val_3 = primitive_value{
      decimal_value{absl::MakeInt128(6321412421, 53441242)}};
    value decimal_val_4 = primitive_value{decimal_value{absl::int128(-1012)}};
    value decimal_val_5 = primitive_value{
      decimal_value{absl::MakeInt128(-1, 123)}};
    value decimal_val_6 = primitive_value{
      decimal_value{absl::MakeInt128(-321123321, 123)}};
};

TEST(TestTransformApplication, IdentityAndVoidTransforms) {
    primitive_test_values test_values;

    auto test_transform = [](const value& val) {
        auto id_transformed
          = apply_transform(val, identity_transform{}).value();
        ASSERT_EQ(val, id_transformed);

        auto void_transformed = apply_transform(val, void_transform{});
        ASSERT_EQ(void_transformed, std::nullopt);
    };

    test_transform(test_values.boolean_val);
    test_transform(test_values.int_val);
    test_transform(test_values.long_val);
    test_transform(test_values.float_val);
    test_transform(test_values.double_val);
    test_transform(test_values.date_val);
    test_transform(test_values.time_val);
    test_transform(test_values.timestamp_val);
    test_transform(test_values.timestamptz_val);
    test_transform(test_values.string_val);
    test_transform(test_values.uuid_val);
    test_transform(test_values.fixed_val);
    test_transform(test_values.binary_val);
    test_transform(test_values.decimal_val);
}

TEST(TestTransformApplication, BucketTransform) {
    primitive_test_values test_values;
    auto test_transform =
      [](const value& val, uint32_t buckets, int32_t expected) {
          auto transformed
            = apply_transform(val, bucket_transform{.n = buckets}).value();
          ASSERT_TRUE(std::holds_alternative<primitive_value>(transformed));
          const auto& p_val = std::get<primitive_value>(transformed);
          ASSERT_TRUE(std::holds_alternative<int_value>(p_val));
          auto bucket = std::get<int_value>(p_val).val;
          ASSERT_EQ(expected, bucket) << fmt::format(
            "Bucket {} for value: {} expected to be equal to {} (bucket count: "
            "{})",
            bucket,
            val,
            expected,
            buckets);
      };

    auto test_transform_3 = [&](
                              const value& val,
                              int32_t expected_bucket_16,
                              int32_t expected_bucket_128,
                              int32_t expected_bucket_2025) {
        test_transform(val, 16, expected_bucket_16);
        test_transform(val, 128, expected_bucket_128);
        test_transform(val, 2025, expected_bucket_2025);
    };

    test_transform_3(test_values.int_val, 13, 93, 976);
    test_transform_3(test_values.long_val, 6, 118, 1283);
    test_transform_3(test_values.date_val, 12, 12, 1270);
    test_transform_3(test_values.time_val, 2, 66, 19);
    test_transform_3(test_values.timestamp_val, 15, 15, 603);
    test_transform_3(test_values.timestamptz_val, 15, 15, 603);
    test_transform_3(test_values.string_val, 12, 28, 1337);
    test_transform_3(test_values.uuid_val, 12, 108, 975);
    test_transform_3(test_values.fixed_val, 10, 74, 197);
    test_transform_3(test_values.binary_val, 10, 74, 197);
    test_transform_3(test_values.decimal_val, 1, 49, 287);
    test_transform_3(test_values.decimal_val_2, 5, 69, 439);
    test_transform_3(test_values.decimal_val_3, 5, 21, 872);
    test_transform_3(test_values.decimal_val_4, 2, 50, 355);
    test_transform_3(test_values.decimal_val_5, 11, 123, 342);
    test_transform_3(test_values.decimal_val_6, 3, 3, 603);
}

TEST(TestTransformApplication, BucketTransformHashingVisitor) {
    // test individual hash values from
    // https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements
    auto test_hash = [&](const auto& val, int32_t expected) {
        ASSERT_EQ(bucket_transform_hashing_visitor{}(val), expected);
    };
    test_hash(int_value{34}, 2017239379);
    test_hash(long_value{34}, 2017239379);
    test_hash(decimal_value{1420}, -500754589);
    int32_t days_since_epoch_2017_11_16 = 17486;
    test_hash(date_value{days_since_epoch_2017_11_16}, -653330422);
    int64_t us_since_midnight_22_31_08 = int64_t{1000000}
                                         * (8 + 60 * (31 + 60 * 22));
    test_hash(time_value{us_since_midnight_22_31_08}, -662762989);
    int64_t us_since_epoch_2017_11_16__22_31_08
      = int64_t{days_since_epoch_2017_11_16} * 86400 * 1000000
        + us_since_midnight_22_31_08;
    test_hash(
      timestamp_value{us_since_epoch_2017_11_16__22_31_08}, -2047944441);
    test_hash(
      timestamp_value{us_since_epoch_2017_11_16__22_31_08 + 1}, -1207196810);
    test_hash(
      timestamptz_value{us_since_epoch_2017_11_16__22_31_08}, -2047944441);
    test_hash(
      timestamptz_value{us_since_epoch_2017_11_16__22_31_08 + 1}, -1207196810);
    {
        iobuf ib;
        uint8_t s[]{"iceberg"};
        ib.append(s, sizeof(s) - 1);
        test_hash(string_value{std::move(ib)}, 1210000089);
    }
    test_hash(
      uuid_value{uuid_t::from_string("f79c3e09-677c-4bbd-a479-3f349cb785e7")},
      1488055340);
    {
        iobuf ib;
        uint8_t bytes[]{0x00, 0x01, 0x02, 0x03};
        ib.append(bytes, sizeof(bytes));
        test_hash(fixed_value{std::move(ib)}, -188683207);
    }
    {
        iobuf ib;
        uint8_t bytes[]{0x00, 0x01, 0x02, 0x03};
        ib.append(bytes, sizeof(bytes));
        test_hash(binary_value{std::move(ib)}, -188683207);
    }
}

TEST(TestTransformApplication, NumericTruncateTransform) {
    auto test =
      [](primitive_value input, uint32_t length, primitive_value expected) {
          value input_wrapped{
            std::in_place_type<primitive_value>, std::move(input)};
          auto transformed = apply_transform(
                               input_wrapped,
                               truncate_transform{.length = length})
                               .value();
          ASSERT_TRUE(std::holds_alternative<primitive_value>(transformed));
          ASSERT_EQ(std::get<primitive_value>(transformed), expected);
      };

    constexpr auto min_signed = std::numeric_limits<int32_t>::min();
    constexpr auto max_signed = std::numeric_limits<int32_t>::max();
    constexpr uint32_t max_signed_u = max_signed;
    constexpr uint32_t max_unsigned = std::numeric_limits<uint32_t>::max();
    auto various_int32s = std::to_array(
      {min_signed, -100, -1, 0, 1, 100, max_signed});
    for (auto i : various_int32s) {
        test(int_value{i}, 0, int_value{i});
        test(int_value{i}, 1, int_value{i});
    }
    test(int_value{1000}, 5, int_value{1000});
    test(int_value{1001}, 5, int_value{1000});
    test(int_value{-1000}, 5, int_value{-1000});
    test(int_value{-1001}, 5, int_value{-1005});
    test(int_value{0}, max_signed_u, int_value{0});
    test(int_value{1000}, max_signed_u, int_value{0});
    test(int_value{-1000}, max_signed_u, int_value{-max_signed});
    test(int_value{max_signed}, max_signed_u, int_value{max_signed});
    test(int_value{min_signed + 1}, max_signed_u, int_value{min_signed + 1});
    // underflow
    test(int_value{min_signed}, max_signed_u, int_value{2});
    for (auto i : various_int32s) {
        for (auto len : std::to_array({max_signed_u + 1, max_unsigned})) {
            test(int_value{i}, len, int_value{0});
        }
    }

    constexpr auto min_signed_long = std::numeric_limits<int64_t>::min();
    constexpr auto max_signed_long = std::numeric_limits<int64_t>::max();
    auto various_int64s = std::to_array(
      {min_signed_long,
       int64_t{-100},
       int64_t{-1},
       int64_t{0},
       int64_t{1},
       int64_t{100},
       max_signed_long});
    for (auto i : various_int64s) {
        test(long_value{i}, 0, long_value{i});
        test(long_value{i}, 1, long_value{i});
    }
    test(long_value{100010001000}, 5, long_value{100010001000});
    test(long_value{100010001001}, 5, long_value{100010001000});
    test(long_value{-100010001000}, 5, long_value{-100010001000});
    test(long_value{-100010001005}, 5, long_value{-100010001005});
    test(long_value{0}, max_signed_u, long_value{0});
    test(long_value{1000}, max_signed_u, long_value{0});
    test(long_value{-1000}, max_signed_u, long_value{-max_signed});
    test(long_value{max_signed}, max_signed_u, long_value{max_signed});
    test(long_value{min_signed + 1}, max_signed_u, long_value{min_signed + 1});
    test(
      long_value{int64_t{max_signed} * 10},
      max_signed_u,
      long_value{int64_t{max_signed} * 10});
    test(
      long_value{int64_t{max_signed} * 10 + 1},
      max_signed_u,
      long_value{int64_t{max_signed} * 10});
    test(
      long_value{int64_t{max_signed} * 10 - 1},
      max_signed_u,
      long_value{int64_t{max_signed} * 9});
    test(
      long_value{-int64_t{max_signed} * 10},
      max_signed_u,
      long_value{-int64_t{max_signed} * 10});
    test(
      long_value{-int64_t{max_signed} * 10 + 1},
      max_signed_u,
      long_value{-int64_t{max_signed} * 10});
    test(
      long_value{-int64_t{max_signed} * 10 - 1},
      max_signed_u,
      long_value{-int64_t{max_signed} * 11});
    // underflow
    test(long_value{min_signed_long}, 3, long_value{max_signed_long});

    constexpr auto min_signed_128 = std::numeric_limits<absl::int128>::min();
    constexpr auto max_signed_128 = std::numeric_limits<absl::int128>::max();
    auto various_int128s = std::to_array(
      {min_signed_128,
       absl::int128{-100},
       absl::int128{-1},
       absl::int128{0},
       absl::int128{1},
       absl::int128{100},
       max_signed_128});
    for (auto i : various_int128s) {
        test(decimal_value{i}, 0, decimal_value{i});
        test(decimal_value{i}, 1, decimal_value{i});
    }
    test(decimal_value{100010001000}, 5, decimal_value{100010001000});
    test(decimal_value{100010001001}, 5, decimal_value{100010001000});
    test(decimal_value{-100010001000}, 5, decimal_value{-100010001000});
    test(decimal_value{-100010001005}, 5, decimal_value{-100010001005});
    test(decimal_value{0}, max_signed_u, decimal_value{0});
    test(decimal_value{1000}, max_signed_u, decimal_value{0});
    test(decimal_value{-1000}, max_signed_u, decimal_value{-max_signed});
    test(decimal_value{max_signed}, max_signed_u, decimal_value{max_signed});
    test(
      decimal_value{min_signed + 1},
      max_signed_u,
      decimal_value{min_signed + 1});
    test(
      decimal_value{max_signed * absl::int128{max_signed_long}},
      max_signed_u,
      decimal_value{max_signed * absl::int128{max_signed_long}});
    test(
      decimal_value{max_signed * absl::int128{max_signed_long} + 1},
      max_signed_u,
      decimal_value{max_signed * absl::int128{max_signed_long}});
    test(
      decimal_value{max_signed * absl::int128{max_signed_long} - 1},
      max_signed_u,
      decimal_value{max_signed * absl::int128{max_signed_long - 1}});
    test(
      decimal_value{-max_signed * absl::int128{max_signed_long}},
      max_signed_u,
      decimal_value{-max_signed * absl::int128{max_signed_long}});
    test(
      decimal_value{-max_signed * absl::int128{max_signed_long} + 1},
      max_signed_u,
      decimal_value{-max_signed * absl::int128{max_signed_long}});
    test(
      decimal_value{-max_signed * absl::int128{max_signed_long} - 1},
      max_signed_u,
      decimal_value{-max_signed * (absl::int128{max_signed_long} + 1)});
    // underflow
    test(
      decimal_value{min_signed_128 + 1}, 5, decimal_value{max_signed_128 - 1});
}

TEST(TestTransformApplication, BinaryTruncateTransform) {
    for (int runs = 0; runs < 10; ++runs) {
        int input_len = random_generators::get_int(100);
        auto random_bytes = std::string(input_len, 0);
        for (int i = 0; i < input_len; ++i) {
            random_bytes[i] = random_generators::get_int(
              std::numeric_limits<uint8_t>::max());
        }
        auto split = tests::fragmented_iobuf(
          random_bytes, random_generators::get_int(10));

        uint32_t truncate_len = random_generators::get_int(input_len + 3);

        std::string expected_truncated_str = random_bytes.substr(
          0, truncate_len);
        iobuf expected_truncated_iobuf;
        expected_truncated_iobuf.append(
          expected_truncated_str.data(), expected_truncated_str.size());
        primitive_value expected{
          std::in_place_type<binary_value>,
          std::move(expected_truncated_iobuf)};

        auto actual_truncated = apply_transform(
                                  binary_value{std::move(split)},
                                  truncate_transform{.length = truncate_len})
                                  .value();

        ASSERT_TRUE(std::holds_alternative<primitive_value>(actual_truncated));
        ASSERT_EQ(std::get<primitive_value>(actual_truncated), expected);
    }
}

TEST(TestTransformApplication, TextTruncateTransform) {
    const std::vector<std::string> available_symbols{
      "\x01", "\t", "\n", "\r", " ", "A", "a", "@", "香", "😊", "⋀", "Ы"};

    for (int runs = 0; runs < 10; ++runs) {
        int input_len = random_generators::get_int(10);
        std::vector<std::string_view> chosen_symbols;
        chosen_symbols.reserve(input_len);
        for (int i = 0; i < input_len; ++i) {
            chosen_symbols.push_back(
              random_generators::random_choice(available_symbols));
        }
        std::string str{std::from_range, chosen_symbols | std::views::join};
        auto split = tests::fragmented_iobuf(
          str, random_generators::get_int(5));

        uint32_t truncate_len = random_generators::get_int(input_len + 3);

        std::string expected_truncated_str{
          std::from_range,
          chosen_symbols | std::views::take(truncate_len) | std::views::join};
        iobuf expected_truncated_iobuf;
        expected_truncated_iobuf.append(
          expected_truncated_str.data(), expected_truncated_str.size());
        primitive_value expected{
          std::in_place_type<string_value>,
          std::move(expected_truncated_iobuf)};

        auto actual_truncated = apply_transform(
                                  string_value{std::move(split)},
                                  truncate_transform{.length = truncate_len})
                                  .value();

        ASSERT_TRUE(std::holds_alternative<primitive_value>(actual_truncated));
        ASSERT_EQ(std::get<primitive_value>(actual_truncated), expected);
    }
}
