/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "iceberg/conversion/time_rfc3339.h"
#include "iceberg/values.h"

#include <gtest/gtest.h>

#include <algorithm>

using namespace iceberg::conversion::time_rfc3339;

template<typename T>
struct test_case {
    std::string name;
    ss::sstring input;
    bool expect_success;
    T expected;
};

template<typename T>
std::ostream& operator<<(std::ostream& os, const test_case<T>& tc) {
    os << "name: " << tc.name << ", input: " << tc.input
       << ", expect_success: " << std::boolalpha << tc.expect_success
       << ", expected: " << tc.expected;

    return os;
}

using timestamptz_test_case = test_case<iceberg::timestamptz_value>;

class TimestampzFromStringTest
  : public testing::TestWithParam<timestamptz_test_case> {};

TEST_P(TimestampzFromStringTest, ParseTimestampz) {
    const auto& param = GetParam();
    auto result = date_time_str_to_timestampz(param.input);

    if (param.expect_success) {
        ASSERT_TRUE(result.has_value())
          << "Expected success but got error: " << result.error().what();
        EXPECT_EQ(
          iceberg::primitive_value(result.value()),
          iceberg::primitive_value(param.expected));
    } else {
        EXPECT_FALSE(result.has_value()) << "Expected failure but got success";
    }
}

INSTANTIATE_TEST_SUITE_P(
  TimeRfc3339,
  TimestampzFromStringTest,
  testing::Values(
    timestamptz_test_case{
      "UTC timezone",
      "2025-07-01T10:15:30Z",
      true,
      iceberg::timestamptz_value{1751364930000000}},
    timestamptz_test_case{
      "UTC timezone with lowercase letters",
      "2025-07-01t10:15:30z",
      true,
      iceberg::timestamptz_value{1751364930000000}},
    timestamptz_test_case{
      "With positive timezone offset",
      "2025-07-01T12:15:30+02:00",
      true,
      iceberg::timestamptz_value{1751364930000000}},
    timestamptz_test_case{
      "With negative timezone offset",
      "2025-07-01T05:15:30-05:00",
      true,
      {1751364930000000}},
    timestamptz_test_case{
      "With fractional seconds",
      "2025-07-01T10:15:30.123456Z",
      true,
      {1751364930123456}},
    timestamptz_test_case{"Invalid format", "not-a-timestamp", false, {0}},
    timestamptz_test_case{"Invalid date", "2025-13-01T10:15:30Z", false, {0}}),
  [](const testing::TestParamInfo<timestamptz_test_case>& info) {
      auto name = info.param.name;
      std::replace_if(
        name.begin(), name.end(), [](char c) { return !std::isalnum(c); }, '_');
      return name;
  });

using date_test_case = test_case<iceberg::date_value>;

class DateFromStringTest : public testing::TestWithParam<date_test_case> {};

TEST_P(DateFromStringTest, ParseDate) {
    const auto& param = GetParam();
    auto result = date_str_to_date(param.input);

    if (param.expect_success) {
        ASSERT_TRUE(result.has_value())
          << "Expected success but got error: " << result.error().what();
        EXPECT_EQ(
          iceberg::primitive_value(result.value()),
          iceberg::primitive_value(param.expected));
    } else {
        EXPECT_FALSE(result.has_value()) << "Expected failure but got success";
    }
}

INSTANTIATE_TEST_SUITE_P(
  TimeRfc3339,
  DateFromStringTest,
  testing::Values(
    date_test_case{"Regular date", "2025-07-01", true, {20270}},
    date_test_case{"Epoch date", "1970-01-01", true, {0}},
    date_test_case{"Pre-epoch date", "1969-12-31", true, {-1}},
    date_test_case{"Leap year date", "2024-02-29", true, {19782}},
    date_test_case{"Invalid format", "not-a-date", false, {0}},
    date_test_case{"Invalid date", "2025-02-30", false, {0}}),
  [](const testing::TestParamInfo<date_test_case>& info) {
      auto name = info.param.name;
      std::replace_if(
        name.begin(), name.end(), [](char c) { return !std::isalnum(c); }, '_');
      return name;
  });

using time_test_case = test_case<iceberg::time_value>;

class TimeFromStringTest : public testing::TestWithParam<time_test_case> {};

TEST_P(TimeFromStringTest, ParseTime) {
    const auto& param = GetParam();
    auto result = time_str_to_time(param.input);

    if (param.expect_success) {
        ASSERT_TRUE(result.has_value())
          << "Expected success but got error: " << result.error().what();
        EXPECT_EQ(
          iceberg::primitive_value(result.value()),
          iceberg::primitive_value(param.expected));
    } else {
        EXPECT_FALSE(result.has_value()) << "Expected failure but got success";
    }
}

INSTANTIATE_TEST_SUITE_P(
  TimeRfc3339,
  TimeFromStringTest,
  testing::Values(
    time_test_case{"Midnight", "00:00:00Z", true, {0}},
    time_test_case{"Regular time", "10:15:30Z", true, {36930000000}},
    time_test_case{"Almost midnight", "23:59:59Z", true, {86399000000}},
    time_test_case{
      "With microseconds", "10:15:30.123456Z", true, {36930123456}},
    time_test_case{
      "With timezone offset", "12:15:30.123456+02:00", true, {36930123456}},
    time_test_case{
      "With negative timezone offset",
      "08:15:30.123456-02:00",
      true,
      {36930123456}},
    time_test_case{"Invalid format", "not-a-time", false, {0}},
    time_test_case{"Invalid time", "25:15:30", false, {0}}),
  [](const testing::TestParamInfo<time_test_case>& info) {
      auto name = info.param.name;
      std::replace_if(
        name.begin(), name.end(), [](char c) { return !std::isalnum(c); }, '_');
      return name;
  });
