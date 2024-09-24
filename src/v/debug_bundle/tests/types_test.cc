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

#include "debug_bundle/types.h"
#include "model/fundamental.h"
#include "model/namespace.h"

#include <absl/container/flat_hash_map.h>
#include <gtest/gtest.h>

#include <iomanip>
#include <optional>

using namespace debug_bundle;

TEST(types_test, convert_string_to_special_date_success) {
    const absl::flat_hash_map<ss::sstring, special_date> names = {
      {"yesterday", special_date::yesterday},
      {"today", special_date::today},
      {"now", special_date::now},
      {"tomorrow", special_date::tomorrow}};

    for (auto&& [k, v] : names) {
        EXPECT_NO_THROW(EXPECT_EQ(v, boost::lexical_cast<special_date>(k)))
          << v << " != " << k;
        EXPECT_EQ(fmt::format("{}", v), k);
        time_variant tv(v);
        EXPECT_EQ(fmt::format("{}", tv), k);
    }
}

TEST(types_test, convert_string_to_special_date_failure) {
    EXPECT_THROW(
      boost::lexical_cast<special_date>("bloop"), std::runtime_error);
    EXPECT_THROW(
      boost::lexical_cast<special_date>("YESTERDAY"), std::runtime_error);
}

TEST(types_test, test_print_iso_8601) {
    const std::string_view test_time = "2024-09-05T14:34:02";
    std::istringstream ss(test_time.data());
    std::tm tmp{};
    ASSERT_NO_THROW(ss >> std::get_time(&tmp, "%FT%T"));
    ASSERT_FALSE(ss.fail());
    // Forces `std::mktime` to "figure it out"
    tmp.tm_isdst = -1;
    std::time_t tt = std::mktime(&tmp);
    auto tp = clock::from_time_t(tt);
    time_variant tv(tp);
    EXPECT_EQ(fmt::format("{}", tv), test_time);
}

TEST(types_test, test_partition_print) {
    const debug_bundle::partition_selection p{
      {model::ns{"kafka"}, model::topic{"test"}},
      {model::partition_id{1},
       model::partition_id{3},
       model::partition_id{5},
       model::partition_id{7}}};

    EXPECT_EQ(fmt::format("{}", p), "kafka/test/1,3,5,7");
}

TEST(types_test, convert_string_to_partition_selection) {
    auto make = partition_selection::from_string_view;
    EXPECT_FALSE(make("").has_value());
    EXPECT_FALSE(make(" ").has_value());
    EXPECT_FALSE(make("//").has_value());
    EXPECT_FALSE(make("/foo/bar/1").has_value());
    EXPECT_FALSE(make("foo/bar/t").has_value());
    EXPECT_FALSE(make("foo/bar/").has_value());
    EXPECT_FALSE(make("foo/bar/1d").has_value());
    EXPECT_FALSE(make(" foo/bar/1").has_value());
    EXPECT_FALSE(make("foo/bar/1 ").has_value());

    partition_selection foo_bar_1{
      {model::ns{"foo"}, model::topic{"bar"}}, {{model::partition_id{1}}}};

    EXPECT_EQ(*make("foo/bar/1"), foo_bar_1);

    partition_selection bar_1_2{
      {model::kafka_namespace, model::topic{"bar"}},
      {{model::partition_id{1}, model::partition_id{2}}}};
    EXPECT_EQ(*make("bar/1,2"), bar_1_2);
}
