/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#define BOOST_TEST_MODULE pandaproxy

#include "pandaproxy/parsing/from_chars.h"

#include "utils/named_type.h"

#include <boost/mpl/list.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <cstddef>
#include <optional>
#include <ratio>
#include <system_error>

namespace pp = pandaproxy;

using mini_milliseconds = std::chrono::duration<int8_t, std::milli>;

using integral_test_types = boost::mpl::list<
  int8_t,
  uint16_t,
  named_type<int8_t, struct Tag>,
  mini_milliseconds,
  std::optional<int8_t>>;

using string_test_types = boost::mpl::
  list<std::string, ss::sstring, named_type<ss::sstring, struct Tag>>;

using optional_test_types = boost::mpl::list<
  std::optional<ss::sstring>,
  std::optional<int8_t>,
  std::optional<named_type<ss::sstring, struct Tag>>,
  std::optional<named_type<mini_milliseconds, struct Tag>>>;

BOOST_AUTO_TEST_CASE_TEMPLATE(from_chars_to_integral, T, integral_test_types) {
    constexpr const auto string_literal{"42"};
    auto res_0 = pp::parse::from_chars<T>{}(string_literal);
    using result_type = std::decay_t<decltype(res_0.value())>;
    static_assert(std::is_same_v<result_type, T>);
    BOOST_REQUIRE(res_0);
    BOOST_REQUIRE(res_0.has_value());
    BOOST_REQUIRE(res_0.value() == T{42});
}

BOOST_AUTO_TEST_CASE_TEMPLATE(
  from_chars_overflow_integral, T, integral_test_types) {
    constexpr const auto string_literal{"65537"};
    auto res_0 = pp::parse::from_chars<T>{}(string_literal);
    using result_type = std::decay_t<decltype(res_0.value())>;
    static_assert(std::is_same_v<result_type, T>);
    BOOST_REQUIRE(!res_0);
    BOOST_REQUIRE(res_0.has_error());
    BOOST_REQUIRE(res_0.error() == std::errc::result_out_of_range);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(
  fron_chars_integral_from_float, T, integral_test_types) {
    using input_type = named_type<int8_t, struct Tag>;
    constexpr const auto string_literal{"1.2"};
    auto res_0 = pp::parse::from_chars<input_type>{}(string_literal);
    using result_type = std::decay_t<decltype(res_0.value())>;
    static_assert(std::is_same_v<result_type, input_type>);
    BOOST_REQUIRE(!res_0);
    BOOST_REQUIRE(res_0.has_error());
    BOOST_REQUIRE(res_0.error() == std::errc::invalid_argument);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(from_chars_to_string, T, string_test_types) {
    constexpr const auto string_literal{"string_literal"};
    auto res_0 = pp::parse::from_chars<T>{}(string_literal);
    using result_type = std::decay_t<decltype(res_0.value())>;
    static_assert(std::is_same_v<result_type, T>);
    BOOST_REQUIRE(res_0);
    BOOST_REQUIRE(res_0.has_value());
    BOOST_REQUIRE_EQUAL(res_0.value(), T{string_literal});
}

BOOST_AUTO_TEST_CASE_TEMPLATE(
  from_chars_to_string_empty, T, string_test_types) {
    constexpr const auto string_literal{""};
    auto res_0 = pp::parse::from_chars<T>{}(string_literal);
    using result_type = std::decay_t<decltype(res_0.value())>;
    static_assert(std::is_same_v<result_type, T>);
    BOOST_REQUIRE(!res_0);
    BOOST_REQUIRE(res_0.has_error());
    BOOST_REQUIRE(res_0.error() == std::errc::invalid_argument);
}

BOOST_AUTO_TEST_CASE_TEMPLATE(
  from_chars_to_optional_empty, T, optional_test_types) {
    constexpr const auto string_literal{""};
    auto res_0 = pp::parse::from_chars<T>{}(string_literal);
    using result_type = std::decay_t<decltype(res_0.value())>;
    static_assert(std::is_same_v<result_type, T>);
    BOOST_REQUIRE(res_0);
    BOOST_REQUIRE(res_0.has_value());
    BOOST_REQUIRE(res_0.value() == std::nullopt);
}
