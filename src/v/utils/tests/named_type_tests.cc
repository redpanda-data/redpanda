// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "utils/named_type.h"

#include <seastar/core/sstring.hh>

#include <boost/test/unit_test.hpp>

#include <set>
#include <string_view>

BOOST_AUTO_TEST_CASE(named_type_basic) {
    using int_alias = named_type<int32_t, struct int_alias_test_module>;
    constexpr auto x = int_alias(5);
    BOOST_REQUIRE(x == 5);
    BOOST_REQUIRE(x <= 5);
    BOOST_REQUIRE(x < 6);
    BOOST_REQUIRE(x != 50);
    BOOST_REQUIRE(x > 4);
    BOOST_REQUIRE(x >= 5);
}
BOOST_AUTO_TEST_CASE(named_type_set) {
    using int_alias = named_type<int32_t, struct int_alias_test_module>;
    std::set<int_alias> foo;
    for (int32_t i = 0; i < 100; ++i) {
        foo.insert(int_alias(i));
        BOOST_REQUIRE(foo.find(int_alias(i)) != foo.end());
    }
}

BOOST_AUTO_TEST_CASE(named_type_unordered_map) {
    using int_alias = named_type<int32_t, struct int_alias_test_module>;
    std::unordered_map<int_alias, int_alias> foo;
    for (int32_t i = 0; i < 100; ++i) {
        foo[int_alias(i)] = int_alias(i);
    }
    BOOST_REQUIRE(foo[int_alias(5)] != int_alias(4));
}

BOOST_AUTO_TEST_CASE(string_named_type_basic) {
    using string_alias
      = named_type<ss::sstring, struct sstring_alias_test_module>;
    string_alias x;
    x = string_alias("foobar");
    BOOST_REQUIRE(x == string_alias("foobar"));
}

BOOST_AUTO_TEST_CASE(named_type_string_set) {
    using string_alias
      = named_type<ss::sstring, struct sstring_alias_test_module>;
    std::set<string_alias> foo;
    for (int32_t i = 0; i < 10; ++i) {
        auto x = ss::to_sstring(i);
        foo.insert(string_alias(x));
        BOOST_REQUIRE(foo.find(string_alias(x)) != foo.end());
    }
}

BOOST_AUTO_TEST_CASE(named_type_prefix_increment) {
    using t = named_type<int, struct int_t_alias_test_module>;
    t t0(0);
    BOOST_TEST(++t0 == 1);
    BOOST_TEST(++t0 == 2);
}

BOOST_AUTO_TEST_CASE(named_type_rvalue_overload) {
    using t = named_type<ss::sstring, struct int_t_alias_test_module>;
    static constexpr std::string_view str{"This shouldn't dangle"};
    auto f = []() { return t{ss::sstring{str}}; };
    const auto& r = f()();

    BOOST_REQUIRE_EQUAL(str, r);
}

BOOST_AUTO_TEST_CASE(named_type_stream_operators) {
    using int_alias = named_type<uint64_t, struct int_t_alias_test_module>;
    int_alias value{123};
    std::stringstream stream;
    fmt::print(stream, "{}", value);
    int_alias from_str_value;
    stream >> from_str_value;
    BOOST_REQUIRE_EQUAL(from_str_value, value);
}

static_assert(
  !std::equality_comparable_with<
    named_type<int, struct tag_0>,
    named_type<int, struct tag_1>>,
  "arithmetic named_types can't be compared directly");
static_assert(
  !std::equality_comparable_with<
    named_type<ss::sstring, struct tag_a>,
    named_type<ss::sstring, struct tag_b>>,
  "non-arithmetic named_types can't be compared directly");

using named_sstring = named_type<ss::sstring, struct named_sstring_tag>;
static_assert(std::equality_comparable_with<named_sstring, ss::sstring>);
static_assert(std::equality_comparable_with<
              std::reference_wrapper<named_sstring>,
              std::reference_wrapper<named_sstring>>);
static_assert(std::equality_comparable_with<
              named_sstring,
              std::reference_wrapper<named_sstring>>);

using named_int = named_type<int, struct named_int_tag>;
static_assert(std::equality_comparable_with<named_int, int>);
static_assert(std::equality_comparable_with<
              std::reference_wrapper<named_int>,
              std::reference_wrapper<named_int>>);
static_assert(
  std::equality_comparable_with<named_int, std::reference_wrapper<named_int>>);
