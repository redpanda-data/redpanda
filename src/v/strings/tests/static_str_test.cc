// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "strings/static_str.h"

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_suite.hpp>
#include <fmt/core.h>

#include <string_view>

// for boost test
std::ostream& operator<<(std::ostream& o, const static_str& s) {
    return o << (std::string_view)s;
}

using namespace std::string_view_literals;

static constexpr std::string_view abc_sv = "abc";

namespace static_tests {
constexpr static_str abc = "abc";
constexpr static_str abc2 = "abc";
constexpr static_str abc3{"abc"sv};
constexpr static_str abd = "abd";

static_assert(abc == abc); // NOLINT(misc-redundant-expression)
static_assert(abc == abc2);
static_assert(abc == abc3);
static_assert(abc != abd);
static_assert(abc < abd);

static_assert(abc == "abc"sv);
static_assert(abc != "ccc"sv);

static_assert(abc <= "abc"sv);
static_assert(!(abc <= "abb"sv));

// check that prefix does not compare equal
static_assert(static_str{abc_sv} != static_str{abc_sv.substr(0, 2)});

constexpr auto zero_term_abc = std::array{'a', 'b', 'c', '\0'};
static_assert(static_str{zero_term_abc.data()} == "abc"sv);

// // the following must not compile:
//
// // init from a constexpr non-null-terminated string
// constexpr auto no_zero_term_abc = std::array{'a', 'b', 'c'};
// static_assert(static_str{no_zero_term_abc.data()} == "abc"sv);

// // init with non-constexpr string
// const auto non_constexpr_str = "abc";
// static_assert(static_str{non_constexpr_str} == "abc"sv);

} // namespace static_tests

BOOST_AUTO_TEST_CASE(static_str_compare) {
    static_str abc = "abc";
    static_str abc2 = "abc";
    static_str abd = "abd";

    BOOST_CHECK(abc == abc);
    BOOST_CHECK(abc == abc2);
    BOOST_CHECK(abc != abd);
    BOOST_CHECK(abc < abd);

    BOOST_CHECK(abc == "abc"sv);
    BOOST_CHECK(abc != "ccc"sv);

    BOOST_CHECK(abc <= "abc"sv);
    BOOST_CHECK(!(abc <= "abb"sv));

    BOOST_CHECK(abc == static_tests::abc);
    BOOST_CHECK(abc <= static_tests::abc);
    BOOST_CHECK(abc < static_tests::abd);
}

BOOST_AUTO_TEST_CASE(static_str_prefix) {
    static_str abc{abc_sv};
    static_str ab{abc_sv.substr(0, 2)};

    BOOST_REQUIRE_EQUAL(abc, "abc"sv);
    BOOST_REQUIRE_EQUAL(ab, "ab"sv);

    BOOST_REQUIRE_NE(ab, abc);
}

BOOST_AUTO_TEST_CASE(static_str_format) {
    using namespace static_tests;

    BOOST_REQUIRE_EQUAL(fmt::format("{}", abc), "abc");
    BOOST_REQUIRE_EQUAL(fmt::format("{:>4}", abc), " abc");
    BOOST_REQUIRE_EQUAL(fmt::format("{:<4}", abc), "abc ");
}
