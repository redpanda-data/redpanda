// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#define BOOST_TEST_MODULE utils

#include "strings/string_switch.h"

#include <boost/test/unit_test.hpp>

constexpr int simple_string_switch(std::string_view str) {
    return string_switch<int>(str)
      .match("one", 1)
      .match("two", 2)
      .match("three", 3)
      .default_match(0);
}

BOOST_AUTO_TEST_CASE(swith_static_types) {
    constexpr int one = simple_string_switch("one");
    BOOST_CHECK_EQUAL(one, 1);
    constexpr int two = simple_string_switch("two");
    BOOST_CHECK_EQUAL(two, 2);
    constexpr int three = simple_string_switch("three");
    BOOST_CHECK_EQUAL(three, 3);
    constexpr int none = simple_string_switch("none");
    BOOST_CHECK_EQUAL(none, 0);
}
BOOST_AUTO_TEST_CASE(swith_dynamic_types) {
    auto one = std::make_unique<int>(simple_string_switch("one"));
    BOOST_CHECK_EQUAL(*one, 1);
}
