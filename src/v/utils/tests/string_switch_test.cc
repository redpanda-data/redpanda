// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/string_switch.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(string_switch_match_one) {
    BOOST_REQUIRE_EQUAL(
      int8_t(42),
      string_switch<int8_t>("hello")
        .match("world", -1)
        .match("hello", 42)
        .default_match(0));
}
BOOST_AUTO_TEST_CASE(string_switch_default) {
    BOOST_REQUIRE_EQUAL(
      int8_t(-66),
      string_switch<int8_t>("hello")
        .match("x", -1)
        .match("y", 42)
        .default_match(-66));
}
BOOST_AUTO_TEST_CASE(string_switch_match_all) {
    BOOST_REQUIRE_EQUAL(
      int8_t(42),
      string_switch<int8_t>("hello")
        .match_all("x", "y", "hello", 42)
        .default_match(-66));
}
BOOST_AUTO_TEST_CASE(string_switch_match_all_max) {
    BOOST_REQUIRE_EQUAL(
      int8_t(42),
      string_switch<int8_t>("hello")
        .match_all(
          "san",
          "francisco",
          "vectorized",
          "redpanda",
          "cycling",
          "c++",
          "x",
          "y",
          "hello",
          42)
        .default_match(-66));
}
