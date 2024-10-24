// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "strings/string_switch.h"

#include <boost/test/unit_test.hpp>

#include <stdexcept>

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

BOOST_AUTO_TEST_CASE(string_switch_no_match) {
    BOOST_CHECK_EXCEPTION(
      string_switch<int8_t>("ccc").match("a", 0).match("b", 1).
      operator int8_t(),
      std::runtime_error,
      [](const std::runtime_error& e) {
          // check that the error string includes the string we were searching
          // for as a weak hint to where the error occurred
          if (!std::string(e.what()).ends_with("ccc")) {
              BOOST_TEST_FAIL(
                "Expected error message to end with ccc but was: " << e.what());
          };
          return true;
      });
}
