// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "strings/string_switch.h"

#include <gtest/gtest.h>

#include <stdexcept>

TEST(StringSwitch, MatchOne) {
    EXPECT_EQ(
      int8_t(42),
      string_switch<int8_t>("hello")
        .match("world", -1)
        .match("hello", 42)
        .default_match(0));
}

TEST(StringSwitch, StartsWithOne) {
    EXPECT_EQ(
      int8_t(42),
      string_switch<int8_t>("hello, world!")
        .starts_with("world", -1)
        .starts_with("hello", 42)
        .default_match(0));
}

TEST(StringSwitch, EndsWithOne) {
    EXPECT_EQ(
      int8_t(42),
      string_switch<int8_t>("hello, world!")
        .ends_with("hello", -1)
        .ends_with("world!", 42)
        .default_match(0));
}

TEST(StringSwitch, Default) {
    EXPECT_EQ(
      int8_t(-66),
      string_switch<int8_t>("hello")
        .match("x", -1)
        .match("y", 42)
        .default_match(-66));
}

TEST(StringSwitch, MatchAll) {
    EXPECT_EQ(
      int8_t(42),
      string_switch<int8_t>("hello")
        .match_all("x", "y", "hello", 42)
        .default_match(-66));
}

TEST(StringSwitch, MatchAllMax) {
    EXPECT_EQ(
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

TEST(StringSwitch, NoMatch) {
    EXPECT_THROW(
      {
          try {
              string_switch<int8_t>("ccc").match("a", 0).match("b", 1).
              operator int8_t();
          } catch (const std::runtime_error& e) {
              // check that the error string includes the string we were
              // searching for as a weak hint to where the error occurred
              EXPECT_TRUE(std::string(e.what()).ends_with("ccc"))
                << "Expected error message to end with 'ccc' but was: "
                << e.what();
              throw;
          }
      },
      std::runtime_error);
}
