/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#ifndef IS_GTEST
#error expected IS_GEST to be defined
#endif

// unset this so we test the fallback macros
#undef IS_GTEST

#include "test_utils/test_macros.h"

#include <gtest/gtest.h>

TEST(TestUtilsTest, test_macros_pass) {
    RPTEST_REQUIRE(true);
    RPTEST_REQUIRE_EQ(1, 1);
    RPTEST_REQUIRE_NE(1, 2);
    RPTEST_REQUIRE_EQ_CORO(1, 1);
    RPTEST_REQUIRE_NE_CORO(1, 2);
    RPTEST_EXPECT_EQ(1, 1);
}

TEST(TestUtilsDeathTest, test_macros_fail) {
    GTEST_SKIP()
      << "TODO(death_tests): re-enable when death tests are made stable in CI.";
    ASSERT_DEATH(RPTEST_FAIL("fail message"), "fail message");
    ASSERT_DEATH(RPTEST_ADD_FAIL("fail message"), "fail message");
    ASSERT_DEATH(RPTEST_FAIL_CORO("fail message"), "fail message");
    ASSERT_DEATH(RPTEST_REQUIRE(false), "false");
    ASSERT_DEATH(RPTEST_REQUIRE_EQ(1, 2), "1 == 2");
    ASSERT_DEATH(RPTEST_REQUIRE_EQ_CORO(1, 2), "1 == 2");
    ASSERT_DEATH(RPTEST_REQUIRE_NE(1, 1), "1 != 1");
    ASSERT_DEATH(RPTEST_REQUIRE_NE_CORO(1, 1), "1 != 1");
    ASSERT_DEATH(RPTEST_EXPECT_EQ(1, 2), "1 == 2");
}
