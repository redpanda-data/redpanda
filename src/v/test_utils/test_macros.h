/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#ifndef IS_GTEST
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#define RPTEST_FAIL(m) BOOST_FAIL(m)
#define RPTEST_ADD_FAIL(m) BOOST_FAIL(m)
#define RPTEST_FAIL_CORO(m) BOOST_FAIL(m)
#define RPTEST_REQUIRE(m) BOOST_REQUIRE(m)
#define RPTEST_REQUIRE_CORO(m) BOOST_REQUIRE(m)
#define RPTEST_REQUIRE_EQ(m, n) BOOST_REQUIRE_EQUAL(m, n)
#define RPTEST_REQUIRE_EQ_CORO(m, n) BOOST_REQUIRE_EQUAL(m, n)
#define RPTEST_REQUIRE_NE(m, n) BOOST_REQUIRE_NE(m, n)
#define RPTEST_REQUIRE_NE_CORO(m, n) BOOST_REQUIRE_NE(m, n)
#else
#include "test_utils/test.h"

#define RPTEST_FAIL(m) FAIL() << (m)
#define RPTEST_ADD_FAIL(m) ADD_FAILURE() << (m)
#define RPTEST_FAIL_CORO(m) ASSERT_TRUE_CORO(false) << (m)
#define RPTEST_REQUIRE(m) ASSERT_TRUE(m)
#define RPTEST_REQUIRE_CORO(m) ASSERT_TRUE_CORO(m)
#define RPTEST_REQUIRE_EQ(m, n) ASSERT_EQ(m, n)
#define RPTEST_REQUIRE_EQ_CORO(m, n) ASSERT_EQ_CORO(m, n)
#define RPTEST_REQUIRE_NE(m, n) ASSERT_NE(m, n)
#define RPTEST_REQUIRE_NE_CORO(m, n) ASSERT_NE_CORO(m, n)
#endif
