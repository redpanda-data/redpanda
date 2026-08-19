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

#include "test_utils/test.h"

#include "test_utils/async.h"
#include "test_utils/test_macros.h"

#include <seastar/core/sleep.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <optional>
#include <type_traits>

using namespace std::chrono_literals;

TEST(TestUtilsTest, assert_eventually_predicate) {
    RPTEST_REQUIRE_EVENTUALLY(100ms, [&] { return true; });
}

TEST_CORO(TestUtilsTest, assert_eventually_predicate_coro) {
    RPTEST_REQUIRE_EVENTUALLY_CORO(100ms, [&] { return true; });
}

TEST(TestUtilsTest, assert_eventually_expression) {
    int attempts = 0;
    RPTEST_REQUIRE_EVENTUALLY(5s, ++attempts >= 3);
    ASSERT_GE(attempts, 3);
}

TEST_CORO(TestUtilsTest, assert_eventually_expression_coro) {
    int attempts = 0;
    RPTEST_REQUIRE_EVENTUALLY_CORO(5s, ++attempts >= 3);
    ASSERT_GE_CORO(attempts, 3);
}

TEST(TestUtilsTest, assert_eventually_expression_with_commas) {
    // an angle-bracket comma splits the macro arguments; __VA_ARGS__
    // rejoins them
    RPTEST_REQUIRE_EVENTUALLY(5s, std::is_same_v<int, int>);
}

TEST(TestUtilsTest, assert_eventually_bool_convertible_expression) {
    std::optional<int> opt;
    auto setter = ss::sleep(10ms).then([&] { opt = 42; });
    RPTEST_REQUIRE_EVENTUALLY(5s, opt);
    ASSERT_EQ(*opt, 42);
    setter.get();
}

TEST(TestUtilsTest, assert_eventually_future_expression) {
    int attempts = 0;
    RPTEST_REQUIRE_EVENTUALLY(5s, ss::make_ready_future<bool>(++attempts >= 3));
    ASSERT_GE(attempts, 3);
}

TEST(TestUtilsTest, assert_eventually_stateful_predicate) {
    RPTEST_REQUIRE_EVENTUALLY(5s, [n = 0]() mutable { return ++n >= 3; });
}

namespace {
// A suspending predicate whose coroutine frame points into the predicate
// object; the object must stay at a stable address while suspended.
struct sleeping_predicate {
    int& attempts;
    ss::future<bool> operator()() {
        co_await ss::sleep(1ms);
        co_return ++attempts >= 3;
    }
};
} // namespace

TEST(TestUtilsTest, assert_eventually_async_predicate) {
    int attempts = 0;
    RPTEST_REQUIRE_EVENTUALLY(5s, sleeping_predicate{attempts});
    ASSERT_GE(attempts, 3);
}

// Compile-time classification of RPTEST_REQUIRE_EVENTUALLY conditions;
// eventually_predicate rejects the ambiguous (both) and invalid (neither)
// cases with a static_assert.
namespace {

using tests::detail::is_eventually_predicate;
using tests::detail::is_eventually_value;

using capturing_lambda = decltype([n = 0] { return n == 0; });
using captureless_lambda = decltype([] { return true; });

// polled as predicates
static_assert(
  is_eventually_predicate<capturing_lambda>
  && !is_eventually_value<capturing_lambda>);

// tested as values
static_assert(!is_eventually_predicate<bool> && is_eventually_value<bool>);
static_assert(!is_eventually_predicate<int*> && is_eventually_value<int*>);
static_assert(
  !is_eventually_predicate<std::optional<int>>
  && is_eventually_value<std::optional<int>>);
static_assert(
  !is_eventually_predicate<ss::future<bool>>
  && is_eventually_value<ss::future<bool>>);
// a non-nullary callable can only be null-checked, not polled
static_assert(
  !is_eventually_predicate<std::function<void(int)>>
  && is_eventually_value<std::function<void(int)>>);

// ambiguous: both invocable and convertible to bool, rejected
static_assert(
  is_eventually_predicate<captureless_lambda>
  && is_eventually_value<captureless_lambda>);
static_assert(
  is_eventually_predicate<std::function<bool()>>
  && is_eventually_value<std::function<bool()>>);
static_assert(
  is_eventually_predicate<bool (*)()> && is_eventually_value<bool (*)()>);

// neither predicate nor value, rejected
static_assert(!is_eventually_predicate<void> && !is_eventually_value<void>);

} // namespace

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
