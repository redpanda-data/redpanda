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
#include "test_utils/async.h"
#include "test_utils/gtest_utils.h"
#include "test_utils/test.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/sleep.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/file.hh>

#include <gtest/gtest.h>

#include <chrono>

using namespace std::chrono_literals;

/**
 * If you're looking for Google Test general documentation,
 * there are excellent docs available online here:
 * - http://google.github.io/googletest/
 * - Assertion reference:
 * http://google.github.io/googletest/reference/assertions.html
 * - Matchers reference:
 * http://google.github.io/googletest/reference/matchers.html
 */

/**
 * By default gtest runs within an seastar::thread
 */
TEST(SeastarTest, Sleep) {
    seastar::sleep(std::chrono::milliseconds(100)).get();
}

/*
 * TEST_CORO() is the coroutine-enabled equivalent of TEST().
 */
TEST_CORO(SeastarTest, SleepCoro) {
    co_await seastar::sleep(std::chrono::milliseconds(100));
    ASSERT_EQ_CORO(100, 100);
}

TEST(SeastarTest, assert_eventually) {
    RPTEST_REQUIRE_EVENTUALLY(100ms, [] { return true; });
}

TEST_CORO(SeastarTest, assert_eventually_coro) {
    RPTEST_REQUIRE_EVENTUALLY_CORO(100ms, [] { return true; });
}

TEST_CORO(SeastarTest, SkippingWorks) {
    GTEST_SKIP_CORO() << "Skipping should not trip the following assertion";
    ASSERT_FALSE_CORO(true);
}

/*
 * TEST_F() runs within a seastar thread
 */
class MySeastarFixture : public testing::Test {
public:
    std::string_view message() const { return "hello"; }

    ~MySeastarFixture() override {
        assert(setup_called);
        assert(teardown_called);
    }

    void SetUp() override {
        seastar::sleep(std::chrono::milliseconds(100)).get();
        setup_called = true;
    }

    void TearDown() override {
        seastar::sleep(std::chrono::milliseconds(100)).get();
        teardown_called = true;
    }

private:
    bool setup_called{false};
    bool teardown_called{false};
};

TEST_F(MySeastarFixture, Sleep) {
    seastar::sleep(std::chrono::milliseconds(100)).get();
    ASSERT_EQ(message(), "hello");
}

class MySeastarParamFixture
  : public MySeastarFixture
  , public ::testing::WithParamInterface<int> {};

TEST_P(MySeastarParamFixture, Sleep) {
    seastar::sleep(std::chrono::milliseconds(GetParam() * 10)).get();
    ASSERT_EQ(GetParam() % 11, 0);
}

INSTANTIATE_TEST_SUITE_P(
  Divisible, MySeastarParamFixture, testing::Values(11, 22, 33));

/*
 * TEST_F_CORO() is the coroutine-enabled equivalent of TEST_F()
 *
 * Extending seastar_test instead of testing::Test allows for
 * using coroutines in setup/teardown functions
 */
class MySeastarCoroFixture : public seastar_test {
public:
    std::string_view message() const { return "hello"; }

    ~MySeastarCoroFixture() override {
        assert(setup_called);
        assert(teardown_called);
    }

    seastar::future<> SetUpAsync() override {
        co_await seastar::sleep(std::chrono::milliseconds(100));
        setup_called = true;
    }

    seastar::future<> TearDownAsync() override {
        co_await seastar::sleep(std::chrono::milliseconds(100));
        teardown_called = true;
    }

private:
    bool setup_called{false};
    bool teardown_called{false};
};

TEST_F_CORO(MySeastarCoroFixture, SleepCoro) {
    co_await seastar::sleep(std::chrono::milliseconds(100));
    ASSERT_EQ_CORO(message(), "hello");
}

/*
 * TEST_P_CORO() is the coroutine-enabled equivalent of TEST_P()
 */
class MySeastarCoroParamFixture
  : public MySeastarCoroFixture
  , public ::testing::WithParamInterface<int> {};

TEST_P_CORO(MySeastarCoroParamFixture, SleepCoro) {
    co_await seastar::sleep(std::chrono::milliseconds(100));
    ASSERT_TRUE_CORO(true);
    ASSERT_FALSE_CORO(false);
}

INSTANTIATE_TEST_SUITE_P(
  Divisible, MySeastarCoroParamFixture, testing::Values(11, 22, 33));

/*
 * Normal test framework macros continues to work.
 */
TEST(NotSeastar, T) {}

class NotSeastarFixture : public ::testing::Test {};
TEST_F(NotSeastarFixture, T) {}

class NotSeastarFixtureParam : public testing::TestWithParam<int> {};
TEST_P(NotSeastarFixtureParam, T) {}

INSTANTIATE_TEST_SUITE_P(
  NotSeastarParamTest, NotSeastarFixtureParam, testing::Values(1, 2, 3));

class directory_test {
public:
    // Test body that creates and removes a test directory.
    // Encapsulated to be reused across different test harness types.
    void create_and_remove() {
        auto dir = get_test_directory();
        auto dir_again = get_test_directory();

        // Repeated calls to get_test_directory() from the same test process
        // should result in the same output.
        ASSERT_EQ(dir, dir_again);

        ASSERT_FALSE(ss::file_exists(dir).get());
        ss::recursive_touch_directory(dir).get();
        auto cleanup = ss::defer([&dir] {
            ss::recursive_remove_directory(std::filesystem::path{dir}).get();
        });
        ASSERT_TRUE(ss::file_exists(dir).get());
    }
};

TEST(NotSeastar, TestGetTestDirectory) {
    ASSERT_NO_FATAL_FAILURE(directory_test{}.create_and_remove());
}

TEST_F(MySeastarFixture, TestGetTestDirectory) {
    ASSERT_NO_FATAL_FAILURE(directory_test{}.create_and_remove());
}

TEST_P(MySeastarParamFixture, TestGetTestDirectory) {
    ASSERT_NO_FATAL_FAILURE(directory_test{}.create_and_remove());
}
