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
#include "test_utils/test.h"

#include <seastar/core/sleep.hh>

/*
 * TEST_(ASYNC|CORO)() is the Seastar-enabled equivalent of TEST().
 */
TEST_ASYNC(SeastarTest, Sleep) {
    seastar::sleep(std::chrono::milliseconds(100)).get();
}

TEST_CORO(SeastarTest, SleepCoro) {
    co_await seastar::sleep(std::chrono::milliseconds(100));
    ASSERT_EQ_CORO(100, 100);
}

/*
 * TEST_F_(ASYNC|CORO)() is the Seastar-enabled equivalent of TEST_F()
 */
struct MySeastarFixture : public seastar_test {
    std::string_view message() const { return "hello"; }

    ~MySeastarFixture() override {
        assert(setup_called);
        assert(teardown_called);
    }

    void SetUpAsync() override {
        seastar::sleep(std::chrono::milliseconds(100)).get();
        setup_called = true;
    }

    void TearDownAsync() override {
        seastar::sleep(std::chrono::milliseconds(100)).get();
        teardown_called = true;
    }

private:
    bool setup_called{false};
    bool teardown_called{false};
};

TEST_F_ASYNC(MySeastarFixture, Sleep) {
    seastar::sleep(std::chrono::milliseconds(100)).get();
    ASSERT_EQ(message(), "hello");
}

TEST_F_CORO(MySeastarFixture, SleepCoro) {
    co_await seastar::sleep(std::chrono::milliseconds(100));
    ASSERT_EQ_CORO(message(), "hello");
}

/*
 * TEST_P_(ASYNC|CORO)() is the Seastar-enabled equivalent of TEST_P()
 */
class MySeastarParamFixture
  : public MySeastarFixture
  , public ::testing::WithParamInterface<int> {};

TEST_P_ASYNC(MySeastarParamFixture, Sleep) {
    seastar::sleep(std::chrono::milliseconds(GetParam() * 10)).get();
    ASSERT_EQ(GetParam() % 11, 0);
}

TEST_P_CORO(MySeastarParamFixture, SleepCoro) {
    co_await seastar::sleep(std::chrono::milliseconds(100));
    ASSERT_TRUE_CORO(true);
    ASSERT_FALSE_CORO(false);
}

INSTANTIATE_TEST_SUITE_P(
  Divisible, MySeastarParamFixture, testing::Values(11, 22, 33));

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
