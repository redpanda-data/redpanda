/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/level_one/metastore/metastore.h"
#include "cloud_topics/level_one/metastore/retry.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/future.hh>
#include <seastar/coroutine/exception.hh>

#include <gtest/gtest.h>

#include <cerrno>
#include <expected>
#include <system_error>

using namespace cloud_topics::l1;

namespace {

using int_result = std::expected<int, metastore::errc>;

TEST(retry_metastore_op_test, returns_value_without_retry) {
    int calls = 0;
    auto func = [&calls]() -> ss::future<int_result> {
        ++calls;
        co_return int_result{7};
    };
    ss::abort_source as;
    auto rtc = make_default_metastore_rtc(as);
    auto result = retry_metastore_op(func, rtc).get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), 7);
    EXPECT_EQ(calls, 1);
}

// A thrown connection abort from the metastore op must be retried, not
// propagated. In the fetch path an escaped exception becomes
// not_leader_for_partition, which traps consumers in a retry loop.
TEST(retry_metastore_op_test, retries_thrown_connection_abort) {
    int calls = 0;
    auto func = [&calls]() -> ss::future<int_result> {
        ++calls;
        if (calls == 1) {
            throw std::system_error(ECONNABORTED, std::system_category());
        }
        co_return int_result{42};
    };
    ss::abort_source as;
    auto rtc = make_default_metastore_rtc(as);
    auto result = retry_metastore_op(func, rtc).get();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), 42);
    EXPECT_EQ(calls, 2);
}

// Shutdown exceptions must propagate, not be retried/swallowed.
TEST(retry_metastore_op_test, propagates_shutdown_exception) {
    int calls = 0;
    auto func = [&calls]() -> ss::future<int_result> {
        ++calls;
        co_await ss::coroutine::return_exception(
          ss::abort_requested_exception());
        co_return int_result{0};
    };
    ss::abort_source as;
    auto rtc = make_default_metastore_rtc(as);
    EXPECT_THROW(
      retry_metastore_op(func, rtc).get(), ss::abort_requested_exception);
    EXPECT_EQ(calls, 1);
}

} // namespace
