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

#include "cluster/crash_reporter.h"
#include "storage/tests/kvstore_fixture.h"

#include <seastar/util/defer.hh>

#include <gtest/gtest.h>

#include <chrono>

using namespace std::chrono_literals;

TEST_F(kvstore_test_fixture, rate_limiter_test) {
    auto kvstore = make_kvstore();
    kvstore->start().get();
    auto defer = ss::defer([&] { kvstore->stop().get(); });

    cluster::crash_reporter::rate_limiter rl{*kvstore};

    // Initially there is no rate limit
    auto wt = rl.wait_time();
    EXPECT_EQ(wt, 0s);

    rl.record().get();

    // After calling record, there is a rate limit
    wt = rl.wait_time();
    EXPECT_GT(wt, 0s);
    EXPECT_LE(wt, cluster::crash_reporter::rate_limiter::upload_rate);
}
