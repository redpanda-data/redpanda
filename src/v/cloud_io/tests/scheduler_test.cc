/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_io/scheduler.h"
#include "test_utils/test.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/coroutine.hh>

using namespace cloud_io;

TEST_CORO(scheduler, PassthroughAdmitAlwaysSucceeds) {
    scheduler s{2};
    EXPECT_EQ(s.total_capacity(), 2u);

    ss::abort_source as;
    co_await s.admit(group_id::producer_upload, as);
    co_await s.admit(group_id::consumer_fetch, as);
    co_await s.admit(group_id::default_group, as);

    co_await s.stop();
}

TEST_CORO(scheduler, PassthroughTryAdmitAlwaysSucceeds) {
    scheduler s{1};

    EXPECT_TRUE(s.try_admit(group_id::default_group));
    EXPECT_TRUE(s.try_admit(group_id::consumer_fetch));

    co_await s.stop();
}
