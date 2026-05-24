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
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/later.hh>

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

// The tests below construct a real reservation_policy through the scheduler
// shell to exercise the integration: factory wiring, has_waiters
// aggregation, the stop()-then-admit drain, and that the configured
// per-group reservation flows through to admission behavior. Policy
// internals are exercised by reservation_policy_test.cc.

TEST_CORO(scheduler, ReservationHasWaitersReflectsQueueState) {
    scheduler s{1, scheduler_config{.policy = policy_type::reservation}};
    EXPECT_FALSE(s.has_waiters());

    ss::abort_source as;
    co_await s.admit(group_id::default_group, as);
    EXPECT_FALSE(s.has_waiters());

    auto queued = s.admit(group_id::default_group, as);
    co_await ss::yield();
    EXPECT_TRUE(s.has_waiters());

    s.release(group_id::default_group);
    co_await std::move(queued);
    EXPECT_FALSE(s.has_waiters());

    s.release(group_id::default_group);
    co_await s.stop();
}

TEST_CORO(scheduler, ReservationStopDrainsAndRejectsAdmits) {
    scheduler s{1, scheduler_config{.policy = policy_type::reservation}};

    ss::abort_source as;
    co_await s.admit(group_id::default_group, as);

    auto queued = s.admit(group_id::default_group, as);
    co_await ss::yield();

    co_await s.stop();

    auto r = co_await ss::coroutine::as_future(std::move(queued));
    EXPECT_TRUE(r.failed());
    r.ignore_ready_future();

    EXPECT_FALSE(s.try_admit(group_id::default_group));
    auto r2 = co_await ss::coroutine::as_future(
      s.admit(group_id::default_group, as));
    EXPECT_TRUE(r2.failed());
    r2.ignore_ready_future();

    s.release(group_id::default_group);
}

TEST_CORO(scheduler, ReservationReservationsRespectConfiguredTargets) {
    // capacity=4, producer_upload reserved=2 → shared=2. Once pu is
    // active, the shared pool is exhausted by default_group, but
    // producer_upload can still admit twice from its dedicated lane.
    scheduler s{
      4,
      scheduler_config{
        .policy = policy_type::reservation,
        .reservation = reservation_policy_config{
          {group_id::producer_upload, 2}}}};

    // Warm pu so it becomes effective-active and its lane is protected
    // from reclaim. The first admit drains pu's untouched lane through
    // the common pool; the matching releases refill the lane back to
    // target via the under-target refill path.
    ss::abort_source as;
    co_await s.admit(group_id::producer_upload, as);
    co_await s.admit(group_id::producer_upload, as);
    s.release(group_id::producer_upload);
    s.release(group_id::producer_upload);

    EXPECT_TRUE(s.try_admit(group_id::default_group));
    EXPECT_TRUE(s.try_admit(group_id::default_group));
    EXPECT_FALSE(s.try_admit(group_id::default_group));

    EXPECT_TRUE(s.try_admit(group_id::producer_upload));
    EXPECT_TRUE(s.try_admit(group_id::producer_upload));
    EXPECT_FALSE(s.try_admit(group_id::producer_upload));

    s.release(group_id::default_group);
    s.release(group_id::default_group);
    s.release(group_id::producer_upload);
    s.release(group_id::producer_upload);
    co_await s.stop();
}
