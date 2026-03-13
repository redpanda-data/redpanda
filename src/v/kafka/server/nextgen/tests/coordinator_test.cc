// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/errors.h"
#include "kafka/server/nextgen/coordinator.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

using kafka::error_code;
using kafka::nextgen::coordinator;

// A heartbeat with member_epoch == -1 (join sentinel) must assign a fresh
// member_id and return epoch 0, entering RECONCILING state.
TEST_CORO(coordinator_test, new_member_registration) {
    coordinator coord;
    auto r = co_await coord.heartbeat("g1", "", -1);
    ASSERT_EQ_CORO(r.ec, error_code::none);
    ASSERT_FALSE_CORO(r.member_id.empty());
    ASSERT_EQ_CORO(r.member_epoch, 0);
    co_await coord.stop();
}

// A second heartbeat with the assigned member_id and epoch 0 must return
// epoch 1, advancing the member toward STABLE.
TEST_CORO(coordinator_test, epoch_advancement) {
    coordinator coord;
    auto r0 = co_await coord.heartbeat("g1", "", -1);
    ASSERT_EQ_CORO(r0.ec, error_code::none);

    auto r1 = co_await coord.heartbeat("g1", r0.member_id, 0);
    ASSERT_EQ_CORO(r1.ec, error_code::none);
    ASSERT_EQ_CORO(r1.member_epoch, 1);
    co_await coord.stop();
}

// A heartbeat with a stale member_epoch must be fenced.
TEST_CORO(coordinator_test, epoch_fencing) {
    coordinator coord;
    auto r0 = co_await coord.heartbeat("g1", "", -1);
    ASSERT_EQ_CORO(r0.ec, error_code::none);

    auto r1 = co_await coord.heartbeat("g1", r0.member_id, 99);
    ASSERT_EQ_CORO(r1.ec, error_code::fenced_member_epoch);
    co_await coord.stop();
}

// A heartbeat with an unknown member_id must be rejected.
TEST_CORO(coordinator_test, unknown_member) {
    coordinator coord;
    auto r = co_await coord.heartbeat("g1", "no-such-member", 0);
    ASSERT_EQ_CORO(r.ec, error_code::unknown_member_id);
    co_await coord.stop();
}

// Verifies that all fields required by the heartbeat handler are populated
// correctly across a join and subsequent epoch advancement.
TEST_CORO(coordinator_test, handler_path_fields) {
    coordinator coord;

    auto r0 = co_await coord.heartbeat("g1", "", -1);
    ASSERT_EQ_CORO(r0.ec, error_code::none);
    ASSERT_FALSE_CORO(r0.member_id.empty());
    ASSERT_EQ_CORO(r0.member_epoch, 0);

    auto r1 = co_await coord.heartbeat("g1", r0.member_id, 0);
    ASSERT_EQ_CORO(r1.ec, error_code::none);
    ASSERT_FALSE_CORO(r1.member_id.empty());
    ASSERT_EQ_CORO(r1.member_epoch, 1);

    auto r2 = co_await coord.heartbeat("g1", r0.member_id, 99);
    ASSERT_EQ_CORO(r2.ec, error_code::fenced_member_epoch);

    co_await coord.stop();
}
