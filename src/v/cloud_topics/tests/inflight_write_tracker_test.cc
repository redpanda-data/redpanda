/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_topics/inflight_write_tracker.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/coroutine/as_future.hh>

using namespace std::chrono_literals;
using seastar::sleep;

TEST_CORO(inflight_write_tracker_test, drain_blocks_until_all_complete) {
    auto tracker = cloud_topics::inflight_write_tracker::make_default();
    co_await tracker->start();

    auto token1 = tracker->track();
    auto token2 = tracker->track();

    bool drain_done = false;
    auto drain_fut = tracker->drain().then(
      [&drain_done] { drain_done = true; });

    co_await sleep(10ms);
    ASSERT_FALSE_CORO(drain_done);

    token1->done.set_value();
    co_await sleep(10ms);
    ASSERT_FALSE_CORO(drain_done);

    token2->done.set_value();
    co_await std::move(drain_fut);
    ASSERT_TRUE_CORO(drain_done);

    co_await tracker->stop();
}

TEST_CORO(inflight_write_tracker_test, drain_fails_on_broken_promise) {
    auto tracker = cloud_topics::inflight_write_tracker::make_default();
    co_await tracker->start();

    auto token1 = tracker->track();
    auto token2 = tracker->track();

    auto drain_fut = tracker->drain();

    token1->done.set_value();
    token2.reset(); // destroy without setting promise -> broken_promise

    auto res = co_await ss::coroutine::as_future(std::move(drain_fut));
    ASSERT_TRUE_CORO(res.failed());
    res.ignore_ready_future();

    co_await tracker->stop();
}

TEST_CORO(inflight_write_tracker_test, drain_empty) {
    auto tracker = cloud_topics::inflight_write_tracker::make_default();
    co_await tracker->start();
    co_await tracker->drain();
    co_await tracker->stop();
}
