/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/seastarx.h"
#include "cloud_io/io_resources.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/scheduling.hh>
#include <seastar/coroutine/as_future.hh>

TEST_CORO(io_resources, effectively_unlimited_bw_does_not_throw) {
    auto sg = co_await ss::create_scheduling_group("cloud_io_test", 100);

    // Passing the "effectively unlimited" bandwidth must be a valid way to
    // disable throttling. Regression test for scylladb/seastar#3370.
    auto fut = co_await ss::coroutine::as_future(
      sg.update_io_bandwidth(cloud_io::io_resources::effectively_unlimited_bw));

    co_await ss::destroy_scheduling_group(sg);

    EXPECT_FALSE(fut.failed()) << fmt::format("e: {}", fut.get_exception());
    if (fut.failed()) {
        std::rethrow_exception(fut.get_exception());
    }
}
