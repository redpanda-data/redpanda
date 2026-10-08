/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "test_utils/reactor_stall_probe.h"
#include "test_utils/test.h"

#include <fmt/format.h>

// Exercises the probe end-to-end so it is always compiled. It deliberately
// asserts nothing: the observed stalls depend on the machine and scheduling, so
// there is nothing meaningful to check.
TEST_CORO(reactor_stall_probe, start_stop_report) {
    tests::reactor_stall_probe probe;
    probe.start();
    auto stats = co_await probe.stop();
    fmt::print("reactor stalls: {}\n", stats);
}
