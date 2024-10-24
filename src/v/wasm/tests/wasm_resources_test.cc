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

#include "wasm/errc.h"
#include "wasm/tests/wasm_fixture.h"

#include <seastar/core/reactor.hh>

#include <gtest/gtest.h>

using namespace std::chrono_literals;

TEST_F(WasmTestFixture, MemoryIsLimited) {
    load_wasm("dynamic");
    EXPECT_NO_THROW(execute_command("allocate", 5_KiB));
    EXPECT_THROW(execute_command("allocate", MAX_MEMORY), wasm::wasm_exception);
}

TEST_F(WasmTestFixture, CpuIsLimited) {
    load_wasm("dynamic");
    // In reality, we don't want this to be over a few milliseconds, but in an
    // effort to prevent test flakiness on hosts with lots of concurrent tests
    // on shared vCPUs, we make this much higher.
    ss::engine().update_blocked_reactor_notify_ms(50ms);
    bool stalled = false;
    ss::reactor::test::set_stall_detector_report_function(
      [&stalled] { stalled = true; });
    EXPECT_THROW(execute_command("loop", 0), wasm::wasm_exception);
    EXPECT_FALSE(stalled);
}
