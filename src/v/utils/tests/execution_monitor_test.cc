// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/execution_monitor.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timer.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <chrono>

using namespace std::chrono_literals;
namespace ss = seastar;

SEASTAR_THREAD_TEST_CASE(execution_monitor_happy_path) {
    // This test verifies that the execution monitor doesn't
    // detect stalls when the callsites are checked properly.
    ssx::execution_monitor monitor("test_monitor", 50ms);

    ss::abort_source as;
    retry_chain_context callsite1("test_callsite1", as, 0x100);
    retry_chain_context callsite2("test_callsite2", as, 0x100);
    retry_chain_context callsite3("test_callsite3", as, 0x100);
    ss::gate gate;

    auto stall_cfg = ssx::execution_monitor::stall_detector_config{
      .cb =
        [](retry_chain_context& ctx) {
            // Stall callback
            BOOST_FAIL("Stall detected at callsite: " + ctx.name());
        },
      .contexts = {&callsite1, &callsite2, &callsite3}};
    auto exit_cfg = ssx::execution_monitor::unexpected_shutdown_detector_config{
      .cb =
        []() {
            // Stall callback
            BOOST_FAIL("Unexpected exit");
        },
      .gate = &gate,
    };

    auto h = gate.hold();
    // Start the monitor with a stall interval of 10ms
    monitor.start(stall_cfg, exit_cfg);

    // Run for two stall intervals (100ms)
    for (int i = 0; i < 10; i++) {
        // Single callsite should be able to prevent the stall from
        // being detected.
        callsite1.reset();
        ss::sleep(10ms).get();
    }

    // Stop the monitor
    monitor.stop();
}

SEASTAR_THREAD_TEST_CASE(execution_monitor_triggered_stall) {
    // This test verifies that the execution monitor detects stalls
    // when they happen.
    ssx::execution_monitor monitor("test_monitor", 50ms);

    ss::abort_source as;
    retry_chain_context callsite1("test_callsite1", as, 0x100);
    retry_chain_context callsite2("test_callsite2", as, 0x100);
    retry_chain_context callsite3("test_callsite3", as, 0x100);
    ss::gate gate;

    bool stall_detected = true;
    auto stall_cfg = ssx::execution_monitor::stall_detector_config{
      .cb =
        [&](retry_chain_context& ctx) {
            // Stall callback
            stall_detected = true;
            BOOST_REQUIRE(ctx.name() == "test_callsite2");
        },
      .contexts = {&callsite1, &callsite2, &callsite3}};
    auto exit_cfg = ssx::execution_monitor::unexpected_shutdown_detector_config{
      .cb =
        []() {
            // Stall callback
            BOOST_FAIL("Unexpected exit");
        },
      .gate = &gate,
    };

    auto h = gate.hold();
    monitor.start(stall_cfg, exit_cfg);

    // Checkpoint callsite2 and then check that it is passed into the stall
    // callback.
    callsite2.reset();
    ss::sleep(120ms).get();

    BOOST_REQUIRE(stall_detected);

    // Stop the monitor
    monitor.stop();
}

SEASTAR_THREAD_TEST_CASE(execution_monitor_trigger_unexpected_shutdown) {
    // This test verifies that the execution monitor can detect unexpected
    // shutdown. The shutdown is represented by a callsite. But the difference
    // is that the shutdown callsite shouldn't be reached unless the shutdown
    // was announced by to monitor.

    ssx::execution_monitor monitor("test_monitor", 50ms);

    ss::abort_source as;
    ss::gate gate;

    auto stall_cfg = ssx::execution_monitor::stall_detector_config{
      .cb =
        [](retry_chain_context& ctx) {
            // Stall callback
            BOOST_FAIL("Stall detected at callsite: " + ctx.name());
        },
      .contexts = {}};
    bool shutdown_detected = false;
    auto exit_cfg = ssx::execution_monitor::unexpected_shutdown_detector_config{
      .cb = [&]() { shutdown_detected = true; },
      .gate = &gate,
    };

    monitor.start(stall_cfg, exit_cfg);

    // The gate is not held
    ss::sleep(90ms).get();

    BOOST_REQUIRE(shutdown_detected);

    // Stop the monitor
    monitor.stop();
}

SEASTAR_THREAD_TEST_CASE(execution_monitor_suspend_no_detection) {
    // This test verifies that the execution monitor does not
    // detect stalls when the callsites are suspended.
    ssx::execution_monitor monitor("test_monitor", 50ms);

    ss::abort_source as;
    retry_chain_context callsite1("test_callsite1", as, 0x100);
    retry_chain_context callsite2("test_callsite2", as, 0x100);
    retry_chain_context callsite3("test_callsite3", as, 0x100);

    auto stall_cfg = ssx::execution_monitor::stall_detector_config{
      .cb =
        [](retry_chain_context& ctx) {
            // Stall callback
            BOOST_FAIL("Stall detected at callsite: " + ctx.name());
        },
      .contexts = {&callsite1, &callsite2, &callsite3}};

    monitor.start(stall_cfg, std::nullopt);

    callsite2.reset();
    callsite2.suspend_to(ss::lowres_clock::now() + 200ms);
    ss::sleep(100ms).get();

    // Stop the monitor
    monitor.stop();
}

SEASTAR_THREAD_TEST_CASE(execution_monitor_suspend_detection) {
    // This test verifies that the execution monitor detects
    // stalls when the callsites are suspended.
    ssx::execution_monitor monitor("test_monitor", 50ms);

    ss::abort_source as;
    retry_chain_context callsite1("test_callsite1", as, 0x100);
    retry_chain_context callsite2("test_callsite2", as, 0x100);

    bool stall_detected = false;
    auto stall_cfg = ssx::execution_monitor::stall_detector_config{
      .cb =
        [&](retry_chain_context& ctx) {
            stall_detected = true;
            BOOST_REQUIRE(ctx.name() == "test_callsite2");
        },
      .contexts = {&callsite1, &callsite2}};

    monitor.start(stall_cfg, std::nullopt);

    callsite1.reset();
    ss::sleep(10ms).get();
    callsite2.reset();
    callsite2.suspend_to(ss::lowres_clock::now() + 30ms);
    ss::sleep(100ms).get();

    // Stop the monitor
    monitor.stop();
}

SEASTAR_THREAD_TEST_CASE(execution_monitor_expected_shutdown) {
    // This test verifies that the execution monitor can tolerate
    // shutdown if it was properly announced.
    ssx::execution_monitor monitor("test_monitor", 50ms);

    ss::abort_source as;
    ss::gate gate;

    auto exit_cfg = ssx::execution_monitor::unexpected_shutdown_detector_config{
      .cb = [&]() { BOOST_FAIL("Shutdown detected"); },
      .gate = &gate,
    };

    monitor.start(std::nullopt, exit_cfg);

    monitor.expect_shutdown();
    // The gate is not held
    ss::sleep(90ms).get();

    // Stop the monitor
    monitor.stop();
}
