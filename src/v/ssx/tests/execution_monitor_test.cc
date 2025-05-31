// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/execution_monitor.h"

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

    ssx::execution_monitor_callsite<> callsite1("test_callsite1");
    ssx::execution_monitor_callsite<> callsite2("test_callsite2");
    ssx::execution_monitor_callsite<> callsite3("test_callsite3");
    ssx::execution_monitor_shutdown_callsite<> shutdown_callsite{
      .name = "test_callsite2"};

    // Start the monitor with a stall interval of 10ms
    monitor.start(
      [](ssx::execution_monitor_callsite<>& cs) {
          // Stall callback
          BOOST_FAIL("Stall detected at callsite: " + cs.name);
      },
      [](ssx::execution_monitor_shutdown_callsite<>& cs) {
          // Shutdown callback
          BOOST_FAIL("Unexpected shutdown at callsite: " + cs.name);
      },
      std::make_tuple(
        std::ref(callsite1), std::ref(callsite2), std::ref(callsite3)),
      std::make_tuple(std::ref(shutdown_callsite)));

    // Run for two stall intervals (100ms)
    for (int i = 0; i < 10; i++) {
        // Single callsite should be able to prevent the stall from
        // being detected.
        callsite1.checkpoint();
        ss::sleep(10ms).get();
    }

    // Stop the monitor
    monitor.stop().get();
}

SEASTAR_THREAD_TEST_CASE(execution_monitor_triggered_stall) {
    // This test verifies that the execution monitor detects stalls
    // when they happen.
    ssx::execution_monitor monitor("test_monitor", 50ms);

    ssx::execution_monitor_callsite<> callsite1("test_callsite1");
    ssx::execution_monitor_callsite<> callsite2("test_callsite2");
    ssx::execution_monitor_callsite<> callsite3("test_callsite3");

    // Start the monitor with a stall interval of 10ms
    bool stall_detected = false;
    monitor.start(
      [&](ssx::execution_monitor_callsite<>& cs) {
          stall_detected = true;
          BOOST_REQUIRE(cs.name == "test_callsite2");
      },
      [](ssx::execution_monitor_shutdown_callsite<>& cs) {
          // Shutdown callback
          BOOST_FAIL("Unexpected shutdown at callsite: " + cs.name);
      },
      std::make_tuple(
        std::ref(callsite1), std::ref(callsite2), std::ref(callsite3)),
      std::make_tuple());

    // Checkpoint callsite2 and then check that it is passed into the stall
    // callback.
    callsite2.checkpoint();
    ss::sleep(120ms).get();

    BOOST_REQUIRE(stall_detected);

    // Stop the monitor
    monitor.stop().get();
}

SEASTAR_THREAD_TEST_CASE(execution_monitor_trigger_unexpected_shutdown) {
    // This test verifies that the execution monitor can detect unexpected
    // shutdown. The shutdown is represented by a callsite. But the difference
    // is that the shutdown callsite shouldn't be reached unless the shutdown
    // was announced by to monitor.
    ssx::execution_monitor monitor("test_monitor", 50ms);

    ssx::execution_monitor_shutdown_callsite<> shutdown_callsite{
      .name = "test_callsite"};

    // Start the monitor with a stall interval of 10ms
    bool shutdown_detected = false;
    monitor.start(
      [](ssx::execution_monitor_callsite<>& cs) {
          // Stall callback
          BOOST_FAIL("Stall detected at callsite: " + cs.name);
      },
      [&](ssx::execution_monitor_shutdown_callsite<>& cs) {
          shutdown_detected = true;
          BOOST_REQUIRE(cs.name == "test_callsite");
      },
      std::make_tuple(),
      std::make_tuple(std::ref(shutdown_callsite)));

    shutdown_callsite.checkpoint();
    ss::sleep(60ms).get();
    BOOST_REQUIRE(shutdown_detected);

    // Stop the monitor
    monitor.stop().get();
}

SEASTAR_THREAD_TEST_CASE(execution_monitor_suspend_no_detection) {
    // This test verifies that the execution monitor does not
    // detect stalls when the callsites are suspended.
    ssx::execution_monitor monitor("test_monitor", 10ms);

    ssx::execution_monitor_callsite<> callsite1("test_callsite1");
    ssx::execution_monitor_callsite<> callsite2("test_callsite2");

    // Start the monitor with a stall interval of 10ms
    monitor.start(
      [](ssx::execution_monitor_callsite<>& cs) {
          // Stall callback
          BOOST_FAIL("Stall detected at callsite: " + cs.name);
      },
      [](ssx::execution_monitor_shutdown_callsite<>& cs) {
          // Shutdown callback
          BOOST_FAIL("Unexpected shutdown at callsite: " + cs.name);
      },
      std::make_tuple(std::ref(callsite1), std::ref(callsite2)),
      std::make_tuple());

    callsite2.checkpoint();
    callsite2.suspend(200ms);
    ss::sleep(100ms).get();

    // Stop the monitor
    monitor.stop().get();
}

SEASTAR_THREAD_TEST_CASE(execution_monitor_suspend_detection) {
    // This test verifies that the execution monitor detects
    // stalls when the callsites are suspended.
    ssx::execution_monitor monitor("test_monitor", 50ms);

    ssx::execution_monitor_callsite<> callsite1("test_callsite1");
    ssx::execution_monitor_callsite<> callsite2("test_callsite2");

    bool stall_detected = false;
    monitor.start(
      [&](ssx::execution_monitor_callsite<>& cs) {
          // Stall callback
          stall_detected = true;
          BOOST_REQUIRE(cs.name == "test_callsite2");
      },
      [](ssx::execution_monitor_shutdown_callsite<>& cs) {
          // Shutdown callback
          BOOST_FAIL("Unexpected shutdown at callsite: " + cs.name);
      },
      std::make_tuple(std::ref(callsite1), std::ref(callsite2)),
      std::make_tuple());

    callsite1.checkpoint();
    ss::sleep(10ms).get();
    callsite2.checkpoint();
    callsite2.suspend(20ms);
    ss::sleep(100ms).get();

    // Stop the monitor
    monitor.stop().get();
}

SEASTAR_THREAD_TEST_CASE(execution_monitor_expected_shutdown) {
    // This test verifies that the execution monitor can tolerate
    // shutdown if it was properly announced.
    ssx::execution_monitor monitor("test_monitor", 50ms);

    ssx::execution_monitor_shutdown_callsite<> shutdown_callsite{
      .name = "test_callsite"};

    monitor.start(
      [](ssx::execution_monitor_callsite<>& cs) {
          // Stall callback
          BOOST_FAIL("Stall detected at callsite: " + cs.name);
      },
      [](ssx::execution_monitor_shutdown_callsite<>& cs) {
          BOOST_FAIL("Shutdown detected at callsite: " + cs.name);
      },
      std::make_tuple(),
      std::make_tuple(std::ref(shutdown_callsite)));

    // Check that the stall is not detected if there are no callsites.
    ss::sleep(100ms).get();
    monitor.expect_shutdown();
    shutdown_callsite.checkpoint();
    ss::sleep(100ms).get();

    // Stop the monitor
    monitor.stop().get();
}

SEASTAR_THREAD_TEST_CASE(callsite_formatting) {
    // The callsite can store some context information in the text
    // form. The context is reset after every checkpoint.
    // This test verifies that the formatting works as expected.
    ssx::execution_monitor_callsite<> callsite("test_callsite");

    callsite.checkpoint();
    callsite.log("test {}", 42);
    BOOST_REQUIRE_EQUAL(callsite.str(), "[test_callsite|1|0] test 42\n");
    callsite.checkpoint();
    BOOST_REQUIRE(callsite.str().empty());

    // Check that the messages can be stacked
    callsite.log("test {}", 42);
    callsite.log("test {}", 42);
    BOOST_REQUIRE(
      callsite.str()
      == "[test_callsite|2|0] test 42\n[test_callsite|2|0] test 42\n");
}
