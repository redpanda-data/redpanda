// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/watchdog.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/manual_clock.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

using namespace std::chrono_literals;

SEASTAR_THREAD_TEST_CASE(watchdog_test_defuse) {
    bool watchdog_triggered = false;
    {
        ssx::watchdog wd(100ms, [&] { watchdog_triggered = true; });
        // to allow some async code to run
        ss::sleep(1ms).get();
    }
    ss::sleep(1ms).get();
    BOOST_REQUIRE(watchdog_triggered == false);
    // watchdog timeout passed
    ss::sleep(200ms).get();
    BOOST_REQUIRE(watchdog_triggered == false);
}

SEASTAR_THREAD_TEST_CASE(watchdog_test_trigger) {
    bool watchdog_triggered = false;
    {
        ssx::watchdog wd(100ms, [&] { watchdog_triggered = true; });
        ss::sleep(200ms).get();
    }
    BOOST_REQUIRE(watchdog_triggered == true);
}

SEASTAR_THREAD_TEST_CASE(watchdog_test_backoff) {
    size_t watchdog_triggered = 0;
    {
        ssx::watchdog wd(10ms, [&] { ++watchdog_triggered; }, 2);
        // 10ms + 20ms (30ms) + 40ms (70ms) + 80ms (150ms). The next timeout
        // would be 160ms (310ms) but the watchdog is destroyed before that.
        ss::sleep(200ms).get();
    }
    // Wait some additional time so that we can check that no more triggers
    // happened after the watchdog is destroyed.
    ss::sleep(50ms).get();

    BOOST_TEST_REQUIRE(watchdog_triggered == 4);
}

SEASTAR_THREAD_TEST_CASE(watchdog_test_backoff_with_max_timeout) {
    size_t watchdog_triggered = 0;
    {
        ssx::watchdog wd(10ms, [&] { ++watchdog_triggered; }, 2, 50ms);
        // 10ms + 20ms (30ms) + 40ms (70ms) + 50ms (120ms) + 50ms (170ms)
        // The next timeout would be (220ms) but the watchdog is destroyed
        // before that.
        ss::sleep(200ms).get();
    }
    // Wait some additional time so that we can check that no more triggers
    // happened after the watchdog is destroyed.
    ss::sleep(50ms).get();

    BOOST_TEST_REQUIRE(watchdog_triggered == 5);
}

SEASTAR_THREAD_TEST_CASE(watchdog_test_backoff_defuse) {
    size_t watchdog_triggered = 0;
    {
        ssx::watchdog wd(10ms, [&] { ++watchdog_triggered; }, 1.0);
        // Allow some async code to run.
        ss::sleep(1ms).get();
    }

    // Watchdog timeout and backoffs passed.
    ss::sleep(50ms).get();

    BOOST_TEST_REQUIRE(watchdog_triggered == 0);
}
