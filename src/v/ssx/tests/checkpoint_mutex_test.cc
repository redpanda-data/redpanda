// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/checkpoint_mutex.h"

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

SEASTAR_THREAD_TEST_CASE(checkpoint_mutex_lock) {
    ssx::checkpoint_mutex mutex("test_checkpoint_mutex");
    BOOST_REQUIRE(mutex.has_units() == true);
    BOOST_REQUIRE_EQUAL(mutex.get_blocking_checkpoint().name, "unavailable");
    BOOST_REQUIRE(
      mutex.get_blocking_checkpoint().time
      == ss::steady_clock_type::time_point::min());

    // Acquire the mutex normally.
    auto units = ssx::get_units_with_checkpoint(mutex, "test_checkpoint").get();
    BOOST_REQUIRE(mutex.has_units() == false);
    BOOST_REQUIRE_EQUAL(
      mutex.get_blocking_checkpoint().name, "test_checkpoint");

    auto fut = ssx::get_units_with_checkpoint(
      mutex, "test_checkpoint2"); // async
    BOOST_REQUIRE(fut.available() == false);

    // We have another waiter so the mutex should not be available.
    units.release();
    BOOST_REQUIRE(mutex.has_units() == false);

    // Another waiter should be able to acquire the mutex.
    auto units2 = std::move(fut).get();
    BOOST_REQUIRE_EQUAL(
      mutex.get_blocking_checkpoint().name, "test_checkpoint2");

    units2.release();
    BOOST_REQUIRE(mutex.has_units() == true);
}

SEASTAR_THREAD_TEST_CASE(checkpoint_mutex_broken) {
    ssx::checkpoint_mutex mutex("test_checkpoint_mutex");
    BOOST_REQUIRE(mutex.has_units() == true);
    BOOST_REQUIRE_EQUAL(mutex.get_blocking_checkpoint().name, "unavailable");

    // Acquire the mutex normally.
    auto units
      = ssx::get_units_with_checkpoint(mutex, "test_checkpoint1").get();
    BOOST_REQUIRE(mutex.has_units() == false);
    BOOST_REQUIRE_EQUAL(
      mutex.get_blocking_checkpoint().name, "test_checkpoint1");

    auto units2 = ssx::get_units_with_checkpoint(
      mutex, "test_checkpoint2"); // async

    // Break the mutex.
    mutex.broken();
    BOOST_REQUIRE(mutex.has_units() == false);

    bool thrown = false;
    try {
        std::move(units2).get();
    } catch (const ss::broken_semaphore& e) {
        ss::sstring msg = e.what();
        // Message should contain the name of the checkpoint that held
        // the units when the semaphore was broken.
        BOOST_REQUIRE(msg.find("test_checkpoint1") != ss::sstring::npos);
        thrown = true;
    }
    BOOST_REQUIRE(thrown);
}

SEASTAR_THREAD_TEST_CASE(checkpoint_mutex_aborted) {
    ssx::checkpoint_mutex mutex("test_checkpoint_mutex");
    BOOST_REQUIRE(mutex.has_units() == true);
    BOOST_REQUIRE_EQUAL(mutex.get_blocking_checkpoint().name, "unavailable");

    // Acquire the mutex normally.
    auto units
      = ssx::get_units_with_checkpoint(mutex, "test_checkpoint1").get();
    BOOST_REQUIRE(mutex.has_units() == false);
    BOOST_REQUIRE_EQUAL(
      mutex.get_blocking_checkpoint().name, "test_checkpoint1");

    ss::abort_source as;
    auto units2 = ssx::get_units_with_checkpoint(
      mutex, as, "test_checkpoint2"); // async

    // Invoke the abort source to simulate an aborted operation.
    as.request_abort();
    BOOST_REQUIRE(mutex.has_units() == false);

    bool thrown = false;
    try {
        std::move(units2).get();
    } catch (const ss::semaphore_aborted& e) {
        ss::sstring msg = e.what();
        // Message should contain the name of the checkpoint that held
        // the units
        BOOST_REQUIRE(msg.find("test_checkpoint1") != ss::sstring::npos);
        thrown = true;
    }
    BOOST_REQUIRE(thrown);

    thrown = false;

    // Check that this works even if we're called with the source which is
    // already aborted.
    auto units3 = ssx::get_units_with_checkpoint(
      mutex, as, "test_checkpoint3"); // async

    BOOST_REQUIRE(mutex.has_units() == false);

    try {
        std::move(units3).get();
    } catch (const ss::semaphore_aborted& e) {
        ss::sstring msg = e.what();
        BOOST_REQUIRE(msg.find("test_checkpoint1") != ss::sstring::npos);
        thrown = true;
    }
    BOOST_REQUIRE(thrown);
}

SEASTAR_THREAD_TEST_CASE(checkpoint_mutex_timed_out) {
    ssx::checkpoint_mutex mutex("test_checkpoint_mutex");
    BOOST_REQUIRE(mutex.has_units() == true);
    BOOST_REQUIRE_EQUAL(mutex.get_blocking_checkpoint().name, "unavailable");

    // Acquire the mutex normally.
    auto units
      = ssx::get_units_with_checkpoint(mutex, "test_checkpoint1").get();
    BOOST_REQUIRE(mutex.has_units() == false);
    BOOST_REQUIRE_EQUAL(
      mutex.get_blocking_checkpoint().name, "test_checkpoint1");

    // Check with the deadline

    auto deadline = std::chrono::steady_clock::now() + 1ms;
    auto units2 = ssx::get_units_with_checkpoint(
      mutex, deadline, "test_checkpoint2"); // async

    // Sleep long enough to ensure that the timeout occurs.
    ss::sleep(100ms).get();
    BOOST_REQUIRE(mutex.has_units() == false);

    bool thrown = false;
    try {
        std::move(units2).get();
    } catch (const ss::semaphore_timed_out& e) {
        ss::sstring msg = e.what();
        BOOST_REQUIRE(msg.find("test_checkpoint1") != ss::sstring::npos);
        thrown = true;
    }
    BOOST_REQUIRE(thrown);

    // Check with the timeout
    thrown = false;
    auto units3 = ssx::get_units_with_checkpoint(
      mutex, 10ms, "test_checkpoint2"); // async

    // Sleep long enough to ensure that the timeout occurs.
    ss::sleep(100ms).get();
    BOOST_REQUIRE(mutex.has_units() == false);

    try {
        std::move(units3).get();
    } catch (const ss::semaphore_timed_out& e) {
        ss::sstring msg = e.what();
        BOOST_REQUIRE(msg.find("test_checkpoint1") != ss::sstring::npos);
        thrown = true;
    }
    BOOST_REQUIRE(thrown);
}
