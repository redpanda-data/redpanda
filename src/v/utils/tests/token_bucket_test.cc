// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/token_bucket.h"

#include <seastar/core/manual_clock.hh>
#include <seastar/testing/thread_test_case.hh>

#include <chrono>
using namespace std::chrono_literals;

constexpr size_t RATE = 25;

SEASTAR_THREAD_TEST_CASE(test_simple_throttle) {
    token_bucket<ss::manual_clock> throttler(RATE, "test_simple_throttle");
    for (size_t i = 0; i < RATE; ++i) {
        ss::abort_source as;
        throttler.throttle(1, as).get();
    }
}

SEASTAR_THREAD_TEST_CASE(test_try_throttle) {
    token_bucket<ss::manual_clock> throttler(RATE, "test_try_throttle");
    for (size_t i = 0; i < RATE; ++i) {
        BOOST_REQUIRE(throttler.try_throttle(1));
    }
    BOOST_REQUIRE(!throttler.try_throttle(1));
}

SEASTAR_THREAD_TEST_CASE(test_throttle_then_try) {
    token_bucket<ss::manual_clock> throttler(RATE, "test_throttle_then_try");
    for (size_t i = 0; i < RATE; ++i) {
        ss::abort_source as;
        throttler.throttle(1, as).get();
    }
    BOOST_REQUIRE(!throttler.try_throttle(1));
}

SEASTAR_THREAD_TEST_CASE(test_wait_until_available) {
    token_bucket<ss::manual_clock> throttler(RATE, "test_wait_until_available");
    for (size_t i = 0; i < RATE; ++i) {
        BOOST_REQUIRE(throttler.try_throttle(1));
    }
    BOOST_REQUIRE(!throttler.try_throttle(1));
    ss::manual_clock::advance(ss::lowres_clock::duration(2s));
    for (size_t i = 0; i < RATE; ++i) {
        BOOST_REQUIRE(throttler.try_throttle(1));
    }
}

SEASTAR_THREAD_TEST_CASE(test_update_rate) {
    const size_t RATE_PART = RATE - 10;
    token_bucket<ss::manual_clock> throttler(RATE, "test_update_rate");

    for (size_t i = 0; i < RATE_PART; ++i) {
        BOOST_REQUIRE(throttler.try_throttle(1));
    }
    throttler.update_rate(RATE_PART);
    // Right after update there should be 0 points because we consumed RATE_PART
    // and downscaled rate to RATE_PART
    BOOST_REQUIRE(!throttler.try_throttle(1));

    ss::manual_clock::advance(ss::lowres_clock::duration(2s));
    for (size_t i = 0; i < RATE_PART; ++i) {
        BOOST_REQUIRE(throttler.try_throttle(1));
    }
    BOOST_REQUIRE(!throttler.try_throttle(1));

    ss::manual_clock::advance(ss::lowres_clock::duration(2s));
    for (size_t i = 0; i < RATE_PART; ++i) {
        BOOST_REQUIRE(throttler.try_throttle(1));
    }
    BOOST_REQUIRE(!throttler.try_throttle(1));
    throttler.update_rate(RATE);
    // Right after update there should be RATE - RATE_PART points because we
    // consumed RATE_PART and upgraded rate to RATE_PART
    for (size_t i = 0; i < RATE - RATE_PART; ++i) {
        BOOST_REQUIRE(throttler.try_throttle(1));
    }
    BOOST_REQUIRE(!throttler.try_throttle(1));
}
