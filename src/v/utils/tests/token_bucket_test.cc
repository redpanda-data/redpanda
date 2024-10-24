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
#include <seastar/core/semaphore.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <chrono>
using namespace std::chrono_literals;

constexpr size_t RATE = 25;

template<typename T>
void try_consume(token_bucket<T>& throttler, size_t amount) {
    for (size_t i = 0; i < amount; ++i) {
        BOOST_REQUIRE(throttler.try_throttle(1));
    }
}

template<typename T>
void consume(token_bucket<T>& throttler, size_t amount) {
    for (size_t i = 0; i < amount; ++i) {
        ss::abort_source as;
        throttler.throttle(1, as).get();
    }
}

SEASTAR_THREAD_TEST_CASE(test_simple_throttle) {
    token_bucket<> throttler(RATE, "test_simple_throttle");
    consume(throttler, RATE);
}

SEASTAR_THREAD_TEST_CASE(test_throttle_wait) {
    token_bucket<> throttler(RATE, "test_throttle_wait");
    consume(throttler, RATE);
    auto start_time = ss::lowres_clock::now();
    consume(throttler, RATE);
    BOOST_REQUIRE(ss::lowres_clock::now() - start_time >= 1s);
}

SEASTAR_THREAD_TEST_CASE(test_throttle_shutdown) {
    token_bucket<ss::manual_clock> throttler(RATE, "test_throttle_shutdown");
    consume(throttler, RATE);
    ss::abort_source as;
    auto result = throttler.throttle(1, as);
    throttler.shutdown();
    BOOST_REQUIRE_EXCEPTION(
      result.get(), ss::broken_named_semaphore, [](ss::broken_named_semaphore) {
          return true;
      });

    BOOST_REQUIRE_EXCEPTION(
      throttler.throttle(1, as).get(),
      ss::broken_named_semaphore,
      [](ss::broken_named_semaphore) { return true; });
    BOOST_REQUIRE(!throttler.try_throttle(1));
}

SEASTAR_THREAD_TEST_CASE(test_throttle_abort) {
    token_bucket<ss::manual_clock> throttler(RATE, "test_throttle_abort");
    consume(throttler, RATE);
    ss::abort_source as;
    auto result = throttler.throttle(1, as);
    as.request_abort();
    BOOST_REQUIRE_EXCEPTION(
      result.get(),
      ss::named_semaphore_aborted,
      [](ss::named_semaphore_aborted) { return true; });
}

SEASTAR_THREAD_TEST_CASE(test_try_throttle) {
    token_bucket<ss::manual_clock> throttler(RATE, "test_try_throttle");
    try_consume(throttler, RATE);
    BOOST_REQUIRE(!throttler.try_throttle(1));
}

SEASTAR_THREAD_TEST_CASE(test_throttle_then_try) {
    token_bucket<ss::manual_clock> throttler(RATE, "test_throttle_then_try");
    consume(throttler, RATE);
    BOOST_REQUIRE(!throttler.try_throttle(1));
}

SEASTAR_THREAD_TEST_CASE(test_wait_until_available) {
    token_bucket<ss::manual_clock> throttler(RATE, "test_wait_until_available");
    try_consume(throttler, RATE);
    BOOST_REQUIRE(!throttler.try_throttle(1));
    ss::manual_clock::advance(ss::lowres_clock::duration(2s));
    try_consume(throttler, RATE);
}

SEASTAR_THREAD_TEST_CASE(test_burst) {
    token_bucket<ss::manual_clock> throttler(RATE, "test_burst", RATE * 3);
    try_consume(throttler, RATE);
    BOOST_REQUIRE(!throttler.try_throttle(1));
    ss::manual_clock::advance(ss::lowres_clock::duration(4s));
    try_consume(throttler, RATE * 3);
    BOOST_REQUIRE(!throttler.try_throttle(1));
}

SEASTAR_THREAD_TEST_CASE(test_update_rate) {
    const size_t RATE_PART = RATE - 10;
    token_bucket<ss::manual_clock> throttler(RATE, "test_update_rate");

    try_consume(throttler, RATE_PART);
    throttler.update_rate(RATE_PART);
    // Right after update there should be 0 points because we consumed RATE_PART
    // and downscaled rate to RATE_PART
    BOOST_REQUIRE(!throttler.try_throttle(1));

    ss::manual_clock::advance(ss::lowres_clock::duration(2s));
    try_consume(throttler, RATE_PART);
    BOOST_REQUIRE(!throttler.try_throttle(1));

    ss::manual_clock::advance(ss::lowres_clock::duration(2s));
    try_consume(throttler, RATE_PART);
    BOOST_REQUIRE(!throttler.try_throttle(1));
    throttler.update_rate(RATE);
    // Right after update there should be RATE - RATE_PART points because we
    // consumed RATE_PART and upgraded rate to RATE_PART
    try_consume(throttler, RATE - RATE_PART);
    BOOST_REQUIRE(!throttler.try_throttle(1));
}

SEASTAR_THREAD_TEST_CASE(test_update_rate_burst) {
    const size_t RATE_PART = RATE - 10;
    token_bucket<ss::manual_clock> throttler(
      RATE, "test_update_rate_burst", RATE * 3);

    ss::manual_clock::advance(ss::lowres_clock::duration(4s));
    try_consume(throttler, RATE);
    throttler.update_rate(RATE_PART);

    // Right after update there should be RATE_PART points
    // because we accumulated 2 x Rate point before update
    // and downscaled rate to RATE_PART. So accumulated points
    // should drop to RATE_PART points
    try_consume(throttler, RATE_PART);
    BOOST_REQUIRE(!throttler.try_throttle(1));

    ss::manual_clock::advance(ss::lowres_clock::duration(4s));
    try_consume(throttler, RATE);
    throttler.update_rate(RATE);
    try_consume(throttler, RATE);
    BOOST_REQUIRE(!throttler.try_throttle(1));
}

SEASTAR_THREAD_TEST_CASE(test_available_tokens) {
    token_bucket<ss::manual_clock> throttler(RATE, "test_available_tokens");
    BOOST_REQUIRE_EQUAL(throttler.available(), RATE);
    consume(throttler, RATE);
    BOOST_REQUIRE_EQUAL(throttler.available(), 0);

    ss::manual_clock::advance(ss::lowres_clock::duration(2s));
    BOOST_REQUIRE_EQUAL(throttler.available(), RATE);
    consume(throttler, 10);
    BOOST_REQUIRE_EQUAL(throttler.available(), RATE - 10);
}

SEASTAR_THREAD_TEST_CASE(test_available_tokens_capacity) {
    token_bucket<ss::manual_clock> throttler(
      RATE, "test_available_tokens_capacity", RATE * 3);
    BOOST_REQUIRE_EQUAL(throttler.available(), RATE);
    consume(throttler, RATE);
    BOOST_REQUIRE_EQUAL(throttler.available(), 0);

    ss::manual_clock::advance(ss::lowres_clock::duration(4s));
    BOOST_REQUIRE_EQUAL(throttler.available(), RATE * 3);
    consume(throttler, 10);
    BOOST_REQUIRE_EQUAL(throttler.available(), RATE * 3 - 10);
}

SEASTAR_THREAD_TEST_CASE(test_available_tokens_change_capacity) {
    token_bucket<ss::manual_clock> throttler(
      RATE, "test_available_tokens_change_capacity", RATE * 3);
    ss::manual_clock::advance(ss::lowres_clock::duration(4s));
    BOOST_REQUIRE_EQUAL(throttler.available(), RATE * 3);
    throttler.update_capacity(RATE * 2);
    BOOST_REQUIRE_EQUAL(throttler.available(), RATE * 2);
    throttler.update_capacity(RATE * 3);
    BOOST_REQUIRE_EQUAL(throttler.available(), RATE * 2);
    ss::manual_clock::advance(ss::lowres_clock::duration(2s));
    BOOST_REQUIRE_EQUAL(throttler.available(), RATE * 3);
}
