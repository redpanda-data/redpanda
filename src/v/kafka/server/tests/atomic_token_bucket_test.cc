// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/atomic_token_bucket.h"
#include "utils/to_string.h"

#include <boost/test/unit_test.hpp>

#include <chrono>

using namespace std::chrono_literals;
using std::chrono::milliseconds;

// The token bucket gets constructed with an initial replenishment timestamp
// based on clock::now(), so we need to be careful with the replenishment times.
// In general these unit tests allow more replenishment time than necessary to
// compensate for this.
const auto epsilon = 100ms;

BOOST_AUTO_TEST_CASE(test_atomic_token_bucket_limit) {
    auto now = ss::lowres_clock::now();

    atomic_token_bucket bucket{10, 10, 1, true};

    // Observe that the bucket starts returning a positive delay proportional to
    // the bucket deficit once the limit is crossed
    bucket.record(5);
    BOOST_CHECK_EQUAL(0ms, bucket.calculate_delay<milliseconds>());
    bucket.record(5);
    BOOST_CHECK_EQUAL(0ms, bucket.calculate_delay<milliseconds>());
    bucket.record(1);
    BOOST_CHECK_EQUAL(100ms, bucket.calculate_delay<milliseconds>());
    bucket.record(9);
    BOOST_CHECK_EQUAL(1000ms, bucket.calculate_delay<milliseconds>());

    // "Wait" 1 second to refill by 10 units as per the bucket rate
    bucket.replenish(now + 1s + epsilon);
    BOOST_CHECK_EQUAL(0ms, bucket.calculate_delay<milliseconds>());

    // "Wait" a long time to refill the bucket as much as possible
    bucket.replenish(now + 10s + 2 * epsilon);
    BOOST_CHECK_EQUAL(0ms, bucket.calculate_delay<milliseconds>());
    bucket.record(11);
    BOOST_CHECK_EQUAL(100ms, bucket.calculate_delay<milliseconds>());
}

BOOST_AUTO_TEST_CASE(test_atomic_token_bucket_start_full) {
    // start_full=true should ensure that we start with a bucket filled up to
    // the limit
    atomic_token_bucket full_bucket{10, 10, 1, true};

    full_bucket.record(10 + 1);
    BOOST_CHECK_EQUAL(100ms, full_bucket.calculate_delay<milliseconds>());

    // start_full=false means that the bucket start empty, but it gets
    // replenished at the configured rate starting from the time when the bucket
    // is constructed
    auto now = ss::lowres_clock::now();
    atomic_token_bucket empty_bucket{10, 10, 1, false};

    empty_bucket.record(10 + 1);
    BOOST_CHECK_EQUAL(1100ms, empty_bucket.calculate_delay<milliseconds>());

    empty_bucket.replenish(now + 1100ms + 1s + epsilon);
    BOOST_CHECK_EQUAL(0ms, empty_bucket.calculate_delay<milliseconds>());

    empty_bucket.record(10 + 1);
    BOOST_CHECK_EQUAL(100ms, empty_bucket.calculate_delay<milliseconds>());
}

BOOST_AUTO_TEST_CASE(test_atomic_token_bucket_threshold) {
    auto now = ss::lowres_clock::now();
    atomic_token_bucket bucket{10, 10, 10, true};

    bucket.record(10 + 1);
    BOOST_CHECK_EQUAL(100ms, bucket.calculate_delay<milliseconds>());

    // Replenishing before we have the threshold's worth of tokens
    // re-accumulated should have no effect
    bucket.replenish(now + 100ms + epsilon);
    BOOST_CHECK_EQUAL(100ms, bucket.calculate_delay<milliseconds>());

    // Replenishing once we are above the threshold updates the deficit
    bucket.replenish(now + 1s + 2 * epsilon);
    BOOST_CHECK_EQUAL(0ms, bucket.calculate_delay<milliseconds>());
}

BOOST_AUTO_TEST_CASE(test_atomic_token_bucket_burst) {
    auto now = ss::lowres_clock::now();
    atomic_token_bucket bucket{10, 100, 1, true};

    // There should be no limit applied until the burst capacity is reached
    bucket.record(11);
    BOOST_CHECK_EQUAL(0ms, bucket.calculate_delay<milliseconds>());
    bucket.record(89);
    BOOST_CHECK_EQUAL(0ms, bucket.calculate_delay<milliseconds>());

    // Once the burst capacity is reached, there's a delay based on the rate
    bucket.record(2);
    BOOST_CHECK_EQUAL(200ms, bucket.calculate_delay<milliseconds>());

    // The bucket gets refilled based on the rate, up to the burst capacity
    bucket.replenish(now + 200ms + epsilon);
    BOOST_CHECK_EQUAL(0ms, bucket.calculate_delay<milliseconds>());
    bucket.replenish(now + 10s + 2 * epsilon);
    bucket.record(100);
    BOOST_CHECK_EQUAL(0ms, bucket.calculate_delay<milliseconds>());
}
