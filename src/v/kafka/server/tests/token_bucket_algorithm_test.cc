// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/token_bucket_rate_tracker.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <chrono>
#include <limits>

using namespace std::chrono_literals;

SEASTAR_THREAD_TEST_CASE(token_bucket_algorithm_test) {
    /// Defaults
    /// Quota = 5 units / second
    /// Time window = 2 seconds
    /// Samples = 10
    const auto quota = 5;
    const auto samples = 10;
    const auto window_sec = 2s;

    const auto now = kafka::token_bucket_rate_tracker::clock::now();

    /// Bucket should start with burst() credits as max, so 100 units
    kafka::token_bucket_rate_tracker rate(quota, samples, window_sec);

    /// Substract 50 units at time T, should leave 50 in bucket
    auto should_delay = rate.record_and_measure(50, now);
    BOOST_CHECK_GE(should_delay, 0);
    BOOST_CHECK_EQUAL(50, rate.units());

    /// Taking another 40 at time T will leave 10
    should_delay = rate.record_and_measure(40, now);
    BOOST_CHECK_GE(should_delay, 0);
    BOOST_CHECK_EQUAL(10, rate.units());

    /// Advance by 2s, adds 'quota' * 'window_sec' units to the bucket, bringing
    /// the total to 20, subtracting 10 will leave 10 units
    should_delay = rate.record_and_measure(10, now + 2s);
    BOOST_CHECK_GE(should_delay, 0);
    BOOST_CHECK_EQUAL(10, rate.units());

    /// Advance by another 2s, adds another 10 units, recording 60 will leave
    /// the final total number of units to -40 credits
    should_delay = rate.record_and_measure(60, now + 4s);
    /// Assert the should_delay value is false since it represents the
    /// algorithm allowing the bursty request to go through
    BOOST_CHECK_GE(should_delay, 0);
    BOOST_CHECK_EQUAL(-40, rate.units());

    /// However subsequent requests should be throttled if enough tokens haven't
    /// been added betwen the two time units
    should_delay = rate.record_and_measure(5, now + 4s);
    BOOST_CHECK_LT(should_delay, 0);
    /// Number of units remains constant as the algorithm doesn't take units
    /// during its 'cooldown' phase, only when resources are being used up.
    BOOST_CHECK_EQUAL(-40, rate.units());

    /// Eventually after enough time, burst should take precedence
    should_delay = rate.record_and_measure(5, now + 100s);
    /// ... and there are enough tokens in the bucket, program can commence
    /// without throttle
    BOOST_CHECK_GE(should_delay, 0);
    BOOST_CHECK_EQUAL(95, rate.units());
}

SEASTAR_THREAD_TEST_CASE(token_bucket_edge_case_test) {
    const auto now = kafka::token_bucket_rate_tracker::clock::now();
    const auto max_val = std::numeric_limits<uint32_t>::max();
    kafka::token_bucket_rate_tracker rate(max_val, max_val, 2s);
    auto should_delay = rate.record_and_measure(40, now);
    BOOST_CHECK_GE(should_delay, 0);
    BOOST_CHECK_EQUAL((max_val - 40), rate.units());
}
