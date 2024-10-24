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
#define BOOST_TEST_MODULE utils

#include "utils/bottomless_token_bucket.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(muldiv_test) {
    constexpr static auto mn = std::numeric_limits<int64_t>::min();
    constexpr static auto mx = std::numeric_limits<int64_t>::max();
    // straight samples
    BOOST_CHECK_EQUAL(muldiv(98733, 9287, 6489), 98733 * 9287 / 6489);
    BOOST_CHECK_EQUAL(muldiv(-494162, -654, -998), -494162 * -654 / -998);
    // zeros
    BOOST_CHECK_EQUAL(muldiv(0, -654, -998), 0);
    BOOST_CHECK_EQUAL(muldiv(-494162, 0, -998), 0);
    // near direct overflow
    BOOST_CHECK_EQUAL(muldiv(6935552231102021794, 2, 3), 4623701487401347862);
    BOOST_CHECK_EQUAL(
      muldiv(10, -2305878194122661888, -3), 7686260647075539626);
    // near intermediate overflow
    BOOST_CHECK_EQUAL(
      muldiv(1152921504606846976, 8495162354, 1085720514), 9020972926972920768);
    // boundary values as 1st argument
    BOOST_CHECK_EQUAL(muldiv(mx, 1, 1), mx);
    BOOST_CHECK_EQUAL(muldiv(mn, 1, 1), mn);
    BOOST_CHECK_EQUAL(muldiv(mx, 777, 1554), mx / 2);
    BOOST_CHECK_EQUAL(muldiv(mn, -32767, 32768), (1ull << 63) - (1ull << 48));
    // boundary values as 2nd argument
    BOOST_CHECK_EQUAL(muldiv(1, mx, 1), mx);
    BOOST_CHECK_EQUAL(muldiv(1, mn, 1), mn);
    // boundary values as 3rd argument
    BOOST_CHECK_EQUAL(muldiv(1, 1, mx), 0);
    BOOST_CHECK_EQUAL(muldiv(1, 1, mn), 0);
    BOOST_CHECK_EQUAL(muldiv(mx / 2, 2, mx), 0); // mx is odd, mx/2 is truncated
    BOOST_CHECK_EQUAL(muldiv(mn / 2, 2, mn), 1); // mn is even
}

BOOST_AUTO_TEST_CASE(test_bottomless_token_bucket) {
    using namespace std::chrono;

    // === straight cases
    bottomless_token_bucket b1(1, milliseconds(1)), b2(1000, seconds(1));
    BOOST_CHECK_EQUAL(b1.quota(), 1);
    BOOST_CHECK_EQUAL(b1.tokens(), 0); // == 1*0.001 truncated
    BOOST_CHECK_EQUAL(b2.quota(), 1000);
    BOOST_CHECK_EQUAL(b2.tokens(), 1000); // == 1000*1

    ss::lowres_clock::time_point now;
    b1.use(1, now);
    b2.use(1, now);
    // token count goes negative because the time is zero
    BOOST_CHECK_EQUAL(b1.tokens(), -1);
    BOOST_CHECK_EQUAL(b2.tokens(), 1000 - 1);

    now += seconds(10);
    b1.use(1, now);
    b2.use(1, now);
    BOOST_CHECK_LE(b1.tokens(), 0); // LE because burst is rounded internally
    BOOST_CHECK_EQUAL(b2.tokens(), b2.quota() /* *1s */ - 1);

    b2.use(11, now);
    BOOST_CHECK_EQUAL(b2.tokens(), b2.quota() - 12);

    now += seconds(1);
    b2.use(1, now);
    BOOST_CHECK_EQUAL(b2.tokens(), b2.quota() - 1);

    b2.use(9999, now);
    BOOST_CHECK_EQUAL(b2.tokens(), b2.quota() - 10000);

    now += milliseconds(100);
    b2.use(1, now);
    BOOST_CHECK_EQUAL(b2.tokens(), b2.quota() - 10000 + 100 - 1);

    // === boundary cases
    bottomless_token_bucket b3(
      bottomless_token_bucket::max_quota, bottomless_token_bucket::max_width);
    BOOST_CHECK_EQUAL(b3.quota(), bottomless_token_bucket::max_quota);
    const auto max_tokens = muldiv(
      bottomless_token_bucket::max_quota,
      bottomless_token_bucket::max_width.count(),
      1000);
    BOOST_CHECK_EQUAL(b3.tokens(), max_tokens);

    // to zero
    b3.use(max_tokens, bottomless_token_bucket::clock::time_point{});
    BOOST_CHECK_EQUAL(b3.tokens(), 0);

    // half way up
    const auto halfwayup = duration_cast<ss::lowres_clock::duration>(
                             bottomless_token_bucket::max_width)
                           / 2;
    now = ss::lowres_clock::time_point{} + halfwayup;
    b3.use(1, now);
    BOOST_CHECK_EQUAL(b3.tokens(), 2'305'843'005'992'468'481 - 1);

    // another half way up
    now += halfwayup;
    b3.use(1, now);
    BOOST_CHECK_EQUAL(b3.tokens(), 4'611'686'011'984'936'962 - 2);

    // limited by burst this time
    now += halfwayup;
    b3.use(1, now);
    BOOST_CHECK_EQUAL(b3.tokens(), 4'611'686'014'132'420'609 - 1);
    // the value is different here due to `now` rounded to milliseconds in use()

    // === setters
    b3.set_quota(1'000'000);
    BOOST_CHECK_EQUAL(
      b3.tokens(),
      bottomless_token_bucket::max_width.count() * 1'000'000
        / 1000 /* ms->s */);
    b3.set_width(milliseconds(3'000));
    BOOST_CHECK_EQUAL(b3.tokens(), 3 * 1'000'000);

    b3.use(1'000'000, now);
    BOOST_CHECK_EQUAL(b3.tokens(), 3 * 1'000'000 - 1'000'000);

    now += milliseconds(3'000);
    b3.use(1, now);
    BOOST_CHECK_EQUAL(b3.tokens(), 3 * 1'000'000 - 1);
}
