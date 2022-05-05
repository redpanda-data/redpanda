// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/periodic.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <chrono>
#include <limits>

using namespace std::chrono_literals;

/**
 * @brief A thread-local mock clock.
 */
class mock_clock {
public:
    using rep = int64_t;
    using period = std::chrono::milliseconds::period;
    using duration = std::chrono::milliseconds;
    using time_point = std::chrono::time_point<mock_clock, duration>;

    static constexpr rep bad_value = std::numeric_limits<rep>::min();

private:
    static thread_local rep _now;

public:
    static time_point now() {
        BOOST_REQUIRE(_now != bad_value);
        return time_point(duration(_now));
    }

    static void advance(duration d) { _now += d.count(); }

    static void set(duration d) {
        _now = d.count();
    }

    static void poison() {
        _now = bad_value;
    }
};

thread_local mock_clock::rep mock_clock::_now = mock_clock::bad_value;

SEASTAR_THREAD_TEST_CASE(periodic_first_check) {
    periodic_ms p{1ms};
    // the first check() for periodic should always be true
    BOOST_CHECK(p.check());
};

using fake_periodic = periodic<mock_clock>;

SEASTAR_THREAD_TEST_CASE(periodic_fake_clock) {
    // fmt::print("{}", periodic_ms::duration_type::min().count());

    for (auto initial : {-100000ms, 0ms, 100000ms}) {
        mock_clock::set(-100000ms);

        fake_periodic p{100ms};

        BOOST_CHECK(p.check());
        BOOST_CHECK(!p.check());

        mock_clock::advance(99ms);
        BOOST_CHECK(!p.check());

        mock_clock::advance(2ms);
        BOOST_CHECK(p.check());
        BOOST_CHECK(!p.check());

        mock_clock::advance(101ms);
        BOOST_CHECK(p.check());
        BOOST_CHECK(!p.check());

        mock_clock::advance(500ms);
        BOOST_CHECK(p.check());
        BOOST_CHECK(!p.check());
    }

    mock_clock::poison();
};
