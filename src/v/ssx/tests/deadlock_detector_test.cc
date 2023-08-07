// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "seastarx.h"
#include "ssx/deadlock_detector.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/when_all.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <stdexcept>

class tracking_monitor {
public:
    void
    report_deadlock(const seastar::sstring& lhs, const seastar::sstring& rhs) {
        last_lhs = lhs;
        last_rhs = rhs;
        count++;
    }
    void report_recursion(const seastar::sstring& lhs) {
        last_lhs = lhs;
        count++;
    }
    int count{0};
    ss::sstring last_lhs;
    ss::sstring last_rhs;
};

SEASTAR_THREAD_TEST_CASE(deadlock_detector_good) {
    using namespace ssx::details;
    deadlock_detector<> detector;

    ss::sstring curr("current");
    ss::sstring prev("previous");

    deadlock_detector<>::lock_vector vec{
      .prev = std::ref(prev),
      .curr = std::ref(curr),
    };
    detector.on_lock(vec);
    detector.on_lock(vec);
}

SEASTAR_THREAD_TEST_CASE(deadlock_detector_fail) {
    using namespace ssx::details;
    deadlock_detector<tracking_monitor> detector;

    ss::sstring curr("current");
    ss::sstring prev("previous");

    deadlock_detector<tracking_monitor>::lock_vector vec{
      .prev = std::ref(prev),
      .curr = std::ref(curr),
    };
    deadlock_detector<tracking_monitor>::lock_vector inv{
      .prev = std::ref(curr),
      .curr = std::ref(prev),
    };
    detector.on_lock(vec);
    BOOST_REQUIRE(detector.get_monitor().count == 0);
    detector.on_lock(inv);
    BOOST_REQUIRE(detector.get_monitor().count == 1);
}

namespace {
void reset() {
    using namespace ssx::details;
    deadlock_detector_waiter_state<tracking_monitor> accessor("accessor");
    accessor.get_deadlock_detector().reset();
}
} // namespace

SEASTAR_THREAD_TEST_CASE(deadlock_detector_waiter_state_good) {
    using namespace ssx::details;
    reset();

    ss::sstring curr("current");
    ss::sstring prev("previous");

    {
        deadlock_detector_waiter_state<tracking_monitor> st1(prev);
        deadlock_detector_waiter_state<tracking_monitor> st2(curr);

        BOOST_REQUIRE_EQUAL(st1.get_monitor().count, 0);
    }

    {
        deadlock_detector_waiter_state<tracking_monitor> st1(prev);
        deadlock_detector_waiter_state<tracking_monitor> st2(curr);

        BOOST_REQUIRE_EQUAL(st1.get_monitor().count, 0);
    }
}

SEASTAR_THREAD_TEST_CASE(deadlock_detector_waiter_state_deadlock) {
    using namespace ssx::details;
    reset();

    ss::sstring curr("current");
    ss::sstring prev("previous");

    {
        deadlock_detector_waiter_state<tracking_monitor> st1(prev);
        deadlock_detector_waiter_state<tracking_monitor> st2(curr);

        BOOST_REQUIRE_EQUAL(st1.get_monitor().count, 0);
    }

    {
        deadlock_detector_waiter_state<tracking_monitor> st1(curr);
        deadlock_detector_waiter_state<tracking_monitor> st2(prev);

        BOOST_REQUIRE_EQUAL(st1.get_monitor().count, 1);
    }
}

SEASTAR_THREAD_TEST_CASE(deadlock_detector_waiter_state_async_good) {
    using namespace ssx::details;
    reset();

    ss::sstring curr("current");
    ss::sstring prev("previous");

    auto fut1 = ss::yield().then([&] {
        deadlock_detector_waiter_state<tracking_monitor> st1(prev);
        deadlock_detector_waiter_state<tracking_monitor> st2(curr);
    });

    auto fut2 = ss::yield().then([&] {
        deadlock_detector_waiter_state<tracking_monitor> st1(prev);
        deadlock_detector_waiter_state<tracking_monitor> st2(curr);
    });

    ss::when_all_succeed(std::move(fut1), std::move(fut2)).get();

    deadlock_detector_waiter_state<tracking_monitor> st(curr);
    BOOST_REQUIRE_EQUAL(st.get_monitor().count, 0);
}

SEASTAR_THREAD_TEST_CASE(deadlock_detector_waiter_state_async_deadlock) {
    using namespace ssx::details;
    reset();

    ss::sstring curr("current");
    ss::sstring prev("previous");

    auto fut1 = ss::yield().then([&] {
        deadlock_detector_waiter_state<tracking_monitor> st1(prev);
        deadlock_detector_waiter_state<tracking_monitor> st2(curr);
    });

    auto fut2 = ss::yield().then([&] {
        deadlock_detector_waiter_state<tracking_monitor> st1(curr);
        deadlock_detector_waiter_state<tracking_monitor> st2(prev);
    });

    ss::when_all_succeed(std::move(fut1), std::move(fut2)).get();

    deadlock_detector_waiter_state<tracking_monitor> st(curr);
    BOOST_REQUIRE_EQUAL(st.get_monitor().count, 1);
}

SEASTAR_THREAD_TEST_CASE(deadlock_detector_waiter_state_sharded) {
    // Check that different shards do not interact with each other
    using namespace ssx::details;
    reset();

    ss::sstring curr("current");
    ss::sstring prev("previous");

    auto fut1 = ss::smp::submit_to(0, [=] {
        deadlock_detector_waiter_state<tracking_monitor> st1(prev);
        deadlock_detector_waiter_state<tracking_monitor> st2(curr);
        BOOST_REQUIRE_EQUAL(st2.get_monitor().count, 0);
    });

    auto fut2 = ss::smp::submit_to(1, [&] {
        deadlock_detector_waiter_state<tracking_monitor> st1(curr);
        deadlock_detector_waiter_state<tracking_monitor> st2(prev);
        BOOST_REQUIRE_EQUAL(st2.get_monitor().count, 0);
    });

    ss::when_all_succeed(std::move(fut1), std::move(fut2)).get();
}
