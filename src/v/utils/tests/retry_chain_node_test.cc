// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/retry_chain_node.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <array>
#include <cstdint>
#include <iostream>
#include <random>

using namespace std::chrono_literals;

static ss::abort_source never_abort;

SEASTAR_THREAD_TEST_CASE(check_fmt) {
    retry_chain_node n1(never_abort, ss::lowres_clock::now() + 100ms, 50ms);
    BOOST_REQUIRE_EQUAL(n1(), "[fiber0|0|100ms]");
    {
        retry_chain_node n2(&n1);
        BOOST_REQUIRE_EQUAL(n2(), "[fiber0~0|0|100ms]");
        {
            retry_chain_node n3(&n2);
            BOOST_REQUIRE_EQUAL(n3(), "[fiber0~0~0|0|100ms]");
            {
                retry_chain_node n4(&n3);
                BOOST_REQUIRE_EQUAL(n4(), "[fiber0~0~0~0|0|100ms]");
            }
            retry_chain_node n5(&n2);
            BOOST_REQUIRE_EQUAL(n5(), "[fiber0~0~1|0|100ms]");
            {
                retry_chain_node n6(&n5);
                BOOST_REQUIRE_EQUAL(n6(), "[fiber0~0~1~0|0|100ms]");
            }
        }
    }
    {
        retry_chain_node n7(&n1);
        BOOST_REQUIRE_EQUAL(n7(), "[fiber0~1|0|100ms]");
    }
    retry_chain_node n8(never_abort, ss::lowres_clock::now() + 100ms, 50ms);
    BOOST_REQUIRE_EQUAL(n8(), "[fiber1|0|100ms]");
    BOOST_REQUIRE_EQUAL(n8("{} + {}", 1, 2), "[fiber1|0|100ms 1 + 2]");
}

SEASTAR_THREAD_TEST_CASE(check_retry1) {
    retry_chain_node n1(never_abort);
    std::array<
      std::pair<ss::lowres_clock::duration, ss::lowres_clock::duration>,
      4>
      intervals = {{
        {100ms, 200ms},
        {200ms, 400ms},
        {400ms, 800ms},
        {800ms, 1600ms},
      }};

    retry_permit permit = n1.retry();
    BOOST_REQUIRE(!permit.is_allowed);
    retry_chain_node n2(ss::lowres_clock::now() + 1600ms, 100ms, &n1);
    for (int i = 0; i < 4; i++) {
        permit = n2.retry();
        BOOST_REQUIRE(permit.is_allowed);
        BOOST_REQUIRE(permit.abort_source == &never_abort);
        BOOST_REQUIRE(permit.delay >= intervals.at(i).first);
        BOOST_REQUIRE(permit.delay < intervals.at(i).second);
    }
    permit = n2.retry();
    BOOST_REQUIRE(!permit.is_allowed);
}

SEASTAR_THREAD_TEST_CASE(check_retry2) {
    ss::abort_source never_abort;
    retry_chain_node n1(never_abort, ss::lowres_clock::now() + 1600ms, 100ms);
    std::array<
      std::pair<ss::lowres_clock::duration, ss::lowres_clock::duration>,
      4>
      intervals = {{
        {100ms, 200ms},
        {200ms, 400ms},
        {400ms, 800ms},
        {800ms, 1600ms},
      }};
    retry_permit permit;
    for (int i = 0; i < 4; i++) {
        permit = n1.retry();
        BOOST_REQUIRE(permit.is_allowed);
        BOOST_REQUIRE(permit.abort_source == &never_abort);
        BOOST_REQUIRE(permit.delay >= intervals.at(i).first);
        BOOST_REQUIRE(permit.delay < intervals.at(i).second);
    }
    permit = n1.retry();
    BOOST_REQUIRE(!permit.is_allowed);

    // n2 should inherit all retry-related stuff
    retry_chain_node n2(&n1);
    for (int i = 0; i < 4; i++) {
        permit = n2.retry();
        BOOST_REQUIRE(permit.is_allowed);
        BOOST_REQUIRE(permit.abort_source == &never_abort);
        BOOST_REQUIRE(permit.delay >= intervals.at(i).first);
        BOOST_REQUIRE(permit.delay < intervals.at(i).second);
    }
    permit = n2.retry();
    BOOST_REQUIRE(!permit.is_allowed);
}

SEASTAR_THREAD_TEST_CASE(check_child_retry_canceled) {
    ss::abort_source as;
    retry_chain_node n1(as, ss::lowres_clock::now() + 1000ms, 100ms);
    retry_chain_node n2(retry_strategy::polling, &n1);
    retry_permit permit = n2.retry();
    BOOST_REQUIRE(permit.is_allowed);
    n1.request_abort();
    BOOST_REQUIRE_THROW(n2.retry(), ss::abort_requested_exception);
}

SEASTAR_THREAD_TEST_CASE(check_root_retry_canceled) {
    ss::abort_source as;
    retry_chain_node n1(
      as, ss::lowres_clock::now() + 1000ms, 100ms, retry_strategy::polling);
    retry_chain_node n2(&n1);
    retry_permit permit = n1.retry();
    BOOST_REQUIRE(permit.is_allowed);
    n2.request_abort();
    BOOST_REQUIRE_THROW(n1.retry(), ss::abort_requested_exception);
}

SEASTAR_THREAD_TEST_CASE(check_abort_requested) {
    ss::abort_source as;
    retry_chain_node n1(as, ss::lowres_clock::now() + 1000ms, 100ms);
    retry_chain_node n2(&n1);
    n1.request_abort();
    BOOST_REQUIRE_THROW(n2.check_abort(), ss::abort_requested_exception);
}

SEASTAR_THREAD_TEST_CASE(check_deadline_propogation_1) {
    retry_chain_node n1(never_abort);
    retry_chain_node n2(1000ms, 100ms, &n1);
    BOOST_REQUIRE(n1.get_timeout() == 0ms);
    BOOST_REQUIRE(n2.get_timeout() == 1000ms);
}

SEASTAR_THREAD_TEST_CASE(check_deadline_propogation_2) {
    retry_chain_node n1(never_abort, 1000ms, 100ms);
    retry_chain_node n2(500ms, 100ms, &n1);
    BOOST_REQUIRE(n1.get_timeout() == 1000ms);
    BOOST_REQUIRE(n2.get_timeout() == 500ms);
}

SEASTAR_THREAD_TEST_CASE(check_deadline_propogation_3) {
    retry_chain_node n1(never_abort, 500ms, 100ms);
    retry_chain_node n2(1000ms, 100ms, &n1);
    BOOST_REQUIRE(n1.get_timeout() == 500ms);
    BOOST_REQUIRE(n2.get_timeout() == 500ms);
}

SEASTAR_THREAD_TEST_CASE(check_node_comparison) {
    ss::abort_source as;
    retry_chain_node r1(as);
    retry_chain_node r2(as);
    retry_chain_node c11(&r1);
    retry_chain_node c111(&c11);
    retry_chain_node c12(&r1);
    retry_chain_node c21(&r2);
    BOOST_REQUIRE(r1.same_root(r1));
    BOOST_REQUIRE(r1.same_root(c11));
    BOOST_REQUIRE(r1.same_root(c111));
    BOOST_REQUIRE(c11.same_root(c12));
    BOOST_REQUIRE(!r1.same_root(r2));
    BOOST_REQUIRE(!r1.same_root(c21));
    BOOST_REQUIRE(!c11.same_root(c21));
}
