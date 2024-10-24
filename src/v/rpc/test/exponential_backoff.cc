// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include <seastar/core/lowres_clock.hh>

#include <boost/test/tools/old/interface.hpp>

#include <chrono>
#define BOOST_TEST_MODULE bytes
#include "rpc/backoff_policy.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(exponential_backoff_policy_test) {
    using namespace std::chrono_literals;
    rpc::backoff_policy p
      = rpc::make_exponential_backoff_policy<ss::lowres_clock>(1s, 120s);

    BOOST_CHECK(p.current_backoff_duration() == 0s);
    p.next_backoff();
    BOOST_CHECK(p.current_backoff_duration() == 1s);
    p.next_backoff();
    BOOST_CHECK(p.current_backoff_duration() == 2s);
    p.next_backoff();
    BOOST_CHECK(p.current_backoff_duration() == 4s);
    p.next_backoff();
    BOOST_CHECK(p.current_backoff_duration() == 8s);
    p.next_backoff();
    BOOST_CHECK(p.current_backoff_duration() == 16s);
    p.next_backoff();
    BOOST_CHECK(p.current_backoff_duration() == 32s);
    p.next_backoff();
    BOOST_CHECK(p.current_backoff_duration() == 64s);
    p.next_backoff();
    BOOST_CHECK(p.current_backoff_duration() == 120s);
    p.next_backoff();
    BOOST_CHECK(p.current_backoff_duration() == 120s);
    p.next_backoff();
    BOOST_CHECK(p.current_backoff_duration() == 120s);
    p.reset();
    BOOST_CHECK(p.current_backoff_duration() == 0s);
    p.next_backoff();
    BOOST_CHECK(p.current_backoff_duration() == 1s);
    p.next_backoff();
    BOOST_CHECK(p.current_backoff_duration() == 2s);
}
