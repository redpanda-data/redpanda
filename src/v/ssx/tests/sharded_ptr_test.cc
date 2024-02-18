// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "ssx/sharded_ptr.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>

SEASTAR_THREAD_TEST_CASE(sharded_ptr) {
    ssx::sharded_ptr<int> p0;
    BOOST_REQUIRE(!p0);

    for (auto i : boost::irange(0u, ss::smp::count)) {
        ss::smp::submit_to(i, [&]() { BOOST_REQUIRE(!p0); }).get();
    }
    p0.reset(std::make_unique<int>(43)).get();
    for (auto i : boost::irange(0u, ss::smp::count)) {
        ss::smp::submit_to(i, [&]() { BOOST_REQUIRE(p0 && *p0 == 43); }).get();
    }
    p0.stop().get();
    for (auto i : boost::irange(0u, ss::smp::count)) {
        ss::smp::submit_to(i, [&]() { BOOST_REQUIRE(!p0); }).get();
    }
}
