// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "utils/waiter_queue.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

SEASTAR_THREAD_TEST_CASE(test_notifying_value) {
    waiter_queue<ss::sstring> queue;
    ss::abort_source as;
    auto f = queue.await("test_q", as);

    queue.notify("test_q");
    BOOST_REQUIRE(f.available());
    BOOST_REQUIRE(!f.failed());
};

SEASTAR_THREAD_TEST_CASE(test_abort_after_await) {
    waiter_queue<ss::sstring> queue;
    ss::abort_source as;
    auto f = queue.await("test_q", as);
    as.request_abort();
    queue.notify("test_q");
    BOOST_REQUIRE(f.available());
    BOOST_REQUIRE(f.failed());
    BOOST_REQUIRE_THROW(f.get(), ss::abort_requested_exception);
};

SEASTAR_THREAD_TEST_CASE(test_abort_before_await) {
    waiter_queue<ss::sstring> queue;
    ss::abort_source as;
    as.request_abort();
    auto f = queue.await("test_q", as);
    queue.notify("test_q");
    BOOST_REQUIRE(f.available());
    BOOST_REQUIRE(f.failed());
    BOOST_REQUIRE_THROW(f.get(), ss::abort_requested_exception);
};

SEASTAR_THREAD_TEST_CASE(test_abort_after_notify) {
    waiter_queue<ss::sstring> queue;
    ss::abort_source as;
    auto f = queue.await("test_q", as);
    queue.notify("test_q");
    as.request_abort();
    BOOST_REQUIRE(f.available());
    BOOST_REQUIRE(!f.failed());
};
