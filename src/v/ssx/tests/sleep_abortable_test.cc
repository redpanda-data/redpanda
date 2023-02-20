// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/sleep_abortable.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <chrono>
#include <numeric>
#include <seastarx.h>

using namespace std::chrono_literals;

SEASTAR_THREAD_TEST_CASE(sleep_abortable_abort1) {
    ss::abort_source as;

    auto fut = ssx::sleep_abortable(1000s, as);
    ss::sleep(100ms).get();
    as.request_abort();
    BOOST_REQUIRE_THROW(fut.get(), ss::sleep_aborted);
}

SEASTAR_THREAD_TEST_CASE(sleep_abortable_normal1) {
    ss::abort_source as;
    ssx::sleep_abortable(10ms, as).get();
}

SEASTAR_THREAD_TEST_CASE(sleep_abortable_normal4) {
    ss::abort_source as1, as2, as3, as4;
    ssx::sleep_abortable(10ms, as1, as2, as3, as4).get();
}

SEASTAR_THREAD_TEST_CASE(sleep_abortable_abort2) {
    ss::abort_source as1;
    ss::abort_source as2;

    auto fut = ssx::sleep_abortable(1000s, as1, as2);
    ss::sleep(100ms).get();
    as1.request_abort();
    BOOST_REQUIRE_THROW(fut.get(), ss::sleep_aborted);
}

SEASTAR_THREAD_TEST_CASE(sleep_abortable_abort3) {
    ss::abort_source as1, as2, as3;

    auto fut = ssx::sleep_abortable(1000s, as1, as2, as3);
    ss::sleep(100ms).get();
    as3.request_abort();
    BOOST_REQUIRE_THROW(fut.get(), ss::sleep_aborted);
}

SEASTAR_THREAD_TEST_CASE(sleep_abortable_abort4) {
    ss::abort_source as1, as2, as3, as4;
    auto fut = ssx::sleep_abortable(1000s, as1, as2, as3, as4);
    ss::sleep(100ms).get();
    as2.request_abort();
    BOOST_REQUIRE_THROW(fut.get(), ss::sleep_aborted);
}

SEASTAR_THREAD_TEST_CASE(sleep_abortable_aborted_twice) {
    ss::abort_source as1, as2;
    auto fut = ssx::sleep_abortable(1000s, as1, as2);

    ss::sleep(100ms).get();
    as1.request_abort();
    as2.request_abort();
    BOOST_REQUIRE_THROW(fut.get(), ss::sleep_aborted);
}
