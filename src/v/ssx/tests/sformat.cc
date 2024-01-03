// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/sformat.h"

#include "base/seastarx.h"

#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(ssx_sformat_simple) {
    ss::sstring foo("foo");
    BOOST_REQUIRE_EQUAL(ssx::sformat("{}", foo), "foo");
}

SEASTAR_THREAD_TEST_CASE(fmt_format_join) {
    std::vector<ss::sstring> vec{"foo", "bar", "baz"};
    BOOST_REQUIRE_EQUAL(fmt::format("{}", fmt::join(vec, ",")), "foo,bar,baz");
}

SEASTAR_THREAD_TEST_CASE(ssx_sformat_join) {
    std::vector<ss::sstring> vec{"foo", "bar", "baz"};
    BOOST_REQUIRE_EQUAL(ssx::sformat("{}", fmt::join(vec, ",")), "foo,bar,baz");
}
