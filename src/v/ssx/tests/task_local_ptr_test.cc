// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/task_local_ptr.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/later.hh>

#include <absl/algorithm/container.h>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

using namespace ssx;

SEASTAR_THREAD_TEST_CASE(task_local_throws_on_illegal_access) {
    task_local<seastar::sstring> val("task-local-value");

    BOOST_REQUIRE(val.has_value());

    seastar::yield().get();

    BOOST_REQUIRE(!val.has_value());
    BOOST_REQUIRE_THROW(val.value(), task_local_ptr_invalidated);
}

SEASTAR_THREAD_TEST_CASE(task_local_make_val) {
    seastar::sstring val = "task-local-value";
    auto tl = make_task_local<seastar::sstring>(val.begin(), val.end());

    BOOST_REQUIRE(tl.has_value());
    BOOST_REQUIRE(tl.value() == val);

    seastar::yield().get();

    BOOST_REQUIRE(!tl.has_value());
}

SEASTAR_THREAD_TEST_CASE(task_local_ptr_not_available_after_scheduling_point) {
    seastar::sstring val = "task_local_value";
    task_local_ptr<seastar::sstring> ptr(&val);
    task_local_ptr<seastar::sstring> nul;

    BOOST_REQUIRE(ptr.has_value());
    BOOST_REQUIRE(ptr != nul);
    BOOST_REQUIRE(ptr != nullptr);
    BOOST_REQUIRE(ptr == ptr);
    BOOST_REQUIRE(ptr == &val);

    seastar::yield().get();

    BOOST_REQUIRE(!ptr.has_value());
}

SEASTAR_THREAD_TEST_CASE(task_local_ptr_throws_on_illegal_access) {
    seastar::sstring val = "task_local_value";
    task_local_ptr<seastar::sstring> ptr(&val);

    BOOST_REQUIRE(ptr.has_value());
    BOOST_REQUIRE(ptr->size());

    seastar::yield().get();

    BOOST_REQUIRE(!ptr.has_value());
    BOOST_REQUIRE_THROW(ptr->size(), task_local_ptr_invalidated);
}
