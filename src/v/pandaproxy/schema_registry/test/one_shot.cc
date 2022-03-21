// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/schema_registry/util.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/std-coroutine.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/later.hh>

#include <boost/range/irange.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <stdexcept>

namespace psr = pandaproxy::schema_registry;
using namespace std::chrono_literals;

SEASTAR_THREAD_TEST_CASE(test_one_shot_simple) {
    size_t count{0};
    psr::one_shot one{[&count]() {
        ++count;
        return ss::now();
    }};
    BOOST_CHECK_EQUAL(count, 0);
    one().get();
    BOOST_CHECK_EQUAL(count, 1);
    one().get();
    BOOST_CHECK_EQUAL(count, 1);
}

SEASTAR_THREAD_TEST_CASE(test_one_shot_fail) {
    size_t count{0};
    psr::one_shot one{[&count]() {
        ++count;
        return ss::make_exception_future<>(std::runtime_error("failed"));
    }};
    BOOST_CHECK_THROW(one().get(), std::runtime_error);
    BOOST_CHECK_EQUAL(count, 1);
    BOOST_CHECK_THROW(one().get(), std::runtime_error);
    BOOST_CHECK_EQUAL(count, 2);
}

SEASTAR_THREAD_TEST_CASE(test_one_shot_multi) {
    size_t count{0};
    size_t ex_count{0};
    psr::one_shot one{[&count]() -> ss::future<> {
        ++count;
        co_await ss::sleep(10ms);
        throw std::runtime_error("failed");
    }};
    ss::parallel_for_each(boost::irange(10), [&](auto) {
        return one().discard_result().handle_exception(
          [&](auto) { ++ex_count; });
    }).get();
    BOOST_CHECK_EQUAL(count, 1);
    BOOST_CHECK_EQUAL(ex_count, 10);
    ss::parallel_for_each(boost::irange(10), [&](auto) {
        return one().discard_result().handle_exception(
          [&](auto) { ++ex_count; });
    }).get();
    BOOST_CHECK_EQUAL(count, 2);
    BOOST_CHECK_EQUAL(ex_count, 20);
}
