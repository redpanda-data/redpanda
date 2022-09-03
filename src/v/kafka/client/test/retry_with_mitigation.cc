// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/retry_with_mitigation.h"

#include <seastar/core/future.hh>
#include <seastar/testing/thread_test_case.hh>

#include <absl/container/flat_hash_set.h>
#include <boost/test/tools/old/interface.hpp>

#include <chrono>
#include <exception>
#include <initializer_list>
#include <stdexcept>

namespace kc = kafka::client;
using namespace std::chrono_literals;

namespace {

struct counted_error : std::exception {
    explicit counted_error(size_t count)
      : count(count) {}
    size_t count;
};

struct context {
    std::vector<bool> _should_throw;
    size_t _count;

    context(std::initializer_list<bool> should_throw)
      : _should_throw(should_throw)
      , _count(0) {}

    ss::future<> operator()() {
        auto count = _count++;
        if (_should_throw[count]) {
            return ss::make_exception_future(counted_error(_count));
        } else {
            return ss::make_ready_future<>();
        }
    }
    ss::future<> operator()(std::exception_ptr) { return ss::now(); }

    size_t retries() { return _should_throw.size() - 1; }
    size_t calls() { return _count; }
};
} // namespace

SEASTAR_THREAD_TEST_CASE(test_retry_fail_0) {
    context ctx = {false, false, false};
    auto res = kc::retry_with_mitigation(
      ctx.retries(), 0ms, std::ref(ctx), ctx);
    BOOST_REQUIRE_NO_THROW(res.get());
    BOOST_REQUIRE_EQUAL(ctx.calls(), 1);
}

SEASTAR_THREAD_TEST_CASE(test_retry_fail_1) {
    context ctx = {true, false, false};
    auto res = kc::retry_with_mitigation(
      ctx.retries(), 0ms, std::ref(ctx), ctx);
    BOOST_REQUIRE_NO_THROW(res.get());
    BOOST_REQUIRE_EQUAL(ctx.calls(), 2);
}

SEASTAR_THREAD_TEST_CASE(test_retry_fail_2) {
    context ctx = {true, true, false};
    auto res = kc::retry_with_mitigation(
      ctx.retries(), 0ms, std::ref(ctx), ctx);
    BOOST_REQUIRE_NO_THROW(res.get());
    BOOST_REQUIRE_EQUAL(ctx.calls(), 3);
}

SEASTAR_THREAD_TEST_CASE(test_retry_fail_all) {
    context ctx = {true, true, true};
    size_t errors{};
    kc::retry_with_mitigation(ctx.retries(), 0ms, std::ref(ctx), ctx)
      .handle_exception_type(
        [&errors](const counted_error& ex) { errors = ex.count; })
      .get();
    BOOST_REQUIRE_EQUAL(ctx.calls(), 3);
    BOOST_REQUIRE_EQUAL(errors, 3);
}
