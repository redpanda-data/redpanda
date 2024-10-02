// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "utils/retry.h"

#include <seastar/core/future.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <stdexcept>

struct retry_counter {
    retry_counter(int s)
      : success_after(s) {}

    ss::future<int> operator()() {
        cnt++;
        if (cnt > success_after) {
            return ss::make_ready_future<int>(cnt);
        }
        return ss::make_exception_future<int>(
          std::logic_error("Expected error"));
    }

    int success_after;
    int cnt = 0;
};

SEASTAR_THREAD_TEST_CASE(retry_then_succed) {
    auto retry_count = retry_with_backoff(
                         5, retry_counter(2), std::chrono::milliseconds(1))
                         .get();
    BOOST_REQUIRE_EQUAL(retry_count, 3);
};

SEASTAR_THREAD_TEST_CASE(retry_then_fail) {
    BOOST_CHECK_THROW(
      retry_with_backoff(2, retry_counter(10), std::chrono::milliseconds(1))
        .get(),
      std::logic_error);
};

SEASTAR_THREAD_TEST_CASE(retry_then_fail_when_cancelled) {
    ss::abort_source as;

    // a retry that doesn't do anything except fail and retry, and should be
    // configured to run longer than the cancel sleep below because we want the
    // cancel sleep to drive an abort of this retry.
    auto retry_count = retry_with_backoff(
      100,
      [] { return ss::make_exception_future<>(std::runtime_error("error")); },
      std::chrono::milliseconds(5),
      as);

    // after 50 milliseconds an abort will be reuqested
    auto cancel = ss::sleep(std::chrono::milliseconds(50)).then([&as] {
        as.request_abort();
    });

    /// When the retry_with_backoff() method is exited early due to the abort
    /// source request, it will return with an exceptional future
    auto cancelled = ss::when_all_succeed(
                       std::move(retry_count), std::move(cancel))
                       .then([](auto) { return false; })
                       .handle_exception([](std::exception_ptr eptr) {
                           try {
                               std::rethrow_exception(std::move(eptr));
                           } catch (const ss::abort_requested_exception&) {
                               return true;
                           } catch (...) {
                               return false;
                           }
                       })
                       .get();
    BOOST_REQUIRE(cancelled);
};
