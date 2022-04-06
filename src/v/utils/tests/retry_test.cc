// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "seastarx.h"
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
                         .get0();
    BOOST_REQUIRE_EQUAL(retry_count, 3);
};

SEASTAR_THREAD_TEST_CASE(retry_then_fail) {
    BOOST_CHECK_THROW(
      retry_with_backoff(2, retry_counter(10), std::chrono::milliseconds(1))
        .get0(),
      std::logic_error);
};
