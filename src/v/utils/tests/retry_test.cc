#include "utils/retry.h"

#include <seastar/core/future.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <stdexcept>

struct retry_counter {
    retry_counter(int s)
      : success_after(s) {}

    seastar::future<int> operator()() {
        cnt++;
        if (cnt > success_after) {
            return seastar::make_ready_future<int>(cnt);
        }
        return seastar::make_exception_future<int>(
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