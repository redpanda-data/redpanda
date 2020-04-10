#include "outcome_future_utils.h"

#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(test_result_wrap) {
    constexpr bool plain_type = std::is_same_v<result_wrap_t<int>, result<int>>;

    constexpr bool future
      = std::is_same_v<result_wrap_t<ss::future<int>>, ss::future<result<int>>>;

    constexpr bool future_result = std::is_same_v<
      result_wrap_t<ss::future<result<int>>>,
      ss::future<result<int>>>;

    constexpr bool already_a_result
      = std::is_same_v<result_wrap_t<result<int>>, result<int>>;

    BOOST_REQUIRE_EQUAL(future, true);
    BOOST_REQUIRE_EQUAL(plain_type, true);
    BOOST_REQUIRE_EQUAL(future_result, true);
    BOOST_REQUIRE_EQUAL(already_a_result, true);
}