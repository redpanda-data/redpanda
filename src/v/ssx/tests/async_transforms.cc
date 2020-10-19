#include "ssx/future-util.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>

#include <numeric>
#include <seastarx.h>

auto plus(int inc) {
    return [inc](auto val) { return val + inc; };
}

// The async_transform* tests also implicity tests that invoked callbacks occur
// in lock step. Since the implementation pushes back the result as soon as the
// future becomes available, if this mechanism was working incorrectly, the
// result array would appear to have out of order elements
SEASTAR_THREAD_TEST_CASE(async_transform_iter_test) {
    std::vector<int> input(10);
    std::iota(input.begin(), input.end(), 0);

    std::vector<int> expected(10);
    std::iota(expected.begin(), expected.end(), 2);

    std::vector<int> out_iter
      = ssx::async_transform(input.begin(), input.end(), plus(2)).get0();
    BOOST_TEST(std::equal(
      out_iter.begin(), out_iter.end(), expected.begin(), expected.end()));
}

SEASTAR_THREAD_TEST_CASE(async_transform_range_test) {
    std::vector<int> input(10);
    std::iota(input.begin(), input.end(), 0);

    std::vector<int> expected(10);
    std::iota(expected.begin(), expected.end(), 2);

    std::vector<int> out_range
      = ssx::async_transform(std::move(input), plus(2)).get0();
    BOOST_TEST(std::equal(
      out_range.begin(), out_range.end(), expected.begin(), expected.end()));
}

SEASTAR_THREAD_TEST_CASE(async_transform_move_test) {
    std::vector<ss::sstring> input{"hello", "world", "this", "is", "FUN"};
    std::vector<ss::sstring> input_copy = input;
    std::vector<ss::sstring> expected = ssx::async_transform(
                                          input_copy.begin(),
                                          input_copy.end(),
                                          [](ss::sstring s) {
                                              return std::move(s);
                                          })
                                          .get0();
    BOOST_TEST(
      std::equal(input.begin(), input.end(), expected.begin(), expected.end()));
}

SEASTAR_THREAD_TEST_CASE(parallel_transform_iter_test) {
    std::vector<int> input(10);
    std::iota(input.begin(), input.end(), 0);

    std::vector<int> expected(10);
    std::iota(expected.begin(), expected.end(), 2);

    std::vector<int> out_iter
      = ssx::parallel_transform(input.begin(), input.end(), plus(2)).get0();
    BOOST_TEST(std::equal(
      out_iter.begin(), out_iter.end(), expected.begin(), expected.end()));
}

SEASTAR_THREAD_TEST_CASE(parallel_transform_range_test) {
    std::vector<int> input(10);
    std::iota(input.begin(), input.end(), 0);

    std::vector<int> expected(10);
    std::iota(expected.begin(), expected.end(), 2);

    std::vector<int> out_range
      = ssx::parallel_transform(std::move(input), plus(2)).get0();
    BOOST_TEST(std::equal(
      out_range.begin(), out_range.end(), expected.begin(), expected.end()));
}
