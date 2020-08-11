#include "ssx/future-util.h"

#include <seastar/testing/thread_test_case.hh>

#include <numeric>

auto plus(int inc) {
    return [inc](auto val) { return val + inc; };
}

SEASTAR_THREAD_TEST_CASE(parallel_tansform_iter_test) {
    std::vector<int> input(10);
    std::iota(input.begin(), input.end(), 0);

    std::vector<int> expected(10);
    std::iota(expected.begin(), expected.end(), 2);

    std::vector<int> out_iter
      = ssx::parallel_transform(input.begin(), input.end(), plus(2)).get0();
    BOOST_TEST(std::equal(
      out_iter.begin(), out_iter.end(), expected.begin(), expected.end()));
}

SEASTAR_THREAD_TEST_CASE(parallel_tansform_range_test) {
    std::vector<int> input(10);
    std::iota(input.begin(), input.end(), 0);

    std::vector<int> expected(10);
    std::iota(expected.begin(), expected.end(), 2);

    std::vector<int> out_range
      = ssx::parallel_transform(std::move(input), plus(2)).get0();
    BOOST_TEST(std::equal(
      out_range.begin(), out_range.end(), expected.begin(), expected.end()));
}
