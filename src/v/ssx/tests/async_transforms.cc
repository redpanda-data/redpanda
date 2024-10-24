// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "ssx/future-util.h"

#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>

#include <numeric>

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
      = ssx::async_transform(input.begin(), input.end(), plus(2)).get();
    BOOST_TEST(std::equal(
      out_iter.begin(), out_iter.end(), expected.begin(), expected.end()));
}

SEASTAR_THREAD_TEST_CASE(async_transform_range_test) {
    std::vector<int> input(10);
    std::iota(input.begin(), input.end(), 0);

    std::vector<int> expected(10);
    std::iota(expected.begin(), expected.end(), 2);

    std::vector<int> out_range = ssx::async_transform(input, plus(2)).get();
    BOOST_TEST(std::equal(
      out_range.begin(), out_range.end(), expected.begin(), expected.end()));
}

SEASTAR_THREAD_TEST_CASE(async_transform_move_test) {
    std::vector<ss::sstring> input{"hello", "world", "this", "is", "FUN"};
    std::vector<ss::sstring> input_copy = input;
    std::vector<ss::sstring> expected = ssx::async_transform(
                                          input_copy.begin(),
                                          input_copy.end(),
                                          [](ss::sstring s) { return s; })
                                          .get();
    BOOST_TEST(
      std::equal(input.begin(), input.end(), expected.begin(), expected.end()));
}

SEASTAR_THREAD_TEST_CASE(async_transform_noncopyable_test) {
    class noncopyable_foo {
    public:
        noncopyable_foo(int value)
          : _x(value) {}
        noncopyable_foo(const noncopyable_foo&) = delete;
        noncopyable_foo(noncopyable_foo&&) = default;

        int value() const { return _x; }

    private:
        int _x;
    };
    std::vector<noncopyable_foo> foos;
    foos.emplace_back(1);
    foos.emplace_back(2);
    foos.emplace_back(3);
    std::vector<noncopyable_foo> results = ssx::async_transform(
                                             foos.begin(),
                                             foos.end(),
                                             [](noncopyable_foo& ncf) {
                                                 return std::move(ncf);
                                             })
                                             .get();
    BOOST_TEST(results[0].value() == 1);
    BOOST_TEST(results[1].value() == 2);
    BOOST_TEST(results[2].value() == 3);
}

SEASTAR_THREAD_TEST_CASE(parallel_transform_iter_test) {
    std::vector<int> input(10);
    std::iota(input.begin(), input.end(), 0);

    std::vector<int> expected(10);
    std::iota(expected.begin(), expected.end(), 2);

    std::vector<int> out_iter
      = ssx::parallel_transform(input.begin(), input.end(), plus(2)).get();
    BOOST_TEST(std::equal(
      out_iter.begin(), out_iter.end(), expected.begin(), expected.end()));
}

SEASTAR_THREAD_TEST_CASE(parallel_transform_range_test) {
    std::vector<int> input(10);
    std::iota(input.begin(), input.end(), 0);

    std::vector<int> expected(10);
    std::iota(expected.begin(), expected.end(), 2);

    std::vector<int> out_range
      = ssx::parallel_transform(std::move(input), plus(2)).get();
    BOOST_TEST(std::equal(
      out_range.begin(), out_range.end(), expected.begin(), expected.end()));
}
