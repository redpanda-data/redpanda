/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "container/chunked_vector.h"
#include "container/tests/bench_utils.h"
#include "random/generators.h"

#include <seastar/testing/perf_tests.hh>

#include <algorithm>
#include <concepts>
#include <iterator>
#include <vector>

template<typename Vector, size_t Size>
struct VectorBenchTest {
    using value_type = typename Vector::value_type;

    static constexpr size_t inner_iters = std::max<size_t>(
      1, 10000 / std::max<size_t>(1, Size));
    static constexpr size_t access_inner_iters = 100;

    const value_type val = make_value();
    const Vector filled = make_filled();

    // copy overloads copy a T either via ctor (this one)
    auto copy(const std::copy_constructible auto& t) { return t; }

    // or a .copy method (this one)
    template<typename T>
    requires requires(const T& t) { t.copy(); }
    auto copy(const T& t) {
        return t.copy();
    }

    static auto make_value() {
        return ::make_value<typename Vector::value_type>();
    }

    static Vector make_filled() {
        Vector v;
        std::generate_n(std::back_inserter(v), Size, make_value);
        return v;
    }

    std::vector<size_t> indexes = [] {
        random_generators::rng rng{0};
        std::vector<size_t> indexes;
        std::generate_n(std::back_inserter(indexes), 1000, [&]() {
            return rng.get_int(0uz, Size - 1);
        });
        return indexes;
    }();

    [[gnu::noinline]]
    size_t run_sort_test() {
        for (size_t iter = 0; iter < inner_iters; ++iter) {
            auto v = copy(filled);
            std::sort(v.begin(), v.end());
            perf_tests::do_not_optimize(v);
        }
        return inner_iters;
    }

    [[gnu::noinline]]
    size_t run_lifo_test() {
        for (size_t iter = 0; iter < inner_iters; ++iter) {
            Vector v;
            for (size_t i = 0; i < Size; ++i) {
                v.push_back(val);
            }
            while (!v.empty()) {
                v.pop_back();
            }
        }
        return inner_iters;
    }

    [[gnu::noinline]]
    size_t run_fill_test() {
        for (size_t iter = 0; iter < inner_iters; ++iter) {
            Vector v = make_filled();
            perf_tests::do_not_optimize(v);
        }
        return inner_iters;
    }

    [[gnu::noinline]]
    size_t run_random_access_test() {
        for (size_t iter = 0; iter < access_inner_iters; ++iter) {
            for (size_t index : indexes) {
                perf_tests::do_not_optimize(filled[index]);
            }
        }
        return access_inner_iters;
    }
};

// NOLINTBEGIN(*-macro-*)
#define VECTOR_PERF_TEST(container, element, size)                             \
    class VectorBenchTest_##container##_##element##_##size                     \
      : public VectorBenchTest<container<element>, size> {};                   \
    PERF_TEST_F(VectorBenchTest_##container##_##element##_##size, Sort) {      \
        return run_sort_test();                                                \
    }                                                                          \
    PERF_TEST_F(VectorBenchTest_##container##_##element##_##size, Lifo) {      \
        return run_lifo_test();                                                \
    }                                                                          \
    PERF_TEST_F(VectorBenchTest_##container##_##element##_##size, Fill) {      \
        return run_fill_test();                                                \
    }                                                                          \
    PERF_TEST_F(                                                               \
      VectorBenchTest_##container##_##element##_##size, RandomAccess) {        \
        return run_random_access_test();                                       \
    }
// NOLINTEND(*-macro-*)

template<typename T>
using std_vector = std::vector<T>;
using ss::sstring;

VECTOR_PERF_TEST(std_vector, int64_t, 64)
VECTOR_PERF_TEST(chunked_vector, int64_t, 64)

VECTOR_PERF_TEST(std_vector, sstring, 64)
VECTOR_PERF_TEST(chunked_vector, sstring, 64)

VECTOR_PERF_TEST(std_vector, large_struct, 64)
VECTOR_PERF_TEST(chunked_vector, large_struct, 64)

VECTOR_PERF_TEST(std_vector, int64_t, 10000)
VECTOR_PERF_TEST(chunked_vector, int64_t, 10000)

VECTOR_PERF_TEST(std_vector, sstring, 10000)
VECTOR_PERF_TEST(chunked_vector, sstring, 10000)

VECTOR_PERF_TEST(std_vector, large_struct, 10000)
VECTOR_PERF_TEST(chunked_vector, large_struct, 10000)

VECTOR_PERF_TEST(std_vector, int64_t, 1048576)
VECTOR_PERF_TEST(chunked_vector, int64_t, 1048576)
