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

#include "base/type_traits.h"
#include "container/fragmented_vector.h"
#include "container/tests/bench_utils.h"
#include "random/generators.h"

#include <seastar/testing/perf_tests.hh>

#include <algorithm>
#include <iterator>
#include <vector>

template<typename Vector, size_t Size>
class VectorBenchTest {
    static auto make_value() {
        return ::make_value<typename Vector::value_type>();
    }
    static auto make_filled() {
        Vector v;
        std::generate_n(std::back_inserter(v), Size, make_value);
        return v;
    }

public:
    void run_sort_test() {
        auto v = make_filled();
        perf_tests::start_measuring_time();
        std::sort(v.begin(), v.end());
        perf_tests::stop_measuring_time();
    }

    void run_fifo_test() {
        Vector v;
        auto val = make_value();
        perf_tests::start_measuring_time();
        for (size_t i = 0; i < Size; ++i) {
            v.push_back(val);
        }
        for (const auto& e : v) {
            perf_tests::do_not_optimize(e);
        }
        perf_tests::stop_measuring_time();
    }

    void run_lifo_test() {
        Vector v;
        auto val = make_value();
        perf_tests::start_measuring_time();
        for (size_t i = 0; i < Size; ++i) {
            v.push_back(val);
        }
        while (!v.empty()) {
            v.pop_back();
        }
        perf_tests::stop_measuring_time();
    }

    void run_fill_test() {
        Vector v;
        auto val = make_value();
        perf_tests::start_measuring_time();
        std::fill_n(std::back_inserter(v), Size, val);
        perf_tests::stop_measuring_time();
    }

    void run_random_access_test() {
        auto v = make_filled();
        std::vector<size_t> indexes;
        std::generate_n(std::back_inserter(indexes), 1000, [&v]() {
            return random_generators::get_int<size_t>(0, v.size() - 1);
        });
        perf_tests::start_measuring_time();
        perf_tests::do_not_optimize(v.front());
        perf_tests::do_not_optimize(v.back());
        for (size_t index : indexes) {
            perf_tests::do_not_optimize(v[index]);
        }
        perf_tests::do_not_optimize(v.front());
        perf_tests::do_not_optimize(v.back());
        perf_tests::stop_measuring_time();
    }
};

// NOLINTBEGIN(*-macro-*)
#define VECTOR_PERF_TEST(container, element, size)                             \
    class VectorBenchTest_##container##_##element##_##size                     \
      : public VectorBenchTest<container<element>, size> {};                   \
    PERF_TEST_F(VectorBenchTest_##container##_##element##_##size, Sort) {      \
        run_sort_test();                                                       \
    }                                                                          \
    PERF_TEST_F(VectorBenchTest_##container##_##element##_##size, Fifo) {      \
        run_fifo_test();                                                       \
    }                                                                          \
    PERF_TEST_F(VectorBenchTest_##container##_##element##_##size, Lifo) {      \
        run_lifo_test();                                                       \
    }                                                                          \
    PERF_TEST_F(VectorBenchTest_##container##_##element##_##size, Fill) {      \
        run_fill_test();                                                       \
    }                                                                          \
    PERF_TEST_F(                                                               \
      VectorBenchTest_##container##_##element##_##size, RandomAccess) {        \
        run_random_access_test();                                              \
    }
// NOLINTEND(*-macro-*)

template<typename T>
using std_vector = std::vector<T>;
using ss::sstring;

VECTOR_PERF_TEST(std_vector, int64_t, 64)
VECTOR_PERF_TEST(fragmented_vector, int64_t, 64)
VECTOR_PERF_TEST(chunked_vector, int64_t, 64)

VECTOR_PERF_TEST(std_vector, sstring, 64)
VECTOR_PERF_TEST(fragmented_vector, sstring, 64)
VECTOR_PERF_TEST(chunked_vector, sstring, 64)

VECTOR_PERF_TEST(std_vector, large_struct, 64)
VECTOR_PERF_TEST(fragmented_vector, large_struct, 64)
VECTOR_PERF_TEST(chunked_vector, large_struct, 64)

VECTOR_PERF_TEST(std_vector, int64_t, 10000)
VECTOR_PERF_TEST(fragmented_vector, int64_t, 10000)
VECTOR_PERF_TEST(chunked_vector, int64_t, 10000)

VECTOR_PERF_TEST(std_vector, sstring, 10000)
VECTOR_PERF_TEST(fragmented_vector, sstring, 10000)
VECTOR_PERF_TEST(chunked_vector, sstring, 10000)

VECTOR_PERF_TEST(std_vector, large_struct, 10000)
VECTOR_PERF_TEST(fragmented_vector, large_struct, 10000)
VECTOR_PERF_TEST(chunked_vector, large_struct, 10000)

VECTOR_PERF_TEST(std_vector, int64_t, 1048576)
VECTOR_PERF_TEST(fragmented_vector, int64_t, 1048576)
VECTOR_PERF_TEST(chunked_vector, int64_t, 1048576)
