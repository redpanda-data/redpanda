// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "ssx/async_algorithm.h"

#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/testing/perf_tests.hh>
#include <seastar/util/later.hh>

#include <cstdlib>
#include <iterator>
#include <random>

using std::ranges::generate_n;

constexpr size_t BIG_ELEM_COUNT = 1000;
constexpr size_t SMALL_ELEM_COUNT = 1;

// Scale iterations inversely with element count so that
// even small-element tests do enough work per start/stop
// timing pair to avoid excessive measurement overhead.
constexpr size_t iterations_for(size_t elem_count) {
    return std::max<size_t>(1000, 1000000 / std::max<size_t>(1, elem_count));
}

namespace {
using vec_t = std::vector<int>;

std::vector<int> rand_vec(ssize_t size) {
    std::vector<int> data;
    std::default_random_engine engine;
    std::uniform_int_distribution<int> dist;
    generate_n(
      std::back_inserter(data), size, [&]() mutable { return dist(engine); });
    return data;
}

struct sum {
    void operator()(int i) {
        perf_tests::do_not_optimize(i); // prevent vectorization
        sum += i;
    }
    uint64_t sum = 0;
};

// split up the inner loop into two variations so that
// the "sync" version doesn't have a co_await in the loop
ss::future<> coro_inner(vec_t& data, auto fn, sum& s, size_t iters) {
    for (size_t i = 0; i < iters; i++) {
        co_await fn(data, s);
    }
}

ss::future<> sync_inner(vec_t& data, auto fn, sum& s, size_t iters) {
    for (size_t i = 0; i < iters; i++) {
        fn(data, s);
    }
    return ss::now();
}

template<size_t elem_count, bool async_inner = true>
ss::future<size_t> run_test_coro(auto fn) {
    constexpr auto iters = iterations_for(elem_count);
    auto data = rand_vec(elem_count);
    sum s;

    perf_tests::start_measuring_time();
    if constexpr (async_inner) {
        co_await coro_inner(data, fn, s, iters);
    } else {
        co_await sync_inner(data, fn, s, iters);
    }
    perf_tests::stop_measuring_time();

    perf_tests::do_not_optimize(s);
    co_return iters;
}

} // namespace

const auto sync_std_for_each = [](vec_t& data, auto fn) {
    std::for_each(data.begin(), data.end(), fn);
};

const auto std_for_each = [](vec_t& data, auto fn) {
    std::for_each(data.begin(), data.end(), fn);
    return ss::now();
};

const auto yielding_loop = [](vec_t& data, auto fn) -> ss::future<> {
    for (auto e : data) {
        fn(e);
        co_await ss::yield();
    }
};

const auto maybe_yield_loop = [](vec_t& data, auto fn) -> ss::future<> {
    for (auto e : data) {
        fn(e);
        co_await ss::maybe_yield();
    }
};

const auto coro_maybe_yield_loop = [](vec_t& data, auto fn) -> ss::future<> {
    for (auto e : data) {
        fn(e);
        co_await ss::coroutine::maybe_yield();
    }
};

const auto async_for_each = [](vec_t& data, auto fn) -> ss::future<> {
    return ssx::async_for_each(data.begin(), data.end(), fn);
};

// x-macro
#define ALL_TESTS_X(f, suffix, count, is_async)                                \
    f(std_for_each, suffix, count, is_async)                                   \
      f(yielding_loop, suffix, count, is_async)                                \
        f(maybe_yield_loop, suffix, count, is_async)                           \
          f(coro_maybe_yield_loop, suffix, count, is_async)                    \
            f(async_for_each, suffix, count, is_async)

#define MAKE_TEST(inner_function, suffix, count, is_async)                     \
    PERF_TEST(algo_bench, inner_function##_##suffix) {                         \
        return run_test_coro<count, is_async>(inner_function);                 \
    }

// generate all the tests for both big and small inner arrays, plus for the
// sync_baseline also a variation of the test where the ITERATIONS loop is
// also sync, which is basically a full sync version of the test (the other
// variations can't run in that mode as their inner methods are in fact async)
MAKE_TEST(sync_std_for_each, big, BIG_ELEM_COUNT, false)
ALL_TESTS_X(MAKE_TEST, big, BIG_ELEM_COUNT, true)
MAKE_TEST(sync_std_for_each, small, SMALL_ELEM_COUNT, false)
ALL_TESTS_X(MAKE_TEST, small, SMALL_ELEM_COUNT, true)
