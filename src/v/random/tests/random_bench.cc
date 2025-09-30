/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "random/generators.h"

#include <seastar/testing/perf_tests.hh>

#include <cstddef>
#include <random>

namespace {

constexpr size_t inner_iters = 10000;

const volatile int dist_upper_bound = 10;

template<typename F>
size_t do_generate(F&& f) {
    for (size_t i = 0; i < inner_iters; ++i) {
        perf_tests::do_not_optimize(f());
    }

    perf_tests::do_not_optimize(f);

    return inner_iters;
}

// return the int_distribution object across an optimization
// barrier to prevent constant-propagation of the bounds (which
// causes the dist calculation to be much cheaper)
template<typename IntType = int>
auto get_dist() {
    std::uniform_int_distribution<IntType> d{0, dist_upper_bound};
    perf_tests::do_not_optimize(&d);
    return d;
}

PERF_TEST(rng_dist, get_int_standalone) {
    int ub = dist_upper_bound;
    return do_generate([=] { return random_generators::get_int<int>(ub); });
}

PERF_TEST(rng_dist, get_int_global) {
    int ub = dist_upper_bound;
    return do_generate(
      [=] { return random_generators::global().get_int<int>(ub); });
}

PERF_TEST(rng_dist, get_int_state) {
    int ub = dist_upper_bound;
    random_generators::rng rng;
    return do_generate([ub, &rng] { return rng.get_int<int>(ub); });
}

PERF_TEST(rng_dist, std_engine_dist) {
    random_generators::rng::engine_type std_engine;
    auto x = std_engine;
    auto dist = get_dist();
    x();
    return do_generate(
      [&, std_engine = std_engine] mutable { return dist(std_engine); });
}

PERF_TEST(rng_dist, std_engine_temp_dist) {
    random_generators::rng::engine_type std_engine;
    return do_generate([&] {
        auto dist = get_dist();
        return dist(std_engine);
    });
}

PERF_TEST(rng_dist, std_dre) {
    std::default_random_engine rng;
    return do_generate([&] {
        auto dist = get_dist();
        return dist(rng);
    });
}

PERF_TEST(rng_dist, pcg32) {
    absl::random_internal::pcg32_2018_engine rng;
    return do_generate([&] {
        auto dist = get_dist();
        return dist(rng);
    });
}

PERF_TEST(rng_dist, pcg64) {
    absl::random_internal::pcg64_2018_engine rng;
    return do_generate([&] {
        auto dist = get_dist();
        return dist(rng);
    });
}

PERF_TEST(rng_raw, std_dre_unknown_seed) {
    std::seed_seq seq{std::random_device{}()};
    std::default_random_engine rng{seq};
    return do_generate([&] { return rng(); });
}

PERF_TEST(rng_raw, std_dre_known_seed) {
    std::seed_seq seq{std::random_device{}()};
    std::default_random_engine rng{seq};
    return do_generate([&] { return rng(); });
}

PERF_TEST(rng_raw, pcg32_raw) {
    absl::random_internal::pcg32_2018_engine rng;
    return do_generate([&] { return rng(); });
}

PERF_TEST(rng_raw, pcg64_raw) {
    absl::random_internal::pcg64_2018_engine rng;
    return do_generate([&] { return rng(); });
}

PERF_TEST(rng_raw, pcg32_dist) {
    absl::random_internal::pcg32_2018_engine rng;
    auto dist = get_dist();
    return do_generate([&] { return dist(rng); });
}

PERF_TEST(rng_raw, pcg64_dist) {
    absl::random_internal::pcg64_2018_engine rng;
    auto dist = get_dist();
    return do_generate([&] { return dist(rng); });
}

PERF_TEST(seeding, random_device_longlived) {
    std::random_device rd;
    return do_generate([&] { return rd(); });
}

PERF_TEST(seeding, random_device_shortlived) {
    return do_generate([] {
        std::random_device rd;
        return rd();
    });
}

PERF_TEST(seeding, absl_make_seed) {
    return do_generate([] { return absl::MakeSeedSeq(); });
}

template<typename T>
auto test_creation() {
    for (size_t i = 0; i < inner_iters; ++i) {
        T rng;
        perf_tests::do_not_optimize(rng);
    }

    return inner_iters;
}

// the "creation" tests are intended to measure the cost of
// creating the rng state objects, not using them.

PERF_TEST(creation, generators_rng) {
    return test_creation<random_generators::rng>();
}

} // namespace
