/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "random/fast_prng.h"

#include <seastar/testing/perf_tests.hh>

#include <random>

constexpr unsigned int seed = 0;
constexpr int iterations = 10000;

template<typename random_source_t>
void bench_source() {
    random_source_t s(seed);

    perf_tests::start_measuring_time();
    for (int i = 0; i < iterations; i++) {
        auto r = s();
        perf_tests::do_not_optimize(r);
    }
    perf_tests::stop_measuring_time();
}

PERF_TEST(random, fast_prng) { bench_source<fast_prng>(); }
PERF_TEST(random, minstd_rand0) { bench_source<std::minstd_rand0>(); }
PERF_TEST(random, minstd_rand) { bench_source<std::minstd_rand>(); }
PERF_TEST(random, mt19937) { bench_source<std::mt19937>(); }
PERF_TEST(random, mt19937_64) { bench_source<std::mt19937_64>(); }
PERF_TEST(random, ranlux24_base) { bench_source<std::ranlux24_base>(); }
PERF_TEST(random, ranlux48_base) { bench_source<std::ranlux48_base>(); }
PERF_TEST(random, ranlux24) { bench_source<std::ranlux24>(); }
PERF_TEST(random, ranlux48) { bench_source<std::ranlux48>(); }
PERF_TEST(random, knuth_b) { bench_source<std::knuth_b>(); }
PERF_TEST(random, default_random_engine) {
    bench_source<std::default_random_engine>();
}
PERF_TEST(random, random_device) { std::random_device()(); }
