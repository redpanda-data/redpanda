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
#include "utils/approximate_histogram.h"
#include "vassert.h"

#include <seastar/testing/perf_tests.hh>

#include <cstdint>

constexpr uint64_t iterations = 10000000;

void adding_basic() {
    uint64_t i = 0, j = 0;
    perf_tests::start_measuring_time();
    for (; i < iterations; i++) {
        j += 1;
        perf_tests::do_not_optimize(j);
    }
    perf_tests::stop_measuring_time();
    vassert(j == iterations, "j != iterations");
}

void adding_histogram() {
    using approx_hist_t
      = approximate_histogram<size_t, uint8_t, 16, 30, size_to_buckets<16, 4>>;
    approx_hist_t hist;
    size_t val = 1 << 3;

    uint64_t i = 0;
    perf_tests::start_measuring_time();
    for (; i < iterations; i++) {
        hist += val;
        perf_tests::do_not_optimize(i);
    }
    perf_tests::stop_measuring_time();

    perf_tests::do_not_optimize(hist);
}

PERF_TEST(approximate_histogram, adding_basic) { adding_basic(); }
PERF_TEST(approximate_histogram, adding_histogram) { adding_histogram(); }
