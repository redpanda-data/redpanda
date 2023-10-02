// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/hdr_hist.h"
#include "utils/log_hist.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/testing/perf_tests.hh>

static constexpr int number_of_values_to_record = 100'000'000;

PERF_TEST(hdr_hist, record) {
    hdr_hist h;
    perf_tests::start_measuring_time();
#pragma nounroll
    for (int i = 0; i < number_of_values_to_record; i++) {
        [[clang::noinline]] h.record(i);
    }
    perf_tests::stop_measuring_time();
    perf_tests::do_not_optimize(h.seastar_histogram_logform());
}

PERF_TEST(log_hist, record) {
    log_hist_internal h;
    perf_tests::start_measuring_time();
#pragma nounroll
    for (int i = 0; i < number_of_values_to_record; i++) {
        [[clang::noinline]] h.record(i);
    }
    perf_tests::stop_measuring_time();
    perf_tests::do_not_optimize(h.internal_histogram_logform());
}
