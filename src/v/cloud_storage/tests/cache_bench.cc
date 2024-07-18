/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/seastarx.h"
#include "cloud_storage/access_time_tracker.h"
#include "ssx/sformat.h"

#include <seastar/testing/perf_tests.hh>

static std::chrono::system_clock::time_point make_ts(int64_t val) {
    auto seconds = std::chrono::seconds(val);
    return std::chrono::system_clock::time_point(seconds);
}

static void run_test(int test_scale) {
    cloud_storage::access_time_tracker tracker;
    std::vector<ss::sstring> names;
    for (int i = 0; i < test_scale; i++) {
        names.push_back(ssx::sformat("name-{}", i));
    }

    for (int i = 0; i < test_scale; i++) {
        perf_tests::start_measuring_time();
        tracker.add(names[i % test_scale], make_ts(i), 0);
        perf_tests::stop_measuring_time();
    }
}

PERF_TEST(cache_utils, cm_sketch_1000) { run_test(1000); }

PERF_TEST(cache_utils, cm_sketch_10000) { run_test(10000); }
