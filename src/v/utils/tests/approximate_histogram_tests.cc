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

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_suite.hpp>

BOOST_AUTO_TEST_CASE(hist_basic_test) {
    using approx_hist_t = memory_histogram_t;
    approx_hist_t hist;

    for (uint64_t i = 0; i < 100000; i++) {
        for (auto j = 4; j < 20; j++) {
            hist += (size_t)1 << j;
        }
    }

    for (size_t i = 0; i < 16; i++) {
        BOOST_REQUIRE(hist[i] >= 1000);
    }
}

BOOST_AUTO_TEST_CASE(counter_max_counts) {
    auto count_1 = approximate_count<uint8_t, 30>::max_count();
    BOOST_REQUIRE(count_1 == 128331);
    auto count_2 = approximate_count<uint8_t, 20>::max_count();
    BOOST_REQUIRE(count_2 == 5061737);
    auto count_3 = approximate_count<uint16_t, 5000>::max_count();
    BOOST_REQUIRE(count_3 == 2458655844);
}

BOOST_AUTO_TEST_CASE(counter_min_counts) {
    // The first increment to any counter should always be counted.

    approximate_count<uint8_t, 30> counter_1;
    ++counter_1;
    BOOST_REQUIRE(counter_1.get() >= 1);

    approximate_count<uint8_t, 20> counter_2;
    ++counter_2;
    BOOST_REQUIRE(counter_2.get() >= 1);

    approximate_count<uint16_t, 5000> counter_3;
    ++counter_3;
    BOOST_REQUIRE(counter_3.get() >= 1);
}

BOOST_AUTO_TEST_CASE(size_to_buckets_test) {
    size_to_buckets<16, 4> hash{};
    for (auto i = 4; i < 16 + 4; i++) {
        BOOST_REQUIRE(hash(1 << i) == i - 4);
    }

    BOOST_REQUIRE(hash(1) == 0);
    BOOST_REQUIRE(hash((size_t)1 << 60) == 15);
}

BOOST_AUTO_TEST_CASE(memory_histogram_t_size) {
    BOOST_REQUIRE(sizeof(memory_histogram_t) == 16);
}
