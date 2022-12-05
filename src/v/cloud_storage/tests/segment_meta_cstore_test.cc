/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/segment_meta_cstore.h"
#include "common_def.h"
#include "random/generators.h"
#include "utils/delta_for.h"
#include "vlog.h"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <algorithm>
#include <limits>

using namespace cloud_storage;

template<class delta_alg>
void append_test_case(const int64_t max_value, delta_alg initial) {
    using frame_t = frame<int64_t, delta_alg>;
    frame_t frame(std::move(initial));
    size_t total_size = 0;
    for (int64_t ix = 0; ix < max_value;
         ix += random_generators::get_int(1, 100)) {
        frame.append(ix);
        total_size++;
        BOOST_REQUIRE_EQUAL(ix, frame.last_value());
    }
    BOOST_REQUIRE_EQUAL(total_size, frame.size());
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_append_xor) {
    append_test_case<details::delta_xor>(10000000, {});
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_append_delta) {
    append_test_case<details::delta_delta<int64_t>>(
      10000000, details::delta_delta<int64_t>(0));
}

template<class delta_alg>
void iter_test_case(const int64_t max_value, delta_alg initial) {
    using frame_t = frame<int64_t, delta_alg>;
    frame_t frame(std::move(initial));
    size_t total_size = 0;
    std::vector<int64_t> expected;
    for (int64_t ix = 0; ix < max_value;
         ix += random_generators::get_int(1, 100)) {
        frame.append(ix);
        expected.push_back(ix);
        total_size++;
    }
    BOOST_REQUIRE_EQUAL(total_size, frame.size());

    int i = 0;
    for (auto actual : frame) {
        BOOST_REQUIRE_EQUAL(actual, expected[i++]);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_iter_xor) {
    iter_test_case<details::delta_xor>(10000000, {});
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_iter_delta) {
    iter_test_case<details::delta_delta<int64_t>>(
      10000000, details::delta_delta<int64_t>(0));
}

template<class delta_alg>
void find_test_case(const int64_t max_value, delta_alg initial) {
    using frame_t = frame<int64_t, delta_alg>;
    frame_t frame(std::move(initial));
    size_t total_size = 0;
    std::vector<int64_t> samples;
    for (int64_t ix = 0; ix < max_value;
         ix += random_generators::get_int(1, 100)) {
        frame.append(ix);
        if (samples.empty() || random_generators::get_int(10) == 0) {
            samples.push_back(ix);
        }
        total_size++;
    }
    BOOST_REQUIRE_EQUAL(total_size, frame.size());
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(samples.begin(), samples.end(), g);

    for (auto expected : samples) {
        auto it = frame.find(expected);
        BOOST_REQUIRE(it != frame.end());
        auto actual = *it;
        BOOST_REQUIRE_EQUAL(actual, expected);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_find_xor) {
    find_test_case<details::delta_xor>(10000000, {});
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_find_delta) {
    find_test_case<details::delta_delta<int64_t>>(
      10000000, details::delta_delta<int64_t>(0));
}

template<class delta_alg>
void lower_bound_test_case(const int64_t max_value, delta_alg initial) {
    using frame_t = frame<int64_t, delta_alg>;
    frame_t frame(std::move(initial));
    size_t total_size = 0;
    std::vector<int64_t> samples;
    int64_t last = 0;
    for (int64_t ix = 10000; ix < max_value;
         ix += random_generators::get_int(1, 100)) {
        frame.append(ix);
        last = ix;
        if (samples.empty() || random_generators::get_int(10) == 0) {
            samples.push_back(ix);
        }
        total_size++;
    }
    BOOST_REQUIRE_EQUAL(total_size, frame.size());
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(samples.begin(), samples.end(), g);

    {
        auto it = frame.lower_bound(last);
        BOOST_REQUIRE_EQUAL(last, *it);
        it = frame.lower_bound(last + 1);
        BOOST_REQUIRE(it == frame.end());
    }

    for (auto expected : samples) {
        auto it = frame.lower_bound(expected);
        BOOST_REQUIRE(it != frame.end());
        auto actual = *it;
        BOOST_REQUIRE_EQUAL(actual, expected);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_lower_bound_xor) {
    lower_bound_test_case<details::delta_xor>(10000000, {});
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_lower_bound_delta) {
    lower_bound_test_case<details::delta_delta<int64_t>>(
      10000000, details::delta_delta<int64_t>(0));
}

template<class delta_alg>
void upper_bound_test_case(const int64_t max_value, delta_alg initial) {
    using frame_t = frame<int64_t, delta_alg>;
    frame_t frame(std::move(initial));
    size_t total_size = 0;
    std::vector<int64_t> samples;
    int64_t last = 0;
    for (int64_t ix = 10000; ix < max_value;
         ix += random_generators::get_int(1, 100)) {
        frame.append(ix);
        if (samples.empty() || random_generators::get_int(10) == 0) {
            samples.push_back(ix);
        }
        last = ix;
        total_size++;
    }
    BOOST_REQUIRE_EQUAL(total_size, frame.size());
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(samples.begin(), samples.end(), g);

    {
        auto it = frame.upper_bound(last);
        BOOST_REQUIRE(it == frame.end());
    }

    for (auto expected : samples) {
        auto it = frame.upper_bound(expected - 1);
        BOOST_REQUIRE(it != frame.end());
        auto actual = *it;
        BOOST_REQUIRE_EQUAL(actual, expected);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_upper_bound_xor) {
    upper_bound_test_case<details::delta_xor>(10000000, {});
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_upper_bound_delta) {
    upper_bound_test_case<details::delta_delta<int64_t>>(
      10000000, details::delta_delta<int64_t>(0));
}

template<class delta_alg>
void at_test_case(const int64_t max_value, delta_alg initial) {
    using frame_t = frame<int64_t, delta_alg>;
    frame_t frame(std::move(initial));
    size_t total_size = 0;
    std::vector<std::pair<int64_t, size_t>> samples;
    for (int64_t value = 0; value < max_value;
         value += random_generators::get_int(1, 100)) {
        frame.append(value);
        if (samples.empty() || random_generators::get_int(10) == 0) {
            samples.emplace_back(value, total_size);
        }
        total_size++;
    }
    BOOST_REQUIRE_EQUAL(total_size, frame.size());
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(samples.begin(), samples.end(), g);

    for (auto [expected, index] : samples) {
        auto it = frame.at(index);
        BOOST_REQUIRE(it != frame.end());
        auto actual = *it;
        BOOST_REQUIRE_EQUAL(actual, expected);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_at_xor) {
    at_test_case<details::delta_xor>(10000000, {});
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_at_delta) {
    at_test_case<details::delta_delta<int64_t>>(
      10000000, details::delta_delta<int64_t>{0});
}
