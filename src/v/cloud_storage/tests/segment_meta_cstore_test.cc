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

using delta_xor_alg = details::delta_xor;
using delta_xor_frame = segment_meta_column_frame<int64_t, delta_xor_alg>;
using delta_delta_alg = details::delta_delta<int64_t>;
using delta_delta_frame = segment_meta_column_frame<int64_t, delta_delta_alg>;
using delta_xor_column = segment_meta_column<int64_t, delta_xor_alg>;
using delta_delta_column = segment_meta_column<int64_t, delta_delta_alg>;

static const delta_xor_alg initial_xor{};
static const delta_delta_alg initial_delta{0};

template<class column_t>
void append_test_case(const int64_t num_elements, column_t& column) {
    size_t total_size = 0;
    int64_t ix = 0;
    for (int64_t i = 0; i < num_elements; i++) {
        ix += random_generators::get_int(1, 100);
        column.append(ix);
        total_size++;
        BOOST_REQUIRE_EQUAL(ix, column.last_value());
    }
    BOOST_REQUIRE_EQUAL(total_size, column.size());
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_append_xor) {
    delta_xor_frame frame(initial_xor);
    append_test_case(100000, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_append_delta) {
    delta_delta_frame frame(initial_delta);
    append_test_case(100000, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_append_xor) {
    delta_xor_column col(initial_xor);
    append_test_case(100000, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_append_delta) {
    delta_delta_column col(initial_delta);
    append_test_case(100000, col);
}

template<class column_t>
void append_tx_test_case(const int64_t num_elements, column_t& column) {
    size_t total_size = 0;
    int64_t ix = 0;
    for (int64_t i = 0; i < num_elements; i++) {
        ix += random_generators::get_int(1, 100);
        auto tx = column.append_tx(ix);
        if (tx) {
            tx->commit();
        }
        total_size++;
        BOOST_REQUIRE_EQUAL(ix, column.last_value());
    }
    BOOST_REQUIRE_EQUAL(total_size, column.size());
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_append_tx_xor) {
    delta_xor_frame frame(initial_xor);
    append_tx_test_case(100000, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_append_tx_delta) {
    delta_delta_frame frame(initial_delta);
    append_tx_test_case(100000, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_append_tx_xor) {
    delta_xor_column col(initial_xor);
    append_tx_test_case(100000, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_append_tx_delta) {
    delta_delta_column col(initial_delta);
    append_tx_test_case(100000, col);
}

template<class column_t>
void iter_test_case(const int64_t num_elements, column_t& column) {
    size_t total_size = 0;
    std::vector<int64_t> expected;
    int64_t ix = 0;
    for (int64_t i = 0; i < num_elements; i++) {
        ix += random_generators::get_int(1, 100);
        column.append(ix);
        expected.push_back(ix);
        total_size++;
    }
    BOOST_REQUIRE_EQUAL(total_size, column.size());

    int i = 0;
    for (auto actual : column) {
        BOOST_REQUIRE_EQUAL(actual, expected[i++]);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_iter_xor) {
    delta_xor_frame frame(initial_xor);
    iter_test_case(100000, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_iter_delta) {
    delta_delta_frame frame(initial_delta);
    iter_test_case(100000, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_iter_xor) {
    delta_xor_column col(initial_xor);
    iter_test_case(100000, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_iter_delta) {
    delta_delta_column col(initial_delta);
    iter_test_case(100000, col);
}

template<class column_t>
void find_test_case(const int64_t num_elements, column_t& column) {
    size_t total_size = 0;
    std::vector<int64_t> samples;
    int64_t ix = 0;
    for (auto i = 0; i < num_elements; i++) {
        ix += random_generators::get_int(1, 100);
        column.append(ix);
        if (samples.empty() || random_generators::get_int(10) == 0) {
            samples.push_back(ix);
        }
        total_size++;
    }
    BOOST_REQUIRE_EQUAL(total_size, column.size());
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(samples.begin(), samples.end(), g);

    for (auto expected : samples) {
        auto it = column.find(expected);
        BOOST_REQUIRE(it != column.end());
        auto actual = *it;
        BOOST_REQUIRE_EQUAL(actual, expected);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_find_xor) {
    delta_xor_frame frame(initial_xor);
    find_test_case(100000, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_find_xor_small) {
    delta_xor_frame frame(initial_xor);
    find_test_case(random_generators::get_int(1, 16), frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_find_delta) {
    delta_delta_frame frame(initial_delta);
    find_test_case(100000, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_find_delta_small) {
    delta_delta_frame frame(initial_delta);
    find_test_case(random_generators::get_int(1, 16), frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_find_xor) {
    delta_xor_column col(initial_xor);
    find_test_case(100000, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_find_xor_small) {
    delta_xor_column col(initial_xor);
    find_test_case(random_generators::get_int(1, 16), col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_find_delta) {
    delta_delta_column col(initial_delta);
    find_test_case(100000, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_find_delta_small) {
    delta_delta_column col(initial_delta);
    find_test_case(random_generators::get_int(1, 16), col);
}

template<class column_t>
void lower_bound_test_case(const int64_t num_elements, column_t& column) {
    size_t total_size = 0;
    std::vector<int64_t> samples;
    int64_t last = 0;
    int64_t ix = 10000;
    for (auto i = 0; i < num_elements; i++) {
        ix += random_generators::get_int(1, 100);
        column.append(ix);
        last = ix;
        if (samples.empty() || random_generators::get_int(10) == 0) {
            samples.push_back(ix);
        }
        total_size++;
    }
    BOOST_REQUIRE_EQUAL(total_size, column.size());
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(samples.begin(), samples.end(), g);

    {
        auto it = column.lower_bound(last);
        BOOST_REQUIRE_EQUAL(last, *it);
        it = column.lower_bound(last + 1);
        BOOST_REQUIRE(it == column.end());
    }

    for (auto expected : samples) {
        auto it = column.lower_bound(expected);
        BOOST_REQUIRE(it != column.end());
        auto actual = *it;
        BOOST_REQUIRE_EQUAL(actual, expected);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_lower_bound_xor) {
    delta_xor_frame frame(initial_xor);
    lower_bound_test_case(100000, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_lower_bound_xor_small) {
    delta_xor_frame frame(initial_xor);
    lower_bound_test_case(random_generators::get_int(1, 16), frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_lower_bound_delta) {
    delta_delta_frame frame(initial_delta);
    lower_bound_test_case(100000, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_lower_bound_delta_small) {
    delta_delta_frame frame(initial_delta);
    lower_bound_test_case(random_generators::get_int(1, 16), frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_lower_bound_xor) {
    delta_xor_column col(initial_xor);
    lower_bound_test_case(100000, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_lower_bound_xor_small) {
    delta_xor_column col(initial_xor);
    lower_bound_test_case(random_generators::get_int(1, 16), col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_lower_bound_delta) {
    delta_delta_column col(initial_delta);
    lower_bound_test_case(100000, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_lower_bound_delta_small) {
    delta_delta_column col(initial_delta);
    lower_bound_test_case(random_generators::get_int(1, 16), col);
}

template<class column_t>
void upper_bound_test_case(const int64_t num_elements, column_t& column) {
    size_t total_size = 0;
    std::vector<int64_t> samples;
    int64_t last = 0;
    int64_t ix = 10000;
    for (auto i = 0; i < num_elements; i++) {
        ix += random_generators::get_int(1, 100);
        column.append(ix);
        if (samples.empty() || random_generators::get_int(10) == 0) {
            samples.push_back(ix);
        }
        last = ix;
        total_size++;
    }
    BOOST_REQUIRE_EQUAL(total_size, column.size());
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(samples.begin(), samples.end(), g);

    {
        auto it = column.upper_bound(last);
        BOOST_REQUIRE(it == column.end());
    }

    for (auto expected : samples) {
        auto it = column.upper_bound(expected - 1);
        BOOST_REQUIRE(it != column.end());
        auto actual = *it;
        BOOST_REQUIRE_EQUAL(actual, expected);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_upper_bound_xor) {
    delta_xor_frame frame(initial_xor);
    upper_bound_test_case(100000, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_upper_bound_xor_small) {
    delta_xor_frame frame(initial_xor);
    upper_bound_test_case(random_generators::get_int(1, 16), frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_upper_bound_delta) {
    delta_delta_frame frame(initial_delta);
    upper_bound_test_case(100000, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_upper_bound_delta_small) {
    delta_delta_frame frame(initial_delta);
    upper_bound_test_case(random_generators::get_int(1, 16), frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_upper_bound_xor) {
    delta_xor_column col(initial_xor);
    upper_bound_test_case(100000, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_upper_bound_xor_small) {
    delta_xor_column col(initial_xor);
    upper_bound_test_case(random_generators::get_int(1, 16), col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_upper_bound_delta) {
    delta_delta_column col(initial_delta);
    upper_bound_test_case(100000, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_upper_bound_delta_small) {
    delta_delta_column col(initial_delta);
    upper_bound_test_case(random_generators::get_int(1, 16), col);
}

template<class column_t>
void at_test_case(const int64_t num_elements, column_t& column) {
    size_t total_size = 0;
    std::vector<std::pair<int64_t, size_t>> samples;
    int64_t value = 0;
    for (int64_t i = 0; i < num_elements; i++) {
        value += random_generators::get_int(1, 100);
        column.append(value);
        if (samples.empty() || random_generators::get_int(10) == 0) {
            samples.emplace_back(value, total_size);
        }
        total_size++;
    }
    BOOST_REQUIRE_EQUAL(total_size, column.size());
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(samples.begin(), samples.end(), g);

    for (auto [expected, index] : samples) {
        auto it = column.at(index);
        BOOST_REQUIRE(it != column.end());
        auto actual = *it;
        BOOST_REQUIRE_EQUAL(actual, expected);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_at_xor) {
    delta_xor_frame frame(initial_xor);
    at_test_case(100000, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_at_xor_small) {
    delta_xor_frame frame(initial_xor);
    at_test_case(random_generators::get_int(16), frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_at_delta) {
    delta_delta_frame frame(initial_delta);
    at_test_case(100000, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_at_delta_small) {
    delta_delta_frame frame(initial_delta);
    at_test_case(random_generators::get_int(16), frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_at_xor) {
    delta_xor_column col(initial_xor);
    at_test_case(100000, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_at_xor_small) {
    delta_xor_column col(initial_xor);
    at_test_case(random_generators::get_int(16), col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_at_delta) {
    delta_delta_column col(initial_delta);
    at_test_case(100000, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_at_delta_small) {
    delta_delta_column col(initial_delta);
    at_test_case(random_generators::get_int(16), col);
}

template<class column_t>
void prefix_truncate_test_case(const int64_t num_elements, column_t& column) {
    size_t total_size = 0;
    std::vector<int64_t> samples;
    int64_t value = 0;
    for (int64_t i = 0; i < num_elements; i++) {
        value += random_generators::get_int(1, 100);
        column.append(value);
        if (samples.empty() || random_generators::get_int(10) == 0) {
            samples.push_back(value);
        }
        total_size++;
    }
    BOOST_REQUIRE_EQUAL(total_size, column.size());

    for (auto value : samples) {
        column.prefix_truncate(value);
        auto it = column.begin();
        BOOST_REQUIRE(it != column.end());
        auto actual = *it;
        BOOST_REQUIRE_EQUAL(actual, value);
    }
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_prefix_truncate_xor) {
    delta_xor_frame frame(initial_xor);
    prefix_truncate_test_case(10, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_frame_prefix_truncate_delta) {
    delta_delta_frame frame(initial_delta);
    prefix_truncate_test_case(10, frame);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_prefix_truncate_xor) {
    delta_xor_column col(initial_xor);
    prefix_truncate_test_case(10, col);
}

BOOST_AUTO_TEST_CASE(test_segment_meta_cstore_col_prefix_truncate_delta) {
    delta_delta_column col(initial_delta);
    prefix_truncate_test_case(10, col);
}
