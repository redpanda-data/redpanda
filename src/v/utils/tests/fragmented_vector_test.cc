/*
 * Copyright 2021 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "random/generators.h"
#include "serde/serde.h"
#include "utils/fragmented_vector.h"

#include <boost/test/unit_test.hpp>

#include <vector>

template<typename T>
static void
test_equal(std::vector<T>& truth, fragmented_vector<T, 1024>& other) {
    BOOST_REQUIRE(!truth.empty());
    BOOST_REQUIRE_EQUAL_COLLECTIONS(
      truth.begin(), truth.end(), other.begin(), other.end());
    BOOST_REQUIRE_EQUAL(truth.empty(), other.empty());
    BOOST_REQUIRE_EQUAL(truth.size(), other.size());
    BOOST_REQUIRE_EQUAL(truth.back(), other.back());
}

BOOST_AUTO_TEST_CASE(fragmented_vector_test) {
    std::vector<int64_t> truth;
    fragmented_vector<int64_t, 1024> other;

    for (int64_t i = 0; i < 2500; i++) {
        truth.push_back(i);
        other.push_back(i);
        test_equal(truth, other);

        other = serde::from_iobuf<decltype(other)>(
          serde::to_iobuf(std::move(other)));
        test_equal(truth, other);
    }

    for (int64_t i = 0; i < 1234; i++) {
        truth.pop_back();
        other.pop_back();
        test_equal(truth, other);

        other = serde::from_iobuf<decltype(other)>(
          serde::to_iobuf(std::move(other)));
        test_equal(truth, other);
    }

    for (int64_t i = 0; i < 123; i++) {
        truth.push_back(i);
        other.push_back(i);
        test_equal(truth, other);

        other = serde::from_iobuf<decltype(other)>(
          serde::to_iobuf(std::move(other)));
        test_equal(truth, other);
    }

    for (int64_t i = 0; i < 1389; i++) {
        test_equal(truth, other);
        truth.pop_back();
        other.pop_back();

        other = serde::from_iobuf<decltype(other)>(
          serde::to_iobuf(std::move(other)));
    }

    BOOST_REQUIRE_EQUAL(truth.size(), other.size());
    BOOST_REQUIRE_EQUAL(truth.empty(), other.empty());

    for (int i = 0; i < 2000; i++) {
        truth.push_back(random_generators::get_int<int64_t>(1000, 3000));
        other.push_back(truth.back());

        other = serde::from_iobuf<decltype(other)>(
          serde::to_iobuf(std::move(other)));
        test_equal(truth, other);
    }
    BOOST_REQUIRE_EQUAL(truth.size(), 2000);
    test_equal(truth, other);

    BOOST_REQUIRE_EQUAL(
      truth, std::vector<int64_t>(other.begin(), other.end()));

    for (int i = 0; i < 6000; i++) {
        auto val = random_generators::get_int<int64_t>(0, 4000);

        auto it = std::lower_bound(truth.begin(), truth.end(), val);
        auto it2 = std::lower_bound(other.begin(), other.end(), val);
        BOOST_REQUIRE_EQUAL(it == truth.end(), it2 == other.end());
        BOOST_REQUIRE_EQUAL(
          std::distance(truth.begin(), it), std::distance(other.begin(), it2));
        BOOST_REQUIRE_EQUAL(
          std::distance(it, truth.end()), std::distance(it2, other.end()));

        it = std::upper_bound(truth.begin(), truth.end(), val);
        it2 = std::upper_bound(other.begin(), other.end(), val);
        BOOST_REQUIRE_EQUAL(it == truth.end(), it2 == other.end());
        BOOST_REQUIRE_EQUAL(
          std::distance(truth.begin(), it), std::distance(other.begin(), it2));
        BOOST_REQUIRE_EQUAL(
          std::distance(it, truth.end()), std::distance(it2, other.end()));

        it = std::find(truth.begin(), truth.end(), val);
        it2 = std::find(other.begin(), other.end(), val);
        BOOST_REQUIRE_EQUAL(it == truth.end(), it2 == other.end());
        BOOST_REQUIRE_EQUAL(
          std::distance(truth.begin(), it), std::distance(other.begin(), it2));
        BOOST_REQUIRE_EQUAL(
          std::distance(it, truth.end()), std::distance(it2, other.end()));
    }
}
