// Copyright 2022 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/iobuf.h"
#include "random/generators.h"
#include "utils/delta_for.h"
#include "vlog.h"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <stdexcept>

template<class TVal>
std::vector<TVal> populate_encoder(
  deltafor_encoder<TVal>& c,
  uint64_t initial_value,
  const std::vector<TVal>& deltas) {
    std::vector<TVal> result;
    auto p = initial_value;
    for (TVal delta : deltas) {
        std::array<TVal, details::FOR_buffer_depth> buf = {};
        for (int x = 0; x < details::FOR_buffer_depth; x++) {
            result.push_back(p);
            buf.at(x) = p;
            p += random_generators::get_int(delta);
            if (p < buf.at(x)) {
                throw std::out_of_range("delta can't be represented");
            }
        }
        c.add(buf);
    }
    return result;
}

BOOST_AUTO_TEST_CASE(roundtrip_test_2) {
    static constexpr int64_t initial_value = 0;
    deltafor_encoder<int64_t> enc(initial_value);
    std::vector<int64_t> deltas = {
      0LL,
      10LL,
      100LL,
      1000LL,
      10000LL,
      100000LL,
      1000000LL,
      10000000LL,
      100000000LL,
      1000000000LL,
      10000000000LL,
      100000000000LL,
      1000000000000LL,
      10000000000000LL,
      100000000000000LL,
      1000000000000000LL,
      10000000000000000LL,
      100000000000000000LL,
      1000000000000000000LL,
    };
    auto expected = populate_encoder(enc, initial_value, deltas);

    deltafor_decoder<int64_t> dec(
      initial_value, enc.get_row_count(), enc.copy());

    std::vector<int64_t> actual;
    std::array<int64_t, details::FOR_buffer_depth> buf{};
    int cnt = 0;
    while (dec.read(buf)) {
        cnt++;
        std::copy(buf.begin(), buf.end(), std::back_inserter(actual));
        buf = {};
    }
    BOOST_REQUIRE_EQUAL(cnt, deltas.size());
    BOOST_REQUIRE(expected == actual);
}

BOOST_AUTO_TEST_CASE(roundtrip_test_1) {
    static constexpr uint64_t initial_value = 0;
    deltafor_encoder<uint64_t> enc(initial_value);
    std::vector<uint64_t> deltas = {
      0ULL,
      10ULL,
      100ULL,
      1000ULL,
      10000ULL,
      100000ULL,
      1000000ULL,
      10000000ULL,
      100000000ULL,
      1000000000ULL,
      10000000000ULL,
      100000000000ULL,
      1000000000000ULL,
      10000000000000ULL,
      100000000000000ULL,
      1000000000000000ULL,
      10000000000000000ULL,
      100000000000000000ULL,
      1000000000000000000ULL,
    };
    auto expected = populate_encoder(enc, initial_value, deltas);

    deltafor_decoder<uint64_t> dec(
      initial_value, enc.get_row_count(), enc.copy());

    std::vector<uint64_t> actual;
    std::array<uint64_t, details::FOR_buffer_depth> buf{};
    int cnt = 0;
    while (dec.read(buf)) {
        cnt++;
        std::copy(buf.begin(), buf.end(), std::back_inserter(actual));
        buf = {};
    }
    BOOST_REQUIRE_EQUAL(cnt, deltas.size());
    BOOST_REQUIRE(expected == actual);
}

template<class TVal>
void test_random_walk_roundtrip(int test_size, int max_delta) {
    static constexpr TVal initial_value = 0;
    deltafor_encoder<TVal> enc(initial_value);
    std::vector<TVal> deltas;
    deltas.reserve(test_size);
    for (int i = 0; i < test_size; i++) {
        deltas.push_back(random_generators::get_int(max_delta));
    }
    auto expected = populate_encoder(enc, initial_value, deltas);

    deltafor_decoder<TVal> dec(initial_value, enc.get_row_count(), enc.copy());

    std::vector<TVal> actual;
    std::array<TVal, details::FOR_buffer_depth> buf{};
    int cnt = 0;
    while (dec.read(buf)) {
        cnt++;
        std::copy(buf.begin(), buf.end(), std::back_inserter(actual));
        buf = {};
    }
    BOOST_REQUIRE_EQUAL(cnt, deltas.size());
    BOOST_REQUIRE(expected == actual);
}

BOOST_AUTO_TEST_CASE(random_walk_test_1) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 100;
    test_random_walk_roundtrip<uint64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_2) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 1000;
    test_random_walk_roundtrip<uint64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_3) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 10000;
    test_random_walk_roundtrip<uint64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_4) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 100000;
    test_random_walk_roundtrip<uint64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_5) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 100;
    test_random_walk_roundtrip<int64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_6) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 1000;
    test_random_walk_roundtrip<int64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_7) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 10000;
    test_random_walk_roundtrip<int64_t>(test_size, max_delta);
}

BOOST_AUTO_TEST_CASE(random_walk_test_8) {
    static constexpr int test_size = 100000;
    static constexpr int max_delta = 100000;
    test_random_walk_roundtrip<int64_t>(test_size, max_delta);
}