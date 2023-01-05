// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#define BOOST_TEST_MODULE random
#include "random/fast_prng.h"
#include "random/generators.h"

#include <absl/container/flat_hash_set.h>
#include <boost/test/unit_test.hpp>

#include <set>
#include <utility>

BOOST_AUTO_TEST_CASE(fast_prng_basic_gen_100_unique_rands) {
    fast_prng rng;
    std::set<uint32_t> test;
    for (auto i = 0; i < 100; ++i) {
        uint32_t x = rng();
        BOOST_CHECK(!test.count(x));
        test.insert(x);
    }
}
BOOST_AUTO_TEST_CASE(alphanum_generator) {
    for (auto i = 0; i < 100; ++i) {
        auto s = random_generators::gen_alphanum_string(i);
        // ensure no \0 in size
        for (auto j = 0; j < i; ++j) {
            BOOST_REQUIRE('\0' != s[j]);
        }
    }
}

BOOST_AUTO_TEST_CASE(alphanum_max_distinct_generator) {
    constexpr size_t cardinality = 11;
    absl::flat_hash_set<ss::sstring> strings;
    for (auto i = 0; i < 100; i++) {
        auto s = random_generators::gen_alphanum_max_distinct(cardinality);
        // ensure no \0 in size
        for (auto j = 0; j < random_generators::alphanum_max_distinct_strlen;
             j++) {
            BOOST_REQUIRE('\0' != s[j]);
        }
        strings.emplace(s);
    }
    BOOST_REQUIRE(strings.size() <= cardinality);
}
