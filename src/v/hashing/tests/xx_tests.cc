// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/named_type.h"

#include <boost/test/tools/old/interface.hpp>

#include <cstddef>
#define BOOST_TEST_MODULE xxhash
#include "hashing/xx.h"

#include <boost/test/unit_test.hpp>

#include <array>

namespace {

template<typename Hash>
void check_incremental_matches(uint64_t expected) {
    Hash inc;
    inc.update(1);
    inc.update(2);
    inc.update(42);
    BOOST_CHECK_EQUAL(inc.digest(), expected);
}

template<typename Hash>
void check_digest_idempotency(uint64_t expected) {
    Hash inc;
    inc.update(1);
    inc.digest();
    inc.update(2);
    inc.digest();
    inc.update(42);
    inc.digest();

    BOOST_CHECK_EQUAL(inc.digest(), expected);
    for (auto i = 0; i < 10; ++i) {
        BOOST_CHECK_EQUAL(inc.digest(), expected);
    }
}

template<typename Hash, typename T, typename V>
void test_incremental_hash(T test, V expected) {
    Hash hash;
    hash.update(test);

    Hash expected_hash;
    expected_hash.update(expected);
    BOOST_REQUIRE_EQUAL(hash.digest(), expected_hash.digest());
}

template<typename Hash>
void check_overload_resolution() {
    using named_str = named_type<std::string, struct str_type>;
    using named_integral = named_type<size_t, struct int_type>;

    test_incremental_hash<Hash>(named_str{"named_str"}, "named_str");
    test_incremental_hash<Hash>(named_integral{10}, (size_t)10);
    named_str s("test_str");
    test_incremental_hash<Hash>(s, s());
}

constexpr std::array<int, 3> updates = {1, 2, 42};

} // namespace

BOOST_AUTO_TEST_CASE(incremental_same_as_array) {
    check_incremental_matches<incremental_xxhash64>(xxhash_64(updates));
    check_incremental_matches<incremental_xxh3_64>(xxh3_64(updates));
}

BOOST_AUTO_TEST_CASE(digest_idempotency) {
    check_digest_idempotency<incremental_xxhash64>(xxhash_64(updates));
    check_digest_idempotency<incremental_xxh3_64>(xxh3_64(updates));
}

BOOST_AUTO_TEST_CASE(overload_resolution) {
    check_overload_resolution<incremental_xxhash64>();
    check_overload_resolution<incremental_xxh3_64>();
}

/// The two hashes are distinct functions, so the same input gives two values.
BOOST_AUTO_TEST_CASE(hashes_differ) {
    incremental_xxhash64 xxhash64;
    incremental_xxh3_64 xxh3;
    xxhash64.update("the same input");
    xxh3.update("the same input");
    BOOST_CHECK_NE(xxhash64.digest(), xxh3.digest());
}
