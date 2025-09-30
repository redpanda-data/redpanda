// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#if IS_SEASTAR_TEST == 0 && defined(IS_BTEST)
#define BOOST_TEST_MODULE random_seeding
#endif

#include "random/generators.h"
#include "test_utils/test_macros.h"

// This test gets compiled into three variants:
// non-seastar boost
// seastar boost
// gtest
// Hence the macro boilerplate below.

#ifdef IS_GTEST

#include <gtest/gtest.h>
#define MAKE_TEST_CASE(name) TEST(random_seeding, name)

#elif IS_BTEST

#if IS_SEASTAR_TEST == 0
#include <boost/test/unit_test.hpp>
#define MAKE_TEST_CASE BOOST_AUTO_TEST_CASE
#elif IS_SEASTAR_TEST == 1
#include <seastar/testing/thread_test_case.hh>
#define MAKE_TEST_CASE SEASTAR_THREAD_TEST_CASE
#else
#error "IS_SEASTAR_TEST must be 0 or 1"
#endif

#else

// the cc_test macros define these
#error one of IS_GTEST or IS_BTEST must be defined

#endif

using random_generators::get_int;
using random_generators::rng;

// Any object of rng encapsulates RNG state. I.e., two rng
// objects with the same state will generate the same sequence of random
// numbers.
//
// The global() object is a thead-local shared instance, used to migrate
// existing callers of the free-function interface, i.e.,
// `random_generators::get_int()` becomes
// `random_generators::global().get_int()`, but new use should consider creating
// and maintaining their own rng object.

// the first and second values expected from `get_int` with the default seed
constexpr int get_int_0 = 79693958;
constexpr int get_int_1 = 1828472881;

using namespace random_generators::internal;

// return true if the global seeding mode is other than "fixed"
bool not_fixed() {
    return random_generators::internal::default_seeding_policy()
           == seeding_mode::random_seed;
}

void check_two_random_numbers() {
    if (not_fixed()) {
        // only applies in fixed mode
        return;
    }
    RPTEST_EXPECT_EQ(get_int<int>(), get_int_0);
    RPTEST_EXPECT_EQ(get_int<int>(), get_int_1);
}

MAKE_TEST_CASE(test_expected_values) { check_two_random_numbers(); }

MAKE_TEST_CASE(test_expected_values2) {
    // same as the above test, but since we run both tests in the same process
    // (usually) it tests that the global rng state is being reset between
    // tests.
    check_two_random_numbers();
}

MAKE_TEST_CASE(test_local_reseeding) {
    if (not_fixed()) {
        return;
    }
    rng local_rng;
    RPTEST_EXPECT_EQ(local_rng.get_int<int>(), get_int_0);
    RPTEST_EXPECT_EQ(local_rng.get_int<int>(), get_int_1);

    local_rng = rng{};
    RPTEST_EXPECT_EQ(local_rng.get_int<int>(), get_int_0);
    RPTEST_EXPECT_EQ(local_rng.get_int<int>(), get_int_1);
}

MAKE_TEST_CASE(test_random_seeding) {
    if (!not_fixed()) {
        return;
    }

    // the opposite of the previous test above, the global generator
    // should be randomly seeded: 1 in 2^32 chance of flaking!
    rng local_rng;
    RPTEST_REQUIRE_NE(local_rng.get_int<int>(), get_int_0);
    RPTEST_REQUIRE_NE(local_rng.get_int<int>(), get_int_1);

    local_rng = rng{};
    RPTEST_REQUIRE_NE(local_rng.get_int<int>(), get_int_0);
    RPTEST_REQUIRE_NE(local_rng.get_int<int>(), get_int_1);
}
