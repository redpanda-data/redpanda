// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "seastarx.h"
#include "utils/fragmented_vector.h"

#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(fragmented_vector_fill_async_test) {
    fragmented_vector<int> v;
    fragmented_vector_fill_async(v, 0).get();
    BOOST_REQUIRE(v.size() == 0);

    // fill with non-zero
    for (int i = 1; i <= 10; ++i) {
        v.push_back(i);
    }
    BOOST_REQUIRE(v.size() == 10);
    for (const auto& e : v) {
        BOOST_REQUIRE(e > 0);
    }

    // fill with zero
    fragmented_vector_fill_async(v, 0).get();
    BOOST_REQUIRE(v.size() == 10);
    for (const auto& e : v) {
        BOOST_REQUIRE(e == 0);
    }
}
