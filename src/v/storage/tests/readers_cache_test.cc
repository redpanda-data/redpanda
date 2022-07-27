// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "seastarx.h"
#include "storage/readers_cache.h"
#include "test_utils/fixture.h"

#include <fmt/ostream.h>

namespace storage {
struct readers_cache_test_fixture {
    readers_cache_test_fixture()
      : cache(
        model::ntp("test", "test", 0), std::chrono::milliseconds(360000)) {}

    bool intersects_locked_range(model::offset begin, model::offset end) {
        return cache.intersects_with_locked_range(begin, end);
    }

    void test_intersects_locked(
      model::offset::type begin, model::offset::type end, bool in_range) {
        BOOST_REQUIRE_EQUAL(
          intersects_locked_range(model::offset(begin), model::offset(end)),
          in_range);
    }

    readers_cache cache;
};

} // namespace storage
using namespace storage;

FIXTURE_TEST(test_range_is_correctly_locked, readers_cache_test_fixture) {
    {
        /**
         * Evict truncate locks range starting from give offset up to max
         */
        auto holder = cache.evict_truncate(model::offset(10)).get();

        // [0,1] is not in [10,inf]
        test_intersects_locked(0, 1, false);

        // [0,10] is in range [10,inf]
        test_intersects_locked(0, 10, true);

        // [9,12] is in range [10, inf]
        test_intersects_locked(9, 12, true);

        // [10,12] is in range [10, inf]
        test_intersects_locked(10, 12, true);

        // [20,25] is in range [10, inf]
        test_intersects_locked(20, 25, true);
    }

    {
        /**
         * Evict prefix truncate locks range starting from 0 up to given offset
         */
        auto holder = cache.evict_prefix_truncate(model::offset(10)).get();

        // [0,1] is in [0,10]
        test_intersects_locked(0, 1, true);

        // [0,10] is in range [0,10]
        test_intersects_locked(0, 10, true);

        // [9,12] is in range [0, 10]
        test_intersects_locked(9, 12, true);

        // [10,12] is in range [0, 10]
        test_intersects_locked(10, 12, true);

        // [20,25] is not in range [0, 10]
        test_intersects_locked(20, 25, false);
    }

    {
        /**
         * Evict range locks given range
         */
        auto holder
          = cache.evict_range(model::offset(5), model::offset(10)).get();

        // [0,1] is not in [5,10]
        test_intersects_locked(0, 1, false);

        // [0,20] is not in range [5,10] but [5,10] is in [0,20]
        test_intersects_locked(0, 20, true);

        // [9,12] is in range [5,10]
        test_intersects_locked(9, 12, true);

        // [10,12] is in range [5,10]
        test_intersects_locked(10, 12, true);

        // [20,25] is not in range [5,10]
        test_intersects_locked(20, 25, false);

        // [4,5] is in range [5,10]
        test_intersects_locked(4, 5, true);

        // [6,7] is in range [5,10]
        test_intersects_locked(6, 7, true);
    }
}
