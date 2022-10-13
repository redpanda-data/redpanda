// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "resource_mgmt/available_memory.h"

#include <seastar/core/memory.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

namespace {
auto& local() { return resources::available_memory::local(); }
} // namespace

SEASTAR_THREAD_TEST_CASE(check_empty_reporters) {
    // when no reclaimers are registered, reclaimable should be 0
    BOOST_CHECK_EQUAL(local().reclaimable(), 0);

    // and available should be equal to seastar free
    auto free = seastar::memory::stats().free_memory();
    BOOST_CHECK_EQUAL(local().available(), free);
}

SEASTAR_THREAD_TEST_CASE(check_with_reporters) {
    size_t reclaimable = 0;
    {
        auto holder = local().register_reporter(
          "test_reporter", [&] { return reclaimable; });

        BOOST_CHECK_EQUAL(local().reclaimable(), 0);

        reclaimable = 5;
        BOOST_CHECK_EQUAL(local().reclaimable(), 5);

        reclaimable = 7;
        BOOST_CHECK_EQUAL(local().reclaimable(), 7);

        {
            auto holder2 = local().register_reporter(
              "test_reporter2", [&] { return 2; });

            BOOST_CHECK_EQUAL(local().reclaimable(), 7 + 2);
        }

        BOOST_CHECK_EQUAL(local().reclaimable(), 7);
    }

    BOOST_CHECK_EQUAL(local().reclaimable(), 0);
}
