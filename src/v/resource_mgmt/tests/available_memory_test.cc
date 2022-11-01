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

#include <memory>
#include <stdexcept>

namespace {
auto& local() { return resources::available_memory::local(); }
} // namespace

// clang is pretty adept at optimizing away heap allocations, when the
// pointer doens't escape out into the wild, which applies to allocations
// we want to do here, so we "sink" the value by writing it to a global
// volatile which prevents optimization.
void* volatile sink;

SEASTAR_THREAD_TEST_CASE(check_empty_reporters) {
    // when no reclaimers are registered, reclaimable should be 0
    BOOST_CHECK_EQUAL(local().reclaimable(), 0);

    // and available should be equal to seastar free
    auto free = seastar::memory::stats().free_memory();
    BOOST_CHECK_EQUAL(local().available(), free);
    // this second identical check effectively asserts that BOOST_CHECK_EQUAL
    // does not allocate (or at least does not reduce free memory), which is
    // required for the remaining tests
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

// this next test only makes sense if we are using the seastar
// allocator since otherwise the stats return bogus, fixed values
#ifndef SEASTAR_DEFAULT_ALLOCATOR

SEASTAR_THREAD_TEST_CASE(check_low_water_mark) {
    resources::available_memory am;

    auto free = seastar::memory::stats().free_memory();
    BOOST_CHECK_EQUAL(am.available(), free);
    BOOST_CHECK_EQUAL(am.reclaimable(), 0);

    // initially the LWM is equal to available since the only update is
    // within this available_low_water_mark call
    BOOST_CHECK_EQUAL(am.available_low_water_mark(), free);

    // 500 KB: larger than the small pool threshold, otherwise available may not
    // decrease
    constexpr size_t large_size = 500 * 1000;
    auto bytes = std::make_unique<char[]>(large_size);
    sink = bytes.get();

    auto free_after = am.available();
    auto lwm_after = am.available_low_water_mark();
    BOOST_CHECK(free - free_after >= large_size);
    BOOST_CHECK_EQUAL(free_after, lwm_after);

    // free the bytes, we expect free to rebound but lwm unchanged
    bytes.reset();
    BOOST_CHECK_EQUAL(free, am.available());
    BOOST_CHECK_EQUAL(lwm_after, am.available_low_water_mark());

    // check that a large allocation without any corresponding
    // update_low_water_mark() call is "invisble" to the LVM
    bytes = std::make_unique<char[]>(large_size * 2);
    BOOST_CHECK(am.available() < free_after);
    bytes.reset();
    BOOST_CHECK_EQUAL(lwm_after, am.available_low_water_mark());

    // same as the last case, but this time we call update, so expect
    // the LWM to drop
    bytes = std::make_unique<char[]>(large_size * 2);
    BOOST_CHECK(am.available() < free_after);
    am.update_low_water_mark();
    bytes.reset();
    BOOST_CHECK(am.available_low_water_mark() < lwm_after);
}

#endif
