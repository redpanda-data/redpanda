/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/prefetch_tracker.h"
#include "cloud_storage/types.h"
#include "config/base_property.h"
#include "config/configuration.h"
#include "config/property.h"
#include "redpanda/tests/fixture.h"
#include "seastarx.h"

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

using namespace cloud_storage;

SEASTAR_THREAD_TEST_CASE(test_prefetch_tracker) { // NOLINT
    prefetch_tracker tracker(true, 100, 10);

    tracker.on_new_segment();
    tracker.set_segment_size(200);
    BOOST_REQUIRE(!tracker());
    tracker.on_bytes_consumed(10); // consumed 10 bytes, 190 bytes remains
    BOOST_REQUIRE(!tracker());
    tracker.on_bytes_consumed(100); // consumed 110 bytes, 90 bytes remains
    BOOST_REQUIRE(!tracker());
    tracker.on_bytes_consumed(80); // consumed 190 bytes, 10 bytes remains
    BOOST_REQUIRE(tracker());
    BOOST_REQUIRE(!tracker());
    tracker.on_bytes_consumed(10); // consumed 200 bytes, 0 bytes remains
    BOOST_REQUIRE(!tracker());
    tracker.on_new_segment();
    BOOST_REQUIRE(!tracker());
    tracker.on_bytes_consumed(
      10); // consumed 10 bytes, segment size not known yet
    BOOST_REQUIRE(!tracker());
    tracker.set_segment_size(200);
    BOOST_REQUIRE(!tracker());
    tracker.on_bytes_consumed(100); // consumed 100 bytes, 90 bytes remains
    BOOST_REQUIRE(!tracker());
    tracker.on_bytes_consumed(80); // consumed 100 bytes, 0 bytes remains
    BOOST_REQUIRE(tracker());
    BOOST_REQUIRE(!tracker());
}

SEASTAR_THREAD_TEST_CASE(test_prefetch_tracker_small_segments) { // NOLINT
    // Validate that prefetching is not utilized for small segments
    prefetch_tracker tracker(true, 0, 100);

    tracker.on_new_segment();
    tracker.set_segment_size(50);
    BOOST_REQUIRE(!tracker());
    tracker.on_bytes_consumed(10);
    BOOST_REQUIRE(!tracker());
    tracker.on_bytes_consumed(40);
    BOOST_REQUIRE(!tracker());
}
