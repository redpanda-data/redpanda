/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "config/configuration.h"
#include "config/property.h"
#include "resource_mgmt/cpu_profiler.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>

#include <chrono>
#include <exception>

SEASTAR_THREAD_TEST_CASE(test_cpu_profiler) {
    using namespace std::literals;

    resources::cpu_profiler cp(
      config::mock_binding(true), config::mock_binding(2ms));
    cp.start().get();

    // The profiler service will request samples from seastar every
    // 256ms since the sample rate is 2ms. So we need to be running
    // for at least that long to ensure the service pulls in samples.
    auto end_time = ss::lowres_clock::now() + 256ms + 10ms;
    while (ss::lowres_clock::now() < end_time) {
        // yield to allow timer to trigger and lowres_clock to update
        ss::thread::maybe_yield();
    }

    auto results = cp.shard_results();
    BOOST_TEST(results.samples.size() >= 1);
}
