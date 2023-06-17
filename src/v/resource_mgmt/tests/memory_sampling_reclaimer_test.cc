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

#include "resource_mgmt/memory_sampling.h"
#include "storage/batch_cache.h"
#include "test_utils/async.h"

#include <seastar/core/memory.hh>
#include <seastar/core/reactor.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/log.hh>

#include <boost/test/unit_test.hpp>

#include <chrono>
#include <sstream>
#include <string>

#ifndef SEASTAR_DEFAULT_ALLOCATOR

/// Test batch cache integration independently
SEASTAR_THREAD_TEST_CASE(reclaim_notifies_memory_sampling) {
    std::stringstream logger_buf;
    seastar::logger test_logger("test");
    test_logger.set_ostream(logger_buf);
    ss::sharded<memory_sampling> memory_sampling_service;

    std::string_view needle("Top-N alloc");
    const auto first_log_limit = 0.80;
    memory_sampling_service
      .start(
        std::ref(test_logger),
        config::mock_binding<bool>(true),
        first_log_limit,
        0.2)
      .get();
    memory_sampling_service.invoke_on_all(&memory_sampling::start).get();

    {
        auto buf = logger_buf.str();
        auto view = std::string_view{buf};
        BOOST_REQUIRE_EQUAL(view.find(needle), std::string_view::npos);
    }

    storage::batch_cache::reclaim_options opts = {
      .growth_window = std::chrono::milliseconds(3000),
      .stable_window = std::chrono::milliseconds(10000),
      .min_size = 128 << 10,
      .max_size = 4 << 20,
      .min_free_memory = 1};
    storage::batch_cache cache(opts, memory_sampling_service);
    storage::batch_cache_index index(cache);

    std::vector<std::vector<char>> dummy_bufs;
    size_t total_memory = seastar::memory::stats().total_memory();
    auto allocate_till_limit = [&](size_t limit) {
        while (seastar::memory::stats().free_memory() > limit) {
            dummy_bufs.emplace_back(1000000);
        }
    };

    allocate_till_limit(first_log_limit * total_memory);

    // simulate a callback from the allocator
    cache.reclaim(1);

    tests::cooperative_spin_wait_with_timeout(std::chrono::seconds(10), [&]() {
        auto buf = logger_buf.str();
        auto view = std::string_view{buf};
        return view.find(needle) != std::string_view::npos;
    }).get(); // will throw if false at timeout

    cache.stop().get();
    memory_sampling_service.stop().get();
}

#endif // SEASTAR_DEFAULT_ALLOCATOR
