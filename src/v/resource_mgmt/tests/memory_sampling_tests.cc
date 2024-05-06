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

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>
#include <fmt/core.h>

#include <chrono>
#include <sstream>
#include <string>

#ifndef SEASTAR_DEFAULT_ALLOCATOR

SEASTAR_THREAD_TEST_CASE(test_no_allocs_in_oom_callback) {
    seastar::memory::scoped_heap_profiling profiling(16000);

    std::vector<std::vector<char>> dummy_bufs;
    std::vector<char> output_buffer;
    const size_t max_output_size = 1000000;
    output_buffer.reserve(max_output_size);

    std::vector<seastar::memory::allocation_site> allocation_sites(1000);
    size_t sampled_sites = 0;

    // make sure we have at least one allocation site
    while (sampled_sites == 0) {
        dummy_bufs.emplace_back(1000);
        sampled_sites = seastar::memory::sampled_memory_profile(
          allocation_sites.data(), allocation_sites.size());
    }

    auto oom_callback = memory_sampling::get_oom_diagnostics_callback();
    auto writer = [&output_buffer](std::string_view bytes) {
        if (output_buffer.size() + bytes.size() < max_output_size) {
            output_buffer.insert(
              output_buffer.end(), bytes.begin(), bytes.end());
        }
    };

    auto before = seastar::memory::stats();
    oom_callback(writer);
    auto after = seastar::memory::stats();

    // to make sure we are actually testing something
    BOOST_REQUIRE_GT(before.mallocs(), 0);
    BOOST_REQUIRE_GT(output_buffer.size(), 0);
    BOOST_REQUIRE_EQUAL(before.large_allocations(), after.large_allocations());
    BOOST_REQUIRE_EQUAL(before.allocated_memory(), after.allocated_memory());
    BOOST_REQUIRE_EQUAL(before.mallocs(), after.mallocs());

    // confirm an average allocation site fits into the oom writer line buffer
    auto allocation_site_needle = fmt::format("{}\n", allocation_sites[0]);
    BOOST_REQUIRE_NE(
      std::string_view(output_buffer.data(), output_buffer.size())
        .find(allocation_site_needle),
      std::string_view::npos);
}

SEASTAR_THREAD_TEST_CASE(test_low_watermark_logging) {
    seastar::logger dummy_logger("dummy");
    std::stringstream output_buf;
    dummy_logger.set_ostream(output_buf);

    const auto first_log_limit = 0.90;
    const auto second_log_limit = 0.80;

    memory_sampling sampling(
      dummy_logger,
      config::mock_binding<bool>(true),
      std::chrono::seconds(1),
      first_log_limit,
      second_log_limit);
    sampling.start();

    std::string_view needle("Top-N alloc");

    {
        // notification shouldn't do anything
        auto buf = output_buf.str();
        auto view = std::string_view{buf};
        BOOST_REQUIRE_EQUAL(view.find(needle), std::string_view::npos);
    }

    std::vector<std::vector<char>> dummy_bufs;
    size_t total_memory = seastar::memory::stats().total_memory();

    auto allocate_till_limit = [&](size_t limit) {
        while (seastar::memory::stats().free_memory() > limit) {
            dummy_bufs.emplace_back(1000000);
        }
    };

    allocate_till_limit(first_log_limit * total_memory);

    tests::cooperative_spin_wait_with_timeout(std::chrono::seconds(10), [&]() {
        auto buf = output_buf.str();
        auto view = std::string_view{buf};
        return view.find(needle) != std::string_view::npos;
    }).get(); // will throw if false at timeout

    allocate_till_limit(second_log_limit * total_memory);

    tests::cooperative_spin_wait_with_timeout(std::chrono::seconds(10), [&]() {
        auto buf = output_buf.str();
        auto view = std::string_view{buf};
        return view.find(needle) != view.rfind(needle);
    }).get(); // will throw if false at timeout

    auto old_size = output_buf.str().size();

    {
        // no more notifications should happen
        auto buf = output_buf.str();
        BOOST_REQUIRE_EQUAL(buf.size(), old_size);
    }

    sampling.stop().get();
}

#endif // SEASTAR_DEFAULT_ALLOCATOR
