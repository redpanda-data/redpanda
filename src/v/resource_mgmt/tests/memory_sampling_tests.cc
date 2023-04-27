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

#include <chrono>
#include <sstream>
#include <string>

#ifndef SEASTAR_DEFAULT_ALLOCATOR

SEASTAR_THREAD_TEST_CASE(test_low_watermark_logging) {
    seastar::logger dummy_logger("dummy");
    std::stringstream output_buf;
    dummy_logger.set_ostream(output_buf);

    const auto first_log_limit = 0.90;
    const auto second_log_limit = 0.80;

    memory_sampling sampling(dummy_logger, first_log_limit, second_log_limit);
    sampling.start();

    std::string_view needle("Top-N alloc");

    {
        // notification shouldn't do anything
        sampling.notify_of_reclaim();
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
    sampling.notify_of_reclaim();

    tests::cooperative_spin_wait_with_timeout(std::chrono::seconds(10), [&]() {
        auto buf = output_buf.str();
        auto view = std::string_view{buf};
        return view.find(needle) != std::string_view::npos;
    }).get(); // will throw if false at timeout

    allocate_till_limit(second_log_limit * total_memory);
    sampling.notify_of_reclaim();

    tests::cooperative_spin_wait_with_timeout(std::chrono::seconds(10), [&]() {
        auto buf = output_buf.str();
        auto view = std::string_view{buf};
        return view.find(needle) != view.rfind(needle);
    }).get(); // will throw if false at timeout

    auto old_size = output_buf.str().size();

    sampling.notify_of_reclaim();

    {
        // no more notifications should happen
        auto buf = output_buf.str();
        BOOST_REQUIRE_EQUAL(buf.size(), old_size);
    }
}

#endif // SEASTAR_DEFAULT_ALLOCATOR
