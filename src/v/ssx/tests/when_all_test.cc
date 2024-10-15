// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "container/fragmented_vector.h"
#include "ssx/when_all.h"

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/memory.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>

#include <boost/container/static_vector.hpp>
#include <fmt/format.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>

using namespace std::chrono_literals;

namespace {

// A small number of items that are easy to parse visually if something goes
// wrong.
constexpr size_t few_items = 5;

template<typename T>
using static_vector = boost::container::static_vector<T, few_items>;

template<typename Container>
Container make_ready_futures(size_t n_items) {
    Container futures;
    futures.reserve(n_items);
    for (size_t i = 0; i < n_items; ++i) {
        futures.push_back(ss::make_ready_future<int>(static_cast<int>(i)));
    }
    return futures;
}

template<typename Container>
Container make_expected(size_t n_items) {
    using value_type = typename Container::value_type;
    Container expected;
    expected.reserve(n_items);
    for (size_t i = 0; i < n_items; ++i) {
        expected.push_back(static_cast<value_type>(i));
    }
    return expected;
}

} // namespace

TEST(WhenAllAlgorithm, when_all_basic_testing) {
    // all futures ready
    {
        const auto expected_vector = ::make_expected<std::vector<int>>(
          few_items);
        auto futures = ::make_ready_futures<std::vector<ss::future<int>>>(
          few_items);
        auto res = ssx::when_all_succeed<std::vector<int>>(std::move(futures));
        EXPECT_EQ(res.get(), expected_vector);
    }

    // all futures ready - boost::static_vector
    {
        const auto expected_vector = ::make_expected<static_vector<int>>(
          few_items);
        auto futures = ::make_ready_futures<static_vector<ss::future<int>>>(
          few_items);
        auto res = ssx::when_all_succeed<static_vector<int>>(
          std::move(futures));
        EXPECT_EQ(res.get(), expected_vector);
    }

    // all futures ready - chunked_vector
    {
        const auto expected_vector = ::make_expected<chunked_vector<int>>(
          few_items);
        auto futures = ::make_ready_futures<chunked_vector<ss::future<int>>>(
          few_items);
        auto res = ssx::when_all_succeed<chunked_vector<int>>(
          std::move(futures));
        EXPECT_EQ(res.get(), expected_vector);
    }

    // all futures ready - input/output different containers
    {
        const auto expected_vector = ::make_expected<chunked_vector<int>>(
          few_items);
        auto futures = ::make_ready_futures<std::vector<ss::future<int>>>(
          few_items);
        auto res = ssx::when_all_succeed<chunked_vector<int>>(
          std::move(futures));
        EXPECT_EQ(res.get(), expected_vector);
    }

    // all futures ready - input/output different value type
    {
        const auto expected_vector = ::make_expected<chunked_vector<double>>(
          few_items);
        auto futures = ::make_ready_futures<std::vector<ss::future<int>>>(
          few_items);
        auto res = ssx::when_all_succeed<chunked_vector<double>>(
          std::move(futures));
        EXPECT_EQ(res.get(), expected_vector);
    }

    // all futures ready - move_only value_type
    {
        using StringPtr = std::unique_ptr<std::string>;
        std::vector<ss::future<StringPtr>> futures;
        futures.reserve(2);
        futures.push_back(seastar::make_ready_future<StringPtr>(
          std::make_unique<std::string>("test string 0")));
        futures.push_back(seastar::make_ready_future<StringPtr>(
          std::make_unique<std::string>("test string 1")));
        auto res = ssx::when_all_succeed<chunked_vector<StringPtr>>(
          std::move(futures));
        auto resolved = res.get();
        EXPECT_EQ(resolved.size(), 2);
        EXPECT_EQ(*resolved[0], "test string 0");
        EXPECT_EQ(*resolved[1], "test string 1");
    }

    // exceptional future
    {
        std::vector<ss::future<int>> futures;
        futures.reserve(few_items);
        futures.push_back(seastar::make_ready_future<int>(0));
        futures.push_back(seastar::make_ready_future<int>(1));
        futures.emplace_back(seastar::make_exception_future<int>(
          std::runtime_error{"Test Exception"}));
        futures.push_back(seastar::make_ready_future<int>(3));
        futures.push_back(seastar::make_ready_future<int>(4));
        auto res = ssx::when_all_succeed<std::vector<int>>(std::move(futures));
        EXPECT_THROW(res.get(), std::runtime_error);
    }
}

TEST(WhenAllAlgorithm, when_all_ongoing_futures) {
    constexpr auto time_increment = 10ms;

    const std::vector<int> expected_std_vector{0, 1, 2, 3, 4};

    // wait for futures
    {
        std::vector<ss::future<int>> futures;
        futures.reserve(few_items);
        for (size_t i = 0; i < few_items; ++i) {
            auto f
              = ss::sleep<seastar::lowres_clock>(i * time_increment).then([i] {
                    return seastar::make_ready_future<int>(static_cast<int>(i));
                });
            futures.push_back(std::move(f));
        }
        auto res = ssx::when_all_succeed<std::vector<int>>(std::move(futures));
        EXPECT_EQ(res.get(), expected_std_vector);
    }
}

// This test is only meaningfull in release mode
#ifdef NDEBUG

TEST(WhenAllAlgorithm, when_all_verify_no_large_allocation) {
    // Enougn items to fill 2 fragments in a chunked_vector<int>.
    // Also, twice the size of the 128kb expected allocation warning threshold.
    constexpr size_t many_items = 2UL * 1024UL * 128UL / sizeof(int);

    // Same as the default memory allocation warning threshold
    const size_t large_allocation_threshold = 1024UL * 128UL + 1UL;
    ss::smp::invoke_on_all([threshold = large_allocation_threshold] {
        ss::memory::set_large_allocation_warning_threshold(threshold);
    }).get();

    // validate that a large vector won't generate a warning
    {
        const auto expected_vector = ::make_expected<chunked_vector<int>>(
          many_items);
        auto futures = ::make_ready_futures<chunked_vector<ss::future<int>>>(
          many_items);
        auto res = ssx::when_all_succeed<chunked_vector<int>>(
          std::move(futures));
        EXPECT_EQ(res.get(), expected_vector);

        const ss::memory::statistics mem = ss::memory::stats();
        EXPECT_EQ(mem.large_allocations(), 0);
    }
}

#endif
