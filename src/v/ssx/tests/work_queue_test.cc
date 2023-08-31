// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "gmock/gmock.h"
#include "seastarx.h"
#include "ssx/work_queue.h"

#include <seastar/core/future.hh>
#include <seastar/core/sleep.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <exception>
#include <stdexcept>
#include <vector>

namespace ssx {

using ::testing::ElementsAre;
using ::testing::IsEmpty;

TEST(WorkQueue, Order) {
    work_queue queue([](auto ex) { std::rethrow_exception(ex); });

    std::vector<int> values;
    ss::promise<> p;

    queue.submit([&] {
        values.push_back(1);
        return ss::now();
    });
    queue.submit([&] {
        values.push_back(2);
        return ss::now();
    });
    queue.submit([&] {
        values.push_back(3);
        p.set_value();
        return ss::now();
    });

    EXPECT_THAT(values, IsEmpty());
    p.get_future().get();
    EXPECT_THAT(values, ElementsAre(1, 2, 3));
    queue.shutdown().get();
}

TEST(WorkQueue, FailuresDoNotStopTheQueue) {
    int error_count = 0;
    work_queue queue([&error_count](auto) { ++error_count; });

    std::vector<int> values;
    ss::promise<> p;

    queue.submit([&] {
        values.push_back(1);
        return ss::now();
    });
    queue.submit(
      [&]() -> ss::future<> { throw std::runtime_error("oh noes!"); });
    queue.submit([&] {
        values.push_back(3);
        p.set_value();
        return ss::now();
    });

    EXPECT_THAT(values, IsEmpty());
    EXPECT_EQ(error_count, 0);
    p.get_future().get();
    EXPECT_EQ(error_count, 1);
    EXPECT_THAT(values, ElementsAre(1, 3));
    queue.shutdown().get();
}

TEST(WorkQueue, ShutdownStopsTheQueueImmediately) {
    work_queue queue([](auto ex) { std::rethrow_exception(ex); });
    int a = 1;
    queue.submit([&] {
        a = 2;
        return ss::now();
    });
    queue.shutdown().get();
    EXPECT_EQ(a, 1);
}

} // namespace ssx
