/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "ssx/event.h"

#include <seastar/core/manual_clock.hh>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace ssx {

using namespace std::chrono_literals;

TEST(Event, SimpleUsage) {
    event ev{"test-event"};
    EXPECT_FALSE(ev.is_set());
    ev.set();
    EXPECT_TRUE(ev.is_set());
    ev.set();
    EXPECT_TRUE(ev.is_set());
    ev.wait().get();
    EXPECT_FALSE(ev.is_set());
}

TEST(Event, SeveralWaiters) {
    event ev{"test-event"};
    auto f1 = ev.wait();
    auto f2 = ev.wait();
    EXPECT_FALSE(f1.available());
    EXPECT_FALSE(f2.available());
    ev.set();
    f1.get();
    f2.get();
    EXPECT_FALSE(ev.is_set());
}

TEST(Event, Timeouts) {
    basic_event<ss::manual_clock> ev{"test-event"};
    auto f1 = ev.wait(3s);
    auto f2 = ev.wait(1s);
    EXPECT_FALSE(f1.available());
    EXPECT_FALSE(f2.available());
    ss::manual_clock::advance(2s);
    ev.set();
    EXPECT_TRUE(f1.get());
    EXPECT_FALSE(f2.get());
    EXPECT_FALSE(ev.is_set());
}

TEST(Event, Broken) {
    event ev{"test-event"};
    auto f1 = ev.wait();
    auto f2 = ev.wait(1s);
    EXPECT_FALSE(f1.available());
    EXPECT_FALSE(f2.available());
    ev.broken();
    EXPECT_THROW(f1.get(), ss::broken_semaphore);
    EXPECT_THROW(f2.get(), ss::broken_semaphore);
}

} // namespace ssx
