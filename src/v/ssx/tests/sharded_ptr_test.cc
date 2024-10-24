// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "ssx/sharded_ptr.h"

#include <seastar/core/semaphore.hh>
#include <seastar/core/smp.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

SEASTAR_THREAD_TEST_CASE(test_sharded_ptr_basic_ops) {
    ssx::sharded_ptr<int> p0;
    BOOST_REQUIRE(!p0);
    BOOST_REQUIRE_EQUAL(p0.shard_id(), ss::this_shard_id());

    // Test operator bool (before reset)
    for (auto i : boost::irange(0u, ss::smp::count)) {
        ss::smp::submit_to(i, [&]() { BOOST_REQUIRE(!p0); }).get();
    }

    // Test reset
    p0.reset(std::make_shared<int>(43)).get();
    p0.reset(43).get();

    // Test operator bool and deref (after reset)
    for (auto i : boost::irange(0u, ss::smp::count)) {
        ss::smp::submit_to(i, [&]() {
            BOOST_REQUIRE(p0 && p0.operator*() == 43);
            BOOST_REQUIRE(p0 && *p0.operator->() == 43);
        }).get();
    }

    // Test operator bool (after stop)
    p0.stop().get();
    for (auto i : boost::irange(0u, ss::smp::count)) {
        ss::smp::submit_to(i, [&]() { BOOST_REQUIRE(!p0); }).get();
    }

    // Test reset (after stop)
    try {
        p0.reset().get();
        BOOST_FAIL("Expected exception");
    } catch (const ss::broken_semaphore&) {
        // Success
    } catch (...) {
        BOOST_FAIL("Unexpected exception");
    }

    // Test stop (after stop)
    try {
        p0.stop().get();
        BOOST_FAIL("Expected exception");
    } catch (const ss::broken_semaphore&) {
        // Success
    } catch (...) {
        BOOST_FAIL("Unexpected exception");
    }
}

SEASTAR_THREAD_TEST_CASE(test_sharded_ptr_stop_without_reset) {
    ssx::sharded_ptr<int> p0;
    p0.stop().get();
}

SEASTAR_THREAD_TEST_CASE(test_sharded_ptr_shared) {
    ssx::sharded_ptr<int> p0;
    p0.reset(42).get();

    std::shared_ptr<int> shared = p0.local();
    std::weak_ptr<int> weak = p0.local();
    BOOST_REQUIRE(p0 && *p0 == 42);

    p0.reset().get();
    BOOST_REQUIRE(shared.get() != nullptr);
    BOOST_REQUIRE(weak.lock().get() != nullptr);

    shared.reset();
    BOOST_REQUIRE(shared.get() == nullptr);
    BOOST_REQUIRE(weak.lock().get() == nullptr);
}

SEASTAR_THREAD_TEST_CASE(test_sharded_ptr_move) {
    ssx::sharded_ptr<int> p0;
    p0.reset(42).get();

    std::shared_ptr<int> shared = p0.local();

    // Move construction
    auto p1{std::move(p0)};
    BOOST_REQUIRE(shared && *shared == 42);
    BOOST_REQUIRE(p1 && p1.local() && *p1 == 42);

    // Move assignment
    p0 = std::move(p1);
    BOOST_REQUIRE(shared && *shared == 42);
    BOOST_REQUIRE(p0 && p0.local() && *p0 == 42);
}

SEASTAR_THREAD_TEST_CASE(test_sharded_ptr_update) {
    ssx::sharded_ptr<int> p0;
    p0.update([](auto x) { return x + 1; }).get();
    p0.update([](auto x) { return x + 1; }).get();

    std::shared_ptr<int> shared = p0.local();
    BOOST_REQUIRE(shared && *shared == 2);
}

SEASTAR_THREAD_TEST_CASE(test_sharded_ptr_update_shared) {
    ssx::sharded_ptr<int> p0;
    p0.update_shared([](auto /* x */) { return std::make_shared<int>(1); })
      .get();
    p0.update_shared([](auto x) { return std::make_shared<int>(*x + 1); })
      .get();

    std::shared_ptr<int> shared = p0.local();
    BOOST_REQUIRE(shared && *shared == 2);
}
