
/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "base/seastarx.h"
#include "utils/object_pool.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

SEASTAR_THREAD_TEST_CASE(test_waiting_on_object) {
    object_pool<ss::sstring> pool;
    ss::abort_source as;
    auto f = pool.allocate_object(as);

    BOOST_REQUIRE(!f.available());
    BOOST_REQUIRE(!f.failed());

    pool.release_object("obj1");

    BOOST_REQUIRE(f.available());
    BOOST_REQUIRE(!f.failed());

    auto obj = f.get();

    BOOST_REQUIRE(obj == "obj1");
};

SEASTAR_THREAD_TEST_CASE(testing_ready_object) {
    object_pool<std::unique_ptr<int>> pool;
    pool.release_object(std::make_unique<int>(1));
    ss::abort_source as;
    auto f = pool.allocate_object(as);

    BOOST_REQUIRE(f.available());
    BOOST_REQUIRE(!f.failed());

    auto obj = f.get();

    BOOST_REQUIRE(*obj == 1);
}

struct move_only {
    move_only(ss::sstring s)
      : s(s) {}
    move_only(move_only&&) = default;
    move_only& operator=(move_only&&) = default;
    move_only(const move_only&) = delete;
    move_only& operator=(const move_only&) = delete;

    ss::sstring s;
};

SEASTAR_THREAD_TEST_CASE(testing_move_only_object) {
    object_pool<move_only> pool;
    pool.release_object(move_only("test"));
    ss::abort_source as;
    auto f = pool.allocate_object(as);

    BOOST_REQUIRE(f.available());
    BOOST_REQUIRE(!f.failed());

    auto obj = f.get();

    BOOST_REQUIRE(obj.s == "test");
}

SEASTAR_THREAD_TEST_CASE(test_abort_on_wait) {
    object_pool<ss::sstring> pool;
    {
        ss::abort_source as;
        auto f = pool.allocate_object(as);

        BOOST_REQUIRE(!f.available());
        BOOST_REQUIRE(!f.failed());

        as.request_abort();
        pool.release_object("obj1");

        BOOST_REQUIRE(f.available());
        BOOST_REQUIRE(f.failed());
        BOOST_REQUIRE_THROW(f.get(), ss::abort_requested_exception);
    }
    {
        ss::abort_source as;
        auto f = pool.allocate_object(as);

        BOOST_REQUIRE(f.available());
        BOOST_REQUIRE(!f.failed());

        auto obj = f.get();

        BOOST_REQUIRE(obj == "obj1");
    }
}

SEASTAR_THREAD_TEST_CASE(test_abort_after_wait) {
    object_pool<ss::sstring> pool;
    ss::abort_source as;
    auto f = pool.allocate_object(as);

    BOOST_REQUIRE(!f.available());
    BOOST_REQUIRE(!f.failed());

    pool.release_object("obj1");
    as.request_abort();

    BOOST_REQUIRE(f.available());
    BOOST_REQUIRE(!f.failed());

    auto obj = f.get();

    BOOST_REQUIRE(obj == "obj1");
}

SEASTAR_THREAD_TEST_CASE(test_ordering_waiters) {
    object_pool<int> pool;
    ss::abort_source as;
    auto f1 = pool.allocate_object(as);
    auto f2 = pool.allocate_object(as);
    auto f3 = pool.allocate_object(as);
    pool.release_object(1);
    pool.release_object(2);
    pool.release_object(3);

    BOOST_REQUIRE(f1.available());
    BOOST_REQUIRE(!f1.failed());
    BOOST_REQUIRE(f1.get() == 1);

    BOOST_REQUIRE(f2.available());
    BOOST_REQUIRE(!f2.failed());
    BOOST_REQUIRE(f2.get() == 2);

    BOOST_REQUIRE(f3.available());
    BOOST_REQUIRE(!f3.failed());
    BOOST_REQUIRE(f3.get() == 3);
}
