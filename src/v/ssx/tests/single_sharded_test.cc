// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/future-util.h"
#include "ssx/single_sharded.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <cstddef>
#include <memory>

struct counter {
    int started = 0;
    int called_foo = 0;
    int stopped = 0;
    ss::future<> stop() { return ss::make_ready_future(); }
};

struct single_service {
    int member = 22;
    counter& cntr;
    single_service(counter& cntr, ss::sharded<counter>& cntrs, int a)
      : cntr(cntr) {
        BOOST_REQUIRE_EQUAL(&cntr, &cntrs.local());
        BOOST_REQUIRE_EQUAL(a, 1);
        ++cntr.started;
    }
    int foo(
      ss::sharded<counter>& cntrs,
      int a,
      std::vector<int>&& v,
      std::unique_ptr<int> uptr) {
        BOOST_REQUIRE_EQUAL(&cntr, &cntrs.local());
        BOOST_REQUIRE_EQUAL(a, 1);
        // make sure it hasn't accidentally been moved-from
        BOOST_REQUIRE_EQUAL(v.size(), 2);
        // same, but for an item that cannot be copied
        BOOST_REQUIRE_EQUAL(bool(uptr), true);
        ++cntr.called_foo;
        member = 23;
        return 42;
    }
    ss::future<> stop() {
        ++cntr.stopped;
        return ss::make_ready_future();
    }
};

struct caller {
    ssx::single_sharded<single_service>& sngl;
    ss::sharded<counter>& cntrs;
    explicit caller(
      ssx::single_sharded<single_service>& sngl, ss::sharded<counter>& cntrs)
      : sngl(sngl)
      , cntrs(cntrs) {}
    ss::future<> call_thrice() {
        std::unique_ptr<std::vector<int>> vec_uptr;
        auto fut = ssx::now(1);
        int ret;

        // pointer to method
        vec_uptr = std::make_unique<std::vector<int>>(2, 0);
        fut = sngl.invoke_on_instance(
          &single_service::foo,
          std::ref(cntrs),
          1,
          std::move(*vec_uptr),
          std::make_unique<int>());
        vec_uptr = nullptr;
        ret = co_await std::move(fut);
        BOOST_REQUIRE_EQUAL(ret, 42);

        // rvalue ref to non-copiable lambda
        auto mvbl_lambda = [ensure_nocopy = std::make_unique<std::tuple<>>()](
                             single_service& ss,
                             ss::sharded<counter>& cntrs,
                             int a,
                             std::vector<int>&& v,
                             std::unique_ptr<int>&& ui) {
            return ss.foo(cntrs, a, std::move(v), std::move(ui));
        };
        auto ml_uptr = std::make_unique<decltype(mvbl_lambda)>(
          std::move(mvbl_lambda));
        vec_uptr = std::make_unique<std::vector<int>>(2, 0);
        fut = sngl.invoke_on_instance(
          std::move(*ml_uptr),
          std::ref(cntrs),
          1,
          std::move(*vec_uptr),
          std::make_unique<int>());
        vec_uptr = nullptr;
        ml_uptr = nullptr;
        ret = co_await std::move(fut);
        BOOST_REQUIRE_EQUAL(ret, 42);

        // lvalue reference to copyable lambda (sadly invoke_on copies)
        auto cpbl_lambda = [](
                             single_service& ss,
                             ss::sharded<counter>& cntrs,
                             int a,
                             std::vector<int>&& v,
                             std::unique_ptr<int>&& ui) {
            return ss.foo(cntrs, a, std::move(v), std::move(ui));
        };
        auto cl_uptr = std::make_unique<decltype(cpbl_lambda)>(
          std::move(cpbl_lambda));
        vec_uptr = std::make_unique<std::vector<int>>(2, 0);
        fut = sngl.invoke_on_instance(
          *cl_uptr,
          std::ref(cntrs),
          1,
          std::move(*vec_uptr),
          std::make_unique<int>());
        vec_uptr = nullptr;
        ml_uptr = nullptr;
        ret = co_await std::move(fut);
        BOOST_REQUIRE_EQUAL(ret, 42);
    }
    ss::future<> stop() { return ss::make_ready_future(); }
};

SEASTAR_THREAD_TEST_CASE(single_sharded) {
    ss::shard_id the_shard = ss::smp::count - 1;

    ss::sharded<counter> counters;
    ssx::single_sharded<single_service> single;
    ss::sharded<caller> callers;

    counters.start().get();
    single
      .start_on(
        the_shard,
        ss::sharded_parameter(
          [&counters]() { return std::ref(counters.local()); }),
        std::ref(counters),
        1)
      .get();
    callers.start(std::ref(single), std::ref(counters)).get();

    callers.invoke_on_all(&caller::call_thrice).get();

    ss::smp::submit_to(the_shard, [&single, &counters]() {
        single.local().foo(
          counters, 1, std::vector<int>{0, 0}, std::make_unique<int>());
        BOOST_REQUIRE_EQUAL(std::as_const(single).local().member, 23);
    }).get();

    callers.stop().get();
    single.stop().get();
    counters
      .invoke_on_all([the_shard](counter cntr) {
          bool on_the_shard = the_shard == ss::this_shard_id();
          BOOST_REQUIRE_EQUAL(cntr.started, on_the_shard ? 1 : 0);
          BOOST_REQUIRE_EQUAL(
            cntr.called_foo, on_the_shard ? ss::smp::count * 3 + 1 : 0);
          BOOST_REQUIRE_EQUAL(cntr.stopped, on_the_shard ? 1 : 0);
      })
      .get();
    counters.stop().get();
}

SEASTAR_THREAD_TEST_CASE(single_sharded_wrong_shard) {
    BOOST_REQUIRE(ss::smp::count > 1);

    ss::shard_id the_shard = ss::smp::count - 2;
    ss::shard_id wrong_shard = ss::smp::count - 1;

    ss::sharded<counter> counters;
    ssx::single_sharded<single_service> single;

    counters.start().get();
    single
      .start_on(
        the_shard,
        ss::sharded_parameter(
          [&counters]() { return std::ref(counters.local()); }),
        std::ref(counters),
        1)
      .get();

    BOOST_REQUIRE_THROW(
      ss::smp::submit_to(wrong_shard, [&single]() { single.local(); }).get(),
      ss::no_sharded_instance_exception);
    BOOST_REQUIRE_THROW(
      ss::smp::submit_to(
        wrong_shard, [&single]() { std::as_const(single).local(); })
        .get(),
      ss::no_sharded_instance_exception);

    single.stop().get();
    counters.stop().get();
}
