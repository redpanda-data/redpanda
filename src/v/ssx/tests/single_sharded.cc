// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/single_sharded.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shard_id.hh>
#include <seastar/core/sharded.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <memory>

struct counter {
    int started = 0;
    int called_foo = 0;
    int called_bar = 0;
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
    void foo(ss::sharded<counter>& cntrs, int a, int&& b, int& c) {
        BOOST_REQUIRE_EQUAL(&cntr, &cntrs.local());
        BOOST_REQUIRE_EQUAL(a, 1);
        BOOST_REQUIRE_EQUAL(b, 2);
        BOOST_REQUIRE_EQUAL(c, -3);
        c = 3;
        ++cntr.called_foo;
        member = 23;
    }
    void bar(std::vector<int>&& v, std::unique_ptr<int> uptr) {
        BOOST_REQUIRE_EQUAL(v.size(), 2);
        BOOST_REQUIRE_EQUAL(bool(uptr), true);
        ++cntr.called_bar;
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
    ss::future<> call_twice() {
        co_await sngl.invoke_on_instance([this](single_service& sngl_inst) {
            int c = -3;
            sngl_inst.foo(cntrs, 1, 2, c);
            BOOST_REQUIRE_EQUAL(c, 3);
        });
        co_await sngl.invoke_on_instance(
          [](single_service& sngl_inst, std::vector<int>&& v) {
              sngl_inst.bar(std::move(v), std::make_unique<int>());
          },
          std::vector<int>{0, 0});
    }
    ss::future<> stop() { return ss::make_ready_future(); }
};

SEASTAR_THREAD_TEST_CASE(single_sharded) {
    ss::shard_id the_shard = ss::smp::count - 1;
    ss::shard_id wrong_shard = std::max(ss::shard_id{0}, the_shard - 1);

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

    callers.invoke_on_all([](caller& cllr) { return cllr.call_twice(); }).get();

    ss::smp::submit_to(the_shard, [&single, &counters]() {
        int c = -3;
        single.local().foo(counters, 1, 2, c);
        BOOST_REQUIRE_EQUAL(c, 3);
        BOOST_REQUIRE_EQUAL(std::as_const(single).local().member, 23);
    }).get();

    if (the_shard != wrong_shard) {
        BOOST_REQUIRE_THROW(
          ss::smp::submit_to(wrong_shard, [&single]() { single.local(); })
            .get(),
          ss::no_sharded_instance_exception);
        BOOST_REQUIRE_THROW(
          ss::smp::submit_to(
            wrong_shard, [&single]() { std::as_const(single).local(); })
            .get(),
          ss::no_sharded_instance_exception);
    }

    callers.stop().get();
    single.stop().get();
    counters
      .invoke_on_all([the_shard](counter cntr) {
          bool on_the_shard = the_shard == ss::this_shard_id();
          BOOST_REQUIRE_EQUAL(cntr.started, on_the_shard ? 1 : 0);
          BOOST_REQUIRE_EQUAL(
            cntr.called_foo, on_the_shard ? ss::smp::count + 1 : 0);
          BOOST_REQUIRE_EQUAL(
            cntr.called_bar, on_the_shard ? ss::smp::count : 0);
          BOOST_REQUIRE_EQUAL(cntr.stopped, on_the_shard ? 1 : 0);
      })
      .get();
    counters.stop().get();
}
