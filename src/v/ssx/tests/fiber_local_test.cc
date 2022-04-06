// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "seastarx.h"
#include "ssx/fiber_local_storage.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <stdexcept>

SEASTAR_THREAD_TEST_CASE(test_fiber_local_iface) {
    ssx::fiber_local<struct _test_tag_1, int> fls;
    fls.set(42);
    auto res = fls.get();
    BOOST_REQUIRE_EQUAL(res, 42);
}

SEASTAR_THREAD_TEST_CASE(test_fiber_local_nested) {
    ssx::fiber_local<struct _test_tag_4, int> fls(42);
    ss::yield()
      .then([] {
          ssx::fiber_local_selector<struct _test_tag_4, int> sel;
          auto res = sel.get();
          BOOST_REQUIRE_EQUAL(res, 42);
          return ss::yield().then([] {
              ssx::fiber_local_selector<struct _test_tag_4, int> sel;
              auto res = sel.get();
              BOOST_REQUIRE_EQUAL(res, 42);
          });
      })
      .get();
}

SEASTAR_THREAD_TEST_CASE(test_fiber_local_shadowing) {
    ssx::fiber_local<struct _test_tag_4, int> fls(42);
    ss::yield()
      .then([]() -> ss::future<> {
          ssx::fiber_local_selector<struct _test_tag_4, int> sel;
          auto res = sel.get();
          BOOST_REQUIRE_EQUAL(res, 42);
          co_await ss::yield();
          res = sel.get();
          BOOST_REQUIRE_EQUAL(res, 42);
          {
              // This should shadow the original fiber_local
              ssx::fiber_local<struct _test_tag_4, int> fls(24);
              co_await ss::yield();
              res = sel.get();
              BOOST_REQUIRE_EQUAL(res, 24);
          }
          res = sel.get();
          BOOST_REQUIRE_EQUAL(res, 42);
      })
      .get();
}

SEASTAR_THREAD_TEST_CASE(test_fiber_local_lifetime) {
    ss::yield()
      .then([] {
          ssx::fiber_local<struct _test_tag_5, int> fls(42);
          ssx::fiber_local_selector<struct _test_tag_5, int> sel;
          auto res = sel.get();
          BOOST_REQUIRE_EQUAL(res, 42);

          return ss::yield().then([]() mutable {
              ssx::fiber_local<struct _test_tag_5, int> fls(32);
              std::vector<int> expected = {32};
              std::vector<int> actual;
              for (auto it = fls.begin(); it != fls.end(); it++) {
                  actual.push_back(it->get());
              }
              BOOST_REQUIRE(expected == actual);
          });
      })
      .get();
}

SEASTAR_THREAD_TEST_CASE(test_fiber_local_iter) {
    ss::yield()
      .then([] {
          ssx::fiber_local<struct _test_tag_6, int> fls(42);
          ssx::fiber_local_selector<_test_tag_6, int> sel;
          auto res = sel.get();
          BOOST_REQUIRE_EQUAL(res, 42);

          return ss::yield()
            .then([]() mutable {
                ssx::fiber_local<struct _test_tag_6, int> fls(43);
                ssx::fiber_local_selector<struct _test_tag_6, int> sel;
                auto res = sel.get();
                BOOST_REQUIRE_EQUAL(res, 43);
                return ss::yield().then([outer = std::move(fls)]() mutable {
                    ssx::fiber_local<struct _test_tag_6, int> fls(44);
                    std::vector<int> expected = {44, 43, 42};
                    std::vector<int> actual;
                    for (auto it = fls.begin(); it != fls.end(); it++) {
                        actual.push_back(it->get());
                    }
                    BOOST_REQUIRE(expected == actual);
                });
            })
            .finally([fls = std::move(fls)] {});
      })
      .get();
}
