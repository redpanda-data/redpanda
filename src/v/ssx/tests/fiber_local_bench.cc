// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "ssx/fiber_local_storage.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/thread.hh>
#include <seastar/testing/perf_tests.hh>

PERF_TEST(fiber_local, test_fiber_local) {
    return ss::async([] {
        ssx::fiber_local<struct tag_type_t, int> fl(42);
        auto id
          = ss::yield()
              .then([] {
                  return ss::yield().then([] {
                      return ss::yield().then([] {
                          ssx::fiber_local_selector<struct tag_type_t, int>
                            selector;
                          perf_tests::start_measuring_time();
                          auto x = selector.get();
                          perf_tests::stop_measuring_time();
                          return ss::make_ready_future<int>(x);
                      });
                  });
              })
              .get();
        perf_tests::do_not_optimize(id);
    });
}

PERF_TEST(fiber_local, test_fiber_local_slow) {
    return ss::async([] {
        // actual fiber_local used by test
        ssx::fiber_local<struct tag_type_t, int> fl(42);
        // a bunch of dummy fiber_local instancess to make life harder
        ssx::fiber_local<struct tag_type_t, int> dummy[1000];

        auto id
          = ss::yield()
              .then([] {
                  return ss::yield().then([] {
                      return ss::yield().then([] {
                          ssx::fiber_local_selector<struct tag_type_t, int>
                            selector;
                          perf_tests::start_measuring_time();
                          auto x = selector.get();
                          perf_tests::stop_measuring_time();
                          return ss::make_ready_future<int>(x);
                      });
                  });
              })
              .get();
        perf_tests::do_not_optimize(id);
        perf_tests::do_not_optimize(dummy);
    });
}
