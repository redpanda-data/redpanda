// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "seastarx.h"
#include "ssx/this_fiber_id.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/future.hh>
#include <seastar/core/gate.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

#include <stdexcept>

using ssx::this_fiber_id;

SEASTAR_THREAD_TEST_CASE(test_fiber_id_smoke_test) {
    auto id = this_fiber_id();
    BOOST_REQUIRE(id != 0);
}

SEASTAR_THREAD_TEST_CASE(test_fiber_id_nested) {
    auto root_id = this_fiber_id();
    ss::yield()
      .then([root_id] {
          auto lvl1 = this_fiber_id();
          BOOST_REQUIRE_EQUAL(root_id, lvl1);
          return ss::yield().then([root_id] {
              auto lvl2 = this_fiber_id();
              BOOST_REQUIRE_EQUAL(root_id, lvl2);
          });
      })
      .get();
}

SEASTAR_THREAD_TEST_CASE(test_fiber_id_background) {
    auto root_id = this_fiber_id();
    ss::gate g;
    (void)ss::with_gate(g, [root_id] {
        // here the code runs in the same fiber
        return ss::yield().then([root_id] {
            // first preemption happense here
            auto lvl1 = this_fiber_id();
            BOOST_REQUIRE(root_id != lvl1);
            return ss::yield().then([root_id, lvl1] {
                auto lvl2 = this_fiber_id();
                BOOST_REQUIRE(root_id != lvl2);
                BOOST_REQUIRE_EQUAL(lvl1, lvl2);
            });
        });
    });
    g.close().get();
}
