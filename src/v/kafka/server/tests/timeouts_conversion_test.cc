// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/timeout.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(test_timeouts_conversion) {
    using namespace std::chrono_literals;
    BOOST_REQUIRE_EQUAL(
      kafka::to_timeout(-2ms).time_since_epoch().count(),
      model::no_timeout.time_since_epoch().count());
    BOOST_REQUIRE_EQUAL(
      kafka::to_timeout(-1ms).time_since_epoch().count(),
      model::no_timeout.time_since_epoch().count());
    BOOST_REQUIRE_EQUAL(
      kafka::to_timeout(0ms).time_since_epoch().count(),
      model::no_timeout.time_since_epoch().count());
    BOOST_REQUIRE_GE(
      kafka::to_timeout(5000ms).time_since_epoch().count(),
      (model::timeout_clock::now() + 5s).time_since_epoch().count());
};
