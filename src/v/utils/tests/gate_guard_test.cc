// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/gate_guard.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(gate_guard_move_assign) {
    ss::gate gate1;
    ss::gate gate2;
    {
        gate_guard g1{gate1};
        gate_guard g2{gate2};
        BOOST_CHECK_EQUAL(gate1.get_count(), 1);
        BOOST_CHECK_EQUAL(gate2.get_count(), 1);
        g1 = std::move(g2);
    }
    BOOST_CHECK_EQUAL(gate1.get_count(), 0);
    BOOST_CHECK_EQUAL(gate2.get_count(), 0);
}
