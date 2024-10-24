// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/tristate.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(tristate_having_value) {
    // with_value
    tristate<int> has_value(10);
    BOOST_REQUIRE_EQUAL(has_value.is_disabled(), false);
    BOOST_REQUIRE_EQUAL(has_value.has_optional_value(), true);
    BOOST_REQUIRE_EQUAL(*has_value, 10);
    BOOST_REQUIRE_EQUAL(has_value.value(), 10);
}

BOOST_AUTO_TEST_CASE(disabled_tristate) {
    // disabled
    tristate<int> disabled;
    BOOST_REQUIRE_EQUAL(disabled.is_disabled(), true);
    BOOST_REQUIRE_EQUAL(disabled.has_optional_value(), false);
}

BOOST_AUTO_TEST_CASE(tristate_with_empty_value) {
    // empty
    tristate<int> empty(std::nullopt);
    BOOST_REQUIRE_EQUAL(empty.is_disabled(), false);
    BOOST_REQUIRE_EQUAL(empty.has_optional_value(), false);
}
