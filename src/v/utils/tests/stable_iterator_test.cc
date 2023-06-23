// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "utils/stable_iterator_adaptor.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(test_stable_iterator_basic) {
    using container = std::vector<int>;
    container test;
    int revision = 0;
    stable_iterator<container::const_iterator, int> it{
      [&] { return revision; }, test.begin()};
    revision++;
    BOOST_REQUIRE_THROW(*it, iterator_stability_violation);
}
