// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"

#include <boost/test/unit_test.hpp>

model::topic test_tp = model::topic("test_topic");
model::topic test_view = model::topic_view("test_topic_view");

BOOST_AUTO_TEST_CASE(test_implicit_conversion_from_topic_to_view) {
    model::topic_view tv = test_tp;
    BOOST_TEST(test_tp() == tv());
}

BOOST_AUTO_TEST_CASE(test_implicit_conversion_from_view_to_topic) {
    model::topic tp = test_view;
    BOOST_TEST(tp() == test_view());
}
