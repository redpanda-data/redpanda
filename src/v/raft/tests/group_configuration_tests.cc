// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/group_configuration.h"
#include "utils/unresolved_address.h"
#define BOOST_TEST_MODULE raft
#include "raft/types.h"

#include <boost/test/unit_test.hpp>

model::broker create_broker(int32_t id) {
    return model::broker(
      model::node_id{id},
      unresolved_address("127.0.0.1", 9002),
      unresolved_address("127.0.0.1", 1234),
      std::nullopt,
      model::broker_properties{});
}

BOOST_AUTO_TEST_CASE(should_return_true_as_it_contains_learner) {
    raft::group_configuration test_grp = raft::group_configuration(
      {create_broker(1)}, model::revision_id(0));

    auto contains = test_grp.contains_broker(model::node_id(1));
    BOOST_REQUIRE_EQUAL(contains, true);
}

BOOST_AUTO_TEST_CASE(should_return_true_as_it_contains_voter) {
    raft::group_configuration test_grp = raft::group_configuration(
      {create_broker(1)}, model::revision_id(0));

    auto contains = test_grp.contains_broker(model::node_id(1));
    BOOST_REQUIRE_EQUAL(contains, true);
}

BOOST_AUTO_TEST_CASE(should_return_false_as_it_does_not_contain_machine) {
    raft::group_configuration test_grp = raft::group_configuration(
      {create_broker(3)}, model::revision_id(0));

    auto contains = test_grp.contains_broker(model::node_id(1));
    BOOST_REQUIRE_EQUAL(contains, false);
}
