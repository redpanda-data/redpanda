// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/metadata.h"
#include "net/unresolved_address.h"
#include "raft/group_configuration.h"

#include <boost/test/tools/old/interface.hpp>
#define BOOST_TEST_MODULE raft
#include "raft/types.h"

#include <boost/test/unit_test.hpp>

model::broker create_broker(int32_t id) {
    return model::broker(
      model::node_id{id},
      net::unresolved_address("127.0.0.1", 9002),
      net::unresolved_address("127.0.0.1", 1234),
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

BOOST_AUTO_TEST_CASE(test_demoting_removed_voters) {
    raft::group_configuration test_grp = raft::group_configuration(
      {create_broker(3)}, model::revision_id(0));

    // add brokers
    test_grp.add({create_broker(1), create_broker(2)}, model::revision_id{0});
    auto demoted = test_grp.maybe_demote_removed_voters();
    BOOST_REQUIRE_EQUAL(demoted, false);

    // promote added nodes to voteres
    test_grp.promote_to_voter(
      raft::vnode(model::node_id{1}, model::revision_id(0)));
    test_grp.promote_to_voter(
      raft::vnode(model::node_id{2}, model::revision_id(0)));
    demoted = test_grp.maybe_demote_removed_voters();
    BOOST_REQUIRE_EQUAL(demoted, false);

    test_grp.discard_old_config();

    // remove single broker
    test_grp.remove({model::node_id(1)});
    demoted = test_grp.maybe_demote_removed_voters();
    BOOST_REQUIRE_EQUAL(demoted, true);
    BOOST_REQUIRE_EQUAL(test_grp.old_config()->voters.size(), 2);
    // node 0 was demoted since it was removed from the cluster
    BOOST_REQUIRE_EQUAL(
      test_grp.old_config()->learners[0],
      raft::vnode(model::node_id{1}, model::revision_id(0)));
    // assert that operation is idempotent
    demoted = test_grp.maybe_demote_removed_voters();
    BOOST_REQUIRE_EQUAL(demoted, false);
}

BOOST_AUTO_TEST_CASE(test_aborting_configuration_change) {
    raft::group_configuration test_grp = raft::group_configuration(
      {create_broker(3)}, model::revision_id(0));

    auto original_brokers = test_grp.brokers();
    auto original_voters = test_grp.current_config().voters;

    // add brokers
    test_grp.add({create_broker(1), create_broker(2)}, model::revision_id{0});
    auto demoted = test_grp.maybe_demote_removed_voters();
    BOOST_REQUIRE_EQUAL(demoted, false);

    // abort change
    test_grp.abort_configuration_change();

    BOOST_REQUIRE_EQUAL(test_grp.type(), raft::configuration_type::simple);
    BOOST_REQUIRE_EQUAL(test_grp.brokers(), original_brokers);
    BOOST_REQUIRE_EQUAL(test_grp.current_config().voters, original_voters);
}

BOOST_AUTO_TEST_CASE(test_reverting_configuration_change) {
    raft::group_configuration test_grp = raft::group_configuration(
      {create_broker(3)}, model::revision_id(0));

    // add brokers
    test_grp.add({create_broker(1), create_broker(2)}, model::revision_id{0});
    auto demoted = test_grp.maybe_demote_removed_voters();
    BOOST_REQUIRE_EQUAL(demoted, false);
    auto current = test_grp.current_config();
    auto old = *test_grp.old_config();

    // abort change
    test_grp.revert_configuration_change();

    BOOST_REQUIRE_EQUAL(test_grp.type(), raft::configuration_type::joint);
    BOOST_REQUIRE_EQUAL(test_grp.brokers().size(), 3);
    BOOST_REQUIRE_EQUAL(test_grp.current_config(), old);
    BOOST_REQUIRE_EQUAL(test_grp.old_config(), current);
}
