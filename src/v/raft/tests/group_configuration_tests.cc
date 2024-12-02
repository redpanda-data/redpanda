// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/metadata.h"
#include "raft/group_configuration.h"
#include "test_utils/test.h"
#include "utils/unresolved_address.h"

#include <fmt/ostream.h>

namespace {
raft::vnode create_vnode(int32_t id) {
    return {model::node_id(id), model::revision_id(0)};
}

raft::group_configuration create_configuration(std::vector<raft::vnode> nodes) {
    return {std::move(nodes), model::revision_id(0)};
}

} // namespace

TEST(test_raft_group_configuration, test_demoting_removed_voters) {
    raft::group_configuration test_grp = create_configuration(
      {create_vnode(3)});

    // add nodes
    test_grp.add(create_vnode(1), model::revision_id{0}, std::nullopt);
    test_grp.promote_to_voter(
      raft::vnode(model::node_id{1}, model::revision_id(0)));
    test_grp.finish_configuration_transition();

    test_grp.add(create_vnode(2), model::revision_id{0}, std::nullopt);
    test_grp.promote_to_voter(
      raft::vnode(model::node_id{2}, model::revision_id(0)));
    test_grp.finish_configuration_transition();

    test_grp.finish_configuration_transition();
    // remove single broker
    test_grp.remove(create_vnode(1), model::revision_id{0});
    // finish configuration transition

    ASSERT_TRUE(test_grp.maybe_demote_removed_voters());
    ASSERT_EQ(test_grp.old_config()->voters.size(), 2);
    // node 0 was demoted since it was removed from the cluster
    ASSERT_EQ(test_grp.old_config()->learners[0], create_vnode(1));
    // assert that operation is idempotent
    ASSERT_FALSE(test_grp.maybe_demote_removed_voters());
}

TEST(test_raft_group_configuration, test_aborting_configuration_change) {
    raft::group_configuration test_grp = raft::group_configuration(
      {create_vnode(3)}, model::revision_id(0));

    auto original_voters = test_grp.current_config().voters;
    auto original_nodes = test_grp.all_nodes();
    // add brokers
    test_grp.add(create_vnode(1), model::revision_id{0}, std::nullopt);

    // abort change
    test_grp.abort_configuration_change(model::revision_id{1});

    ASSERT_EQ(test_grp.get_state(), raft::configuration_state::simple);
    ASSERT_EQ(test_grp.current_config().voters, original_voters);
    ASSERT_EQ(test_grp.all_nodes(), original_nodes);
}

TEST(
  test_raft_group_configuration,
  test_reverting_configuration_change_when_adding) {
    raft::group_configuration test_grp = raft::group_configuration(
      {create_vnode(3)}, model::revision_id(0));

    // add brokers
    test_grp.add(create_vnode(1), model::revision_id{0}, std::nullopt);

    // abort change
    test_grp.cancel_configuration_change(model::revision_id{1});

    ASSERT_EQ(test_grp.get_state(), raft::configuration_state::simple);
    ASSERT_EQ(test_grp.all_nodes().size(), 1);
    ASSERT_EQ(test_grp.current_config().voters.size(), 1);
    ASSERT_EQ(test_grp.current_config().learners.size(), 0);
}
