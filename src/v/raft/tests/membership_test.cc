// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/errc.h"
#include "raft/group_configuration.h"
#include "raft/tests/raft_fixture.h"
#include "raft/tests/raft_fixture_retry_policy.h"
#include "test_utils/async.h"

#include <gtest/gtest.h>

#include <system_error>
using namespace raft;

struct membership_test_fixture : raft_fixture {
    /**
     * After removing a node, check that the remaining nodes
     * have all advanced past it, to the same offset.
     *
     * @param removed_offset The offset of the just-removed group.
     */
    void verify_removed_node_is_behind(model::node_id removed_id) {
        tests::cooperative_spin_wait_with_timeout(3s, [this, removed_id] {
            auto leader_id = get_leader();
            if (!leader_id) {
                return false;
            }
            auto leader_offset = node(*leader_id).raft()->last_visible_index();
            auto& removed_node = node(removed_id);
            return removed_node.raft()->last_leader_visible_index()
                   < leader_offset;
        }).get();
    }
};

TEST_F(membership_test_fixture, add_one_replica) {
    create_simple_group(1).get();
    auto leader_id = wait_for_leader(10s).get();
    auto& leader = node(leader_id);
    auto res = leader.raft()
                 ->replicate(
                   make_batches(10, 10, 128),
                   replicate_options(consistency_level::quorum_ack))
                 .get();
    ASSERT_FALSE(res.has_error());
    auto& new_node = add_node(model::node_id(1), model::revision_id(0));
    new_node.init_and_start({}).get();
    retry_with_leader(
      default_timeout(),
      [vn = new_node.get_vnode()](raft_node_instance& leader) {
          return leader.raft()->add_group_member(vn, model::revision_id(0));
      })
      .get();
    wait_for_committed_offset(leader.raft()->dirty_offset(), 10s).get();

    ASSERT_EQ(leader.raft()->config().all_nodes().size(), 2);
}
TEST_F(membership_test_fixture, remove_non_leader) {
    create_simple_group(3).get();
    auto leader_id = wait_for_leader(10s).get();
    auto& leader = node(leader_id);
    auto res = leader.raft()
                 ->replicate(
                   make_batches(10, 10, 128),
                   replicate_options(consistency_level::quorum_ack))
                 .get();
    ASSERT_FALSE(res.has_error());
    auto to_remove_id = random_follower_id();

    auto r_res = retry_with_leader(
                   default_timeout(),
                   [this, to_remove_id](raft_node_instance& leader) {
                       return leader.raft()->remove_member(
                         node(*to_remove_id).get_vnode(),
                         model::revision_id(0));
                   })
                   .get();
    ASSERT_EQ(r_res, errc::success);

    leader.raft()
      ->replicate(
        make_batches(10, 10, 128),
        replicate_options(consistency_level::quorum_ack))
      .get();

    verify_removed_node_is_behind(*to_remove_id);
}

TEST_F(membership_test_fixture, remove_current_leader) {
    create_simple_group(3).get();
    auto leader_id = wait_for_leader(10s).get();
    auto& leader = node(leader_id);
    auto res = leader.raft()
                 ->replicate(
                   make_batches(10, 10, 128),
                   replicate_options(consistency_level::quorum_ack))
                 .get();
    ASSERT_FALSE(res.has_error());

    auto r_res = retry_with_leader(
                   default_timeout(),
                   [this, leader_id](raft_node_instance& leader) {
                       return leader.raft()->remove_member(
                         node(leader_id).get_vnode(), model::revision_id(0));
                   })
                   .get();
    ASSERT_EQ(r_res, errc::success);
    auto new_leader_id = wait_for_leader(10s).get();
    auto& new_leader = node(new_leader_id);
    new_leader.raft()
      ->replicate(
        make_batches(10, 10, 128),
        replicate_options(consistency_level::quorum_ack))
      .get();

    verify_removed_node_is_behind(leader_id);
}
