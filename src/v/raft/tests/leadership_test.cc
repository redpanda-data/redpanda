// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/tests/raft_fixture.h"
#include "test_utils/async.h"

#include <gmock/gmock.h>
using namespace raft;
struct leadership_test_fixture : raft_fixture {
    ::testing::AssertionResult assert_single_leader() {
        auto leaders = std::ranges::count_if(
          nodes(), [](auto& node) { return node.second->raft()->is_leader(); });

        if (leaders != 1) {
            return ::testing::AssertionFailure()
                   << "Expected 1 leader, got " << leaders;
        }
        return ::testing::AssertionSuccess();
    }

    ::testing::AssertionResult assert_leadership_stable(model::node_id id) {
        ss::sleep(get_election_timeout() * 3).get();
        if (!node(id).raft()->is_leader()) {
            return ::testing::AssertionFailure()
                   << "Expected leader to be stable, previous: " << id
                   << " current: " << wait_for_leader(10s).get();
        }
        return ::testing::AssertionSuccess();
    }

    void wait_for_no_leader() {
        tests::cooperative_spin_wait_with_timeout(10s, [this] {
            return std::ranges::all_of(nodes(), [](auto& node) {
                return node.second->raft()->is_leader() == false;
            });
        }).get();
    }

    ::testing::AssertionResult assert_stable_no_leader() {
        ss::sleep(get_election_timeout() * 3).get();
        auto no_leader = std::ranges::all_of(nodes(), [](auto& node) {
            return node.second->raft()->is_leader() == false;
        });
        if (!no_leader) {
            return ::testing::AssertionFailure()
                   << "Raft group is expected to have no leader";
        }
        return ::testing::AssertionSuccess();
    }
};

TEST_F(leadership_test_fixture, test_single_node_group) {
    create_simple_group(1).get();
    auto leader_id = wait_for_leader(10s).get();

    ASSERT_TRUE(assert_single_leader());

    // leader should be stable when there are no failures
    ASSERT_TRUE(assert_leadership_stable(leader_id));
};

TEST_F(leadership_test_fixture, test_leader_is_elected_in_group) {
    create_simple_group(3).get();
    auto leader_id = wait_for_leader(10s).get();

    ASSERT_TRUE(assert_single_leader());

    // leader should be stable when there are no failures
    ASSERT_TRUE(assert_leadership_stable(leader_id));
    wait_for_committed_offset(node(leader_id).raft()->dirty_offset(), 10s)
      .get();
};

TEST_F(
  leadership_test_fixture, test_leader_is_elected_after_current_leader_fail) {
    create_simple_group(3).get();
    auto leader_id = wait_for_leader(10s).get();

    ASSERT_TRUE(assert_single_leader());

    // leader should be stable when there are no failures
    ASSERT_TRUE(assert_leadership_stable(leader_id));
    stop_node(leader_id).get();

    auto new_leader_id = wait_for_leader(10s).get();
    ASSERT_TRUE(assert_single_leader());
    // require leader id has changed
    ASSERT_NE(leader_id, new_leader_id);
    wait_for_committed_offset(node(new_leader_id).raft()->dirty_offset(), 10s)
      .get();

    ASSERT_TRUE(assert_leadership_stable(new_leader_id));
}

TEST_F(
  leadership_test_fixture,
  test_leader_is_not_elected_when_there_is_no_majority) {
    create_simple_group(3).get();
    auto leader_id = wait_for_leader(10s).get();

    ASSERT_TRUE(assert_single_leader());

    // leader should be stable when there are no failures
    ASSERT_TRUE(assert_leadership_stable(leader_id));
    stop_node(leader_id).get();

    auto new_leader_id = wait_for_leader(10s).get();
    ASSERT_TRUE(assert_single_leader());
    stop_node(new_leader_id).get();
    wait_for_no_leader();
    ss::sleep(get_election_timeout() * 3).get();
    ASSERT_TRUE(assert_stable_no_leader());
    // leader is re-elected when nodes are back up
    add_node(leader_id, model::revision_id{0});
    node(leader_id).init_and_start(all_vnodes()).get();
    leader_id = wait_for_leader(10s).get();
    ASSERT_TRUE(assert_leadership_stable(leader_id));
}
