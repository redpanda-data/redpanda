// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "finjector/hbadger.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "raft/tests/raft_group_fixture.h"
#include "raft/types.h"

FIXTURE_TEST(test_single_node_group, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 1);
    gr.enable_all();

    wait_for_group_leader(gr);
    assert_at_most_one_leader(gr);

    validate_logs_replication(gr);

    // leader should be stable when there are no failures
    assert_stable_leadership(gr, 5);
};

FIXTURE_TEST(test_leader_is_elected_in_group, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();

    wait_for_group_leader(gr);
    assert_at_most_one_leader(gr);

    validate_logs_replication(gr);

    // leader should be stable when there are no failures
    assert_stable_leadership(gr, 5);
};

FIXTURE_TEST(
  test_leader_is_elected_after_current_leader_fail, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);

    assert_at_most_one_leader(gr);

    // Stop the current leader
    tstlog.info("Stopping current leader {}", leader_id);
    gr.disable_node(leader_id);

    auto new_leader_id = wait_for_group_leader(gr);
    assert_at_most_one_leader(gr);

    // require leader id has changed
    BOOST_REQUIRE_NE(leader_id, new_leader_id);
    assert_stable_leadership(gr);
};

FIXTURE_TEST(
  test_leader_is_not_elected_when_there_is_no_majority, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);

    gr.enable_all();

    auto leader_id = wait_for_group_leader(gr);
    assert_at_most_one_leader(gr);

    validate_logs_replication(gr);

    // Stop the current leader
    tstlog.info("Stopping current leader {}", leader_id);
    gr.disable_node(leader_id);

    auto new_leader_id = wait_for_group_leader(gr);
    assert_at_most_one_leader(gr);

    // second_leader killed
    tstlog.info("Stopping newly elected leader {}", new_leader_id);
    gr.disable_node(new_leader_id);

    assert_stable_leadership(gr);
};

FIXTURE_TEST(
  test_leader_is_reelected_when_majority_is_back_up, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();

    // Wait for first leader
    auto leader_id = wait_for_group_leader(gr);
    assert_at_most_one_leader(gr);

    validate_logs_replication(gr);

    // Disable current leader and other node
    gr.disable_node(leader_id);

    for (auto& [id, node] : gr.get_members()) {
        if (leader_id != id) {
            gr.disable_node(id);
            break;
        }
    }

    // enable again the old leader
    gr.enable_node(leader_id);
    // wait for next leader to be elected after recovery
    wait_for_group_leader(gr);
    assert_at_most_one_leader(gr);
};
