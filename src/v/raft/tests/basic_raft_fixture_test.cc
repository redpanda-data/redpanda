// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "raft/tests/raft_fixture.h"
#include "raft/tests/raft_group_fixture.h"
#include "raft/types.h"
#include "serde/serde.h"
#include "storage/record_batch_builder.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

using namespace raft;

/**
 * Some basic Raft tests validating if Raft test fixture is working correctly
 */

TEST_F_CORO(raft_fixture, test_single_node_can_elect_leader) {
    auto& n0 = add_node(model::node_id(0), model::revision_id(0));
    co_await n0.start({n0.get_vnode()});
    auto leader = co_await wait_for_leader(10s);

    ASSERT_EQ_CORO(leader, model::node_id(0));
}

TEST_F_CORO(raft_fixture, test_multi_nodes_cluster_can_elect_leader) {
    co_await create_simple_group(5);

    auto leader = co_await wait_for_leader(10s);

    ASSERT_TRUE_CORO(all_ids().contains(leader));

    co_await tests::cooperative_spin_wait_with_timeout(10s, [this, leader] {
        for (const auto& [_, n] : nodes()) {
            if (leader != n->raft()->get_leader_id()) {
                return false;
            }
        }
        return true;
    });
}

TEST_F_CORO(raft_fixture, validate_replication) {
    co_await create_simple_group(5);

    auto leader = co_await wait_for_leader(10s);
    auto& leader_node = node(leader);

    auto result = co_await leader_node.raft()->replicate(
      make_batches({{"k_1", "v_1"}, {"k_2", "v_2"}, {"k_3", "v_3"}}),
      replicate_options(consistency_level::quorum_ack));
    ASSERT_TRUE_CORO(result.has_value());
    auto committed_offset = leader_node.raft()->committed_offset();

    // wait for committed offset to propagate
    co_await wait_for_committed_offset(committed_offset, 5s);
    auto all_batches = co_await leader_node.read_all_data_batches();

    ASSERT_EQ_CORO(all_batches.size(), 3);

    co_await assert_logs_equal();
}

TEST_F_CORO(raft_fixture, validate_recovery) {
    co_await create_simple_group(5);
    auto leader = co_await wait_for_leader(10s);

    // stop one of the nodes
    co_await stop_node(model::node_id(3));

    leader = co_await wait_for_leader(10s);
    auto& leader_node = node(leader);

    // replicate batches
    auto result = co_await leader_node.raft()->replicate(
      make_batches({{"k_1", "v_1"}, {"k_2", "v_2"}, {"k_3", "v_3"}}),
      replicate_options(consistency_level::quorum_ack));
    ASSERT_TRUE_CORO(result.has_value());

    auto& new_n3 = add_node(model::node_id(3), model::revision_id(0));
    co_await new_n3.start(all_vnodes());

    // wait for committed offset to propagate
    auto committed_offset = leader_node.raft()->committed_offset();
    co_await wait_for_committed_offset(committed_offset, 5s);

    auto all_batches = co_await leader_node.read_all_data_batches();

    ASSERT_EQ_CORO(all_batches.size(), 3);

    co_await assert_logs_equal();
}

TEST_F_CORO(raft_fixture, validate_adding_nodes_to_cluster) {
    co_await create_simple_group(1);
    // wait for leader
    auto leader = co_await wait_for_leader(10s);
    ASSERT_EQ_CORO(leader, model::node_id(0));
    auto& leader_node = node(leader);

    // replicate batches
    auto result = co_await leader_node.raft()->replicate(
      make_batches({{"k_1", "v_1"}, {"k_2", "v_2"}, {"k_3", "v_3"}}),
      replicate_options(consistency_level::quorum_ack));
    ASSERT_TRUE_CORO(result.has_value());

    auto& n1 = add_node(model::node_id(1), model::revision_id(0));
    auto& n2 = add_node(model::node_id(2), model::revision_id(0));
    // start other two nodes with empty configuration
    co_await n1.start({});
    co_await n2.start({});

    // update group configuration
    co_await leader_node.raft()->replace_configuration(
      all_vnodes(), model::revision_id(0));

    // wait for committed offset to propagate
    auto committed_offset = leader_node.raft()->committed_offset();

    // wait for committed offset to propagate
    co_await wait_for_committed_offset(committed_offset, 10s);

    auto all_batches = co_await leader_node.read_all_data_batches();

    ASSERT_EQ_CORO(all_batches.size(), 3);

    co_await assert_logs_equal();
}
