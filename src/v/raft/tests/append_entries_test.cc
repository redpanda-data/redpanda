#include "finjector/hbadger.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/timeout_clock.h"
#include "raft/consensus_utils.h"
#include "raft/tests/raft_group_fixture.h"
#include "raft/types.h"

#include <system_error>

FIXTURE_TEST(test_entries_are_replicated_to_all_nodes, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();

    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;
    auto res = leader_raft->replicate(random_batches_entry(1)).get0();

    validate_logs_replication(gr);
};

FIXTURE_TEST(test_replicate_multiple_entries_single_node, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 1);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;
    for (int i = 0; i < 5; ++i) {
        if (leader_raft->is_leader()) {
            auto res = leader_raft->replicate(random_batches_entry(5)).get0();
        }
    }

    validate_logs_replication(gr);

    wait_for(
      10s,
      [this, &gr] { return are_all_commit_indexes_the_same(gr); },
      "State is consistent after replication");
};

FIXTURE_TEST(test_replicate_multiple_entries, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;
    for (int i = 0; i < 5; ++i) {
        if (leader_raft->is_leader()) {
            auto res = leader_raft->replicate(random_batches_entry(5)).get0();
        }
    }

    validate_logs_replication(gr);
    wait_for(
      10s,
      [this, &gr] { return are_all_commit_indexes_the_same(gr); },
      "State is consistent");
};

FIXTURE_TEST(test_single_node_recovery, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    model::node_id disabled_id;
    for (auto& [id, _] : gr.get_members()) {
        // disable one of the non leader nodes
        if (leader_id != id) {
            disabled_id = id;
            gr.disable_node(id);
            break;
        }
    }
    auto leader_raft = gr.get_member(leader_id).consensus;
    // append some entries
    for (int i = 0; i < 5; ++i) {
        if (leader_raft->is_leader()) {
            auto res = leader_raft->replicate(random_batches_entry(5)).get0();
        }
    }
    validate_logs_replication(gr);

    gr.enable_node(disabled_id);

    validate_logs_replication(gr);

    wait_for(
      10s,
      [this, &gr] { return are_all_commit_indexes_the_same(gr); },
      "After recovery state is consistent");

    validate_logs_replication(gr);
};

FIXTURE_TEST(test_single_node_recovery_multi_terms, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    model::node_id disabled_id;
    for (auto& [id, _] : gr.get_members()) {
        // disable one of the non leader nodes
        if (leader_id = gr.get_leader_id().value(); leader_id != id) {
            disabled_id = id;
            gr.disable_node(id);
            break;
        }
    }
    auto leader_raft = gr.get_member(leader_id).consensus;
    // append some entries in current term
    for (int i = 0; i < 5; ++i) {
        if (leader_raft->is_leader()) {
            auto res = leader_raft->replicate(random_batches_entry(5)).get0();
        }
    }

    // roll the term
    leader_raft->step_down().get0();
    leader_id = wait_for_group_leader(gr);
    leader_raft = gr.get_member(leader_id).consensus;
    // append some entries in next term
    for (int i = 0; i < 5; ++i) {
        if (leader_raft->is_leader()) {
            auto res = leader_raft->replicate(random_batches_entry(5)).get0();
        }
    }

    validate_logs_replication(gr);

    gr.enable_node(disabled_id);

    validate_logs_replication(gr);

    wait_for(
      10s,
      [this, &gr] { return are_all_commit_indexes_the_same(gr); },
      "State is conistent after recovery");
};

FIXTURE_TEST(test_recovery_of_crashed_leader_truncation, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto first_leader_id = wait_for_group_leader(gr);
    model::node_id disabled_id;
    std::vector<model::node_id> disabled_nodes{};
    for (auto& [id, _] : gr.get_members()) {
        // disable all nodes except the leader
        if (id != first_leader_id) {
            disabled_nodes.push_back(id);
        }
    }
    for (auto& id : disabled_nodes) {
        gr.disable_node(id);
    }
    // append some entries to leader log
    auto leader_raft = gr.get_member(first_leader_id).consensus;
    auto res = leader_raft->replicate(random_batches_entry(2)).get0();

    // shut down the leader
    gr.disable_node(first_leader_id);

    // enable nodes that were disabled before we appended on leader
    for (auto id : disabled_nodes) {
        gr.enable_node(model::node_id(id));
    }
    // wait for leader to be elected from enabled nodes
    auto leader_id = wait_for_group_leader(gr);
    leader_raft = gr.get_member(leader_id).consensus;

    // append some entries via new leader so old one has some data to
    // truncate
    res = leader_raft->replicate(random_batches_entry(2)).get0();

    validate_logs_replication(gr);

    gr.enable_node(first_leader_id);

    // wait for data to be replicated to old leader node (have to truncate)
    validate_logs_replication(gr);

    wait_for(
      10s,
      [this, &gr] { return are_all_commit_indexes_the_same(gr); },
      "After recovery state should be consistent");
};