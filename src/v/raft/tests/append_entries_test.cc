// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "bytes/bytes.h"
#include "finjector/hbadger.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "model/tests/random_batch.h"
#include "model/timestamp.h"
#include "raft/consensus_utils.h"
#include "raft/tests/raft_group_fixture.h"
#include "raft/types.h"
#include "random/generators.h"
#include "storage/api.h"
#include "storage/kvstore.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "test_utils/async.h"

#include <system_error>

FIXTURE_TEST(test_entries_are_replicated_to_all_nodes, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();

    bool success = replicate_random_batches(gr, 10).get0();

    BOOST_REQUIRE(success);
    validate_logs_replication(gr);
    validate_offset_translation(gr);
};

FIXTURE_TEST(test_replicate_multiple_entries_single_node, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 1);
    gr.enable_all();
    for (int i = 0; i < 5; ++i) {
        bool success = replicate_random_batches(gr, 5).get0();
        BOOST_REQUIRE(success);
    }

    validate_logs_replication(gr);

    wait_for(
      10s,
      [&gr] { return are_all_commit_indexes_the_same(gr); },
      "State is consistent after replication");
};

FIXTURE_TEST(
  test_replicate_multiple_entries_single_node_relaxed_consistency,
  raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 1);
    gr.enable_all();
    for (int i = 0; i < 5; ++i) {
        bool success = replicate_random_batches(
                         gr, 5, raft::consistency_level::leader_ack)
                         .get0();
        BOOST_REQUIRE(success);
    }

    validate_logs_replication(gr);

    wait_for(
      10s,
      [&gr] { return are_all_consumable_offsets_are_the_same(gr); },
      "State is consistent after replication");
    auto& n = gr.get_member(model::node_id(0));
    auto last_visible = n.consensus->last_visible_index();
    auto lstats = n.log->offsets();
    BOOST_REQUIRE_EQUAL(last_visible, lstats.dirty_offset);
};

FIXTURE_TEST(test_replicate_multiple_entries, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;
    for (int i = 0; i < 5; ++i) {
        bool success = replicate_random_batches(gr, 5).get0();
        BOOST_REQUIRE(success);
    }

    validate_logs_replication(gr);
    wait_for(
      10s,
      [&gr] { return are_all_commit_indexes_the_same(gr); },
      "State is consistent");
    validate_offset_translation(gr);
};

FIXTURE_TEST(test_replicate_with_expected_term_leader, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;
    auto term = leader_raft->term();
    bool success = replicate_random_batches(
                     term, gr, 5, raft::consistency_level::leader_ack)
                     .get0();
    // check term to make sure there were no leader elections
    leader_id = wait_for_group_leader(gr);
    leader_raft = gr.get_member(leader_id).consensus;
    auto new_term = leader_raft->term();
    // require call to be successfull only if there was no leader election
    if (new_term == term) {
        BOOST_REQUIRE(success);
    }
};

FIXTURE_TEST(test_replicate_with_expected_term_quorum, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;
    auto term = leader_raft->term();

    bool success = replicate_random_batches(
                     term, gr, 5, raft::consistency_level::quorum_ack)
                     .get0();
    // check term to make sure there were no leader elections
    leader_id = wait_for_group_leader(gr);
    leader_raft = gr.get_member(leader_id).consensus;
    auto new_term = leader_raft->term();
    // require call to be successfull only if there was no leader election
    if (new_term == term) {
        BOOST_REQUIRE(success);
    }
};

FIXTURE_TEST(test_replicate_violating_expected_term_leader, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;
    auto term = leader_raft->term() + model::term_id(100);
    bool success = replicate_random_batches(
                     term, gr, 5, raft::consistency_level::leader_ack)
                     .get0();
    // check term to make sure there were no leader elections
    leader_id = wait_for_group_leader(gr);
    leader_raft = gr.get_member(leader_id).consensus;
    auto new_term = leader_raft->term();
    // require call to be successfull only if there was no leadership change
    if (new_term == term) {
        BOOST_REQUIRE(success);
    }
};

FIXTURE_TEST(test_replicate_violating_expected_term_quorum, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;
    auto term = leader_raft->term() + model::term_id(100);
    bool success = replicate_random_batches(
                     term, gr, 5, raft::consistency_level::quorum_ack)
                     .get0();
    BOOST_REQUIRE(!success);
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
    bool success = replicate_random_batches(gr, 8).get0();
    BOOST_REQUIRE(success);
    validate_logs_replication(gr);

    gr.enable_node(disabled_id);

    validate_logs_replication(gr);

    wait_for(
      10s,
      [&gr] { return are_all_commit_indexes_the_same(gr); },
      "After recovery state is consistent");

    validate_logs_replication(gr);
    validate_offset_translation(gr);
};

FIXTURE_TEST(test_empty_node_recovery, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    bool success = replicate_random_batches(gr, 5).get0();
    BOOST_REQUIRE(success);

    validate_logs_replication(gr);
    model::node_id disabled_id;
    for (auto& [id, m] : gr.get_members()) {
        // disable one of the non leader nodes
        if (gr.get_leader_id() != id) {
            disabled_id = id;
            // truncate the node log
            auto path = m.log->config().work_directory();
            // remove node directory
            gr.disable_node(id);
            std::filesystem::remove_all(std::filesystem::path(path));
            break;
        }
    }

    gr.enable_node(disabled_id);

    validate_logs_replication(gr);

    wait_for(
      10s,
      [&gr] { return are_all_commit_indexes_the_same(gr); },
      "After recovery state is consistent");
    validate_offset_translation(gr);
};

FIXTURE_TEST(test_empty_node_recovery_relaxed_consistency, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    bool success = replicate_random_batches(
                     gr, 5, raft::consistency_level::leader_ack)
                     .get0();
    BOOST_REQUIRE(success);

    validate_logs_replication(gr);
    model::node_id disabled_id;
    for (auto& [id, m] : gr.get_members()) {
        // disable one of the non leader nodes
        if (gr.get_leader_id() != id) {
            disabled_id = id;
            auto path = m.log->config().work_directory();
            // remove node directory
            gr.disable_node(id);
            std::filesystem::remove_all(std::filesystem::path(path));
            break;
        }
    }

    gr.enable_node(disabled_id);

    validate_logs_replication(gr);

    wait_for(
      10s,
      [&gr] { return are_all_consumable_offsets_are_the_same(gr); },
      "After recovery state is consistent");
    validate_offset_translation(gr);
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
    // append some entries in current term
    auto success = replicate_random_batches(gr, 5).get0();
    BOOST_REQUIRE(success);

    // roll the term
    retry_with_leader(gr, 5, 1s, [](raft_node& leader) {
        return leader.consensus
          ->step_down(leader.consensus->term() + model::term_id(1), "test")
          .then([] { return true; });
    }).get0();
    // append some entries in next term
    success = replicate_random_batches(gr, 5).get0();
    BOOST_REQUIRE(success);

    validate_logs_replication(gr);

    gr.enable_node(disabled_id);
    // wait for recovery
    validate_logs_replication(gr);

    wait_for(
      10s,
      [&gr] { return are_all_commit_indexes_the_same(gr); },
      "State is conistent after recovery");
    validate_offset_translation(gr);
};

FIXTURE_TEST(test_recovery_of_crashed_leader_truncation, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto first_leader_id = wait_for_group_leader(gr);
    std::vector<model::node_id> disabled_nodes{};
    for (auto& [id, _] : gr.get_members()) {
        // disable all nodes except the leader
        if (id != first_leader_id) {
            disabled_nodes.push_back(id);
        }
    }
    for (auto& id : disabled_nodes) {
        info("disabling node {}", id);
        gr.disable_node(id);
    }
    // append some entries to leader log
    auto leader_raft = gr.get_member(first_leader_id).consensus;
    auto f = leader_raft->replicate(
      random_batches_reader(2), default_replicate_opts);
    leader_raft.release();
    // since replicate doesn't accept timeout client have to deal with it.
    ss::with_timeout(model::timeout_clock::now() + 1s, std::move(f))
      .handle_exception_type([](const ss::timed_out_error&) {
          return result<raft::replicate_result>(
            rpc::errc::client_request_timeout);
      })
      .discard_result()
      .get0();

    // shut down the leader
    info("shutting down leader {}", first_leader_id);
    gr.disable_node(first_leader_id);

    // enable nodes that were disabled before we appended on leader
    for (auto id : disabled_nodes) {
        info("enabling node {}", id);
        gr.enable_node(model::node_id(id));
    }
    // wait for leader to be elected from enabled nodes

    // append some entries via new leader so old one has some data to
    // truncate
    bool success = replicate_random_batches(gr, 2).get0();
    BOOST_REQUIRE(success);

    validate_logs_replication(gr);

    gr.enable_node(first_leader_id);

    // wait for data to be replicated to old leader node (have to truncate)
    validate_logs_replication(gr);

    wait_for(
      10s,
      [&gr] { return are_all_commit_indexes_the_same(gr); },
      "After recovery state should be consistent");
    validate_offset_translation(gr);
};

FIXTURE_TEST(test_append_entries_with_relaxed_consistency, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();

    bool success = replicate_random_batches(
                     gr, 2, raft::consistency_level::leader_ack)
                     .get0();
    BOOST_REQUIRE(success);

    validate_logs_replication(gr);

    wait_for(
      10s,
      [&gr] { return are_all_consumable_offsets_are_the_same(gr); },
      "After recovery state is consistent");

    validate_offset_translation(gr);
};

FIXTURE_TEST(
  test_append_entries_with_relaxed_consistency_single_node, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 1);
    gr.enable_all();
    bool success = replicate_random_batches(
                     gr, 10, raft::consistency_level::leader_ack)
                     .get0();
    BOOST_REQUIRE(success);
    validate_logs_replication(gr);

    wait_for(
      1s,
      [&gr] { return are_all_consumable_offsets_are_the_same(gr); },
      "After recovery state is consistent");

    wait_for(
      1s,
      [&gr] {
          auto& node = gr.get_members().begin()->second;
          auto lstats = node.log->offsets();
          return node.consensus->last_visible_index() == lstats.dirty_offset;
      },
      "Commit index is advanced ");
};

/**
 *
 * This test tests recovery of log with gaps
 *
 * Example situation:
 *
 * Leader log: [0,10]|--gap--|[21,40]|--gap--|[45,59][60,73]
 *
 *
 * Expected outcome:
 *
 * Follower log has exactly the same set of batches as leader
 *
 */
FIXTURE_TEST(test_compacted_log_recovery, raft_test_fixture) {
    raft_group gr = raft_group(
      raft::group_id(0), 3, model::cleanup_policy_bitflags::compaction, 10_MiB);

    auto cfg = storage::log_builder_config();
    cfg.base_dir = ssx::sformat("{}/{}", gr.get_data_dir(), 0);

    // for now, as compaction isn't yet ready we simulate it with log builder
    auto ntp = node_ntp(raft::group_id(0), model::node_id(0));
    storage::ntp_config::default_overrides overrides;
    overrides.cleanup_policy_bitflags
      = model::cleanup_policy_bitflags::compaction;
    storage::ntp_config ntp_config(
      ntp,
      cfg.base_dir,
      std::make_unique<storage::ntp_config::default_overrides>(
        std::move(overrides)));
    storage::disk_log_builder builder(std::move(cfg));

    builder | storage::start(std::move(ntp_config)) | storage::add_segment(0);
    auto batch = model::test::make_random_batch(model::offset(0), 1, false);
    builder.add_batch(std::move(batch)).get0();
    // roll term - this was triggering ch1284 (ghost batches influencing
    // segments roll)
    builder.add_segment(model::offset(1), model::term_id(1)).get0();
    batch = model::test::make_random_batch(model::offset(1), 5, false);
    batch.set_term(model::term_id(1));
    builder.add_batch(std::move(batch)).get0();
    // gap from 6 to 19
    batch = model::test::make_random_batch(model::offset(20), 30, false);
    batch.set_term(model::term_id(1));
    builder.add_batch(std::move(batch)).get0();
    // gap from 50 to 67, at term boundary
    builder.add_segment(model::offset(68), model::term_id(2)).get0();
    batch = model::test::make_random_batch(model::offset(68), 11, false);
    batch.set_term(model::term_id(2));
    builder.add_batch(std::move(batch)).get0();
    builder.get_log().flush().get0();
    builder.stop().get0();

    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    model::node_id disabled_id;
    auto leader_raft = gr.get_member(leader_id).consensus;
    ss::abort_source as;

    // disable one of the non leader nodes
    for (auto& [id, _] : gr.get_members()) {
        if (leader_id != id) {
            disabled_id = id;
            gr.disable_node(id);
            break;
        }
    }
    validate_logs_replication(gr);

    gr.enable_node(disabled_id);

    validate_logs_replication(gr);

    wait_for(
      3s,
      [&gr] { return are_all_commit_indexes_the_same(gr); },
      "After recovery state is consistent");

    validate_logs_replication(gr);
    validate_offset_translation(gr);
};

/**
 *
 * This test is testing a case where there is a gap between start of leader
 log
 * and end of the follower log.
 *
 * Example situation:
 *
 * Leader log: |-------------gap------------|[53,60][61,70][71,100]...
 *
 * Follower log: [0,10][11,20][21,40]
 *
 * Expected outcome:
 *
 * Follower log gets prefix truncated and recovered with leader log
 *
 */
FIXTURE_TEST(test_collected_log_recovery, raft_test_fixture) {
    raft_group gr = raft_group(
      raft::group_id(0), 3, model::cleanup_policy_bitflags::deletion, 1_KiB);

    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    model::node_id disabled_id;
    ss::abort_source as;

    // disable one of the non leader nodes
    for (auto& [id, _] : gr.get_members()) {
        if (leader_id != id) {
            disabled_id = id;
            gr.disable_node(id);
            break;
        }
    }
    auto first_ts = model::timestamp::now();
    // append some entries
    [[maybe_unused]] bool res
      = replicate_compactible_batches(gr, first_ts).get0();

    auto second_ts = model::timestamp(first_ts() + 100);
    info("Triggerring log collection with timestamp {}", first_ts);
    // append some more entries
    res = replicate_compactible_batches(gr, second_ts).get0();

    validate_logs_replication(gr);

    // compact log at the leader
    info("Compacting log of node: {}", leader_id);
    retry_with_leader(gr, 5, 2s, [first_ts, &as](raft_node& n) {
        return n.log
          ->compact(storage::compaction_config(
            first_ts,
            100_MiB,
            model::offset::max(),
            ss::default_priority_class(),
            as,
            storage::debug_sanitize_files::yes))
          .then([] { return true; });
    }).get0();

    gr.enable_node(disabled_id);

    validate_logs_replication(gr);

    wait_for(
      10s,
      [&gr] { return are_all_commit_indexes_the_same(gr); },
      "After recovery state is consistent");

    validate_logs_replication(gr);
    validate_offset_translation(gr);
};

FIXTURE_TEST(test_snapshot_recovery, raft_test_fixture) {
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
    bool success = replicate_random_batches(gr, 5).get0();
    BOOST_REQUIRE(success);
    validate_logs_replication(gr);

    tests::cooperative_spin_wait_with_timeout(2s, [&gr] {
        auto offset
          = gr.get_members().begin()->second.consensus->committed_offset();
        if (offset <= model::offset(0)) {
            return false;
        }
        return are_all_commit_indexes_the_same(gr);
    }).get0();

    // store snapshot
    for (auto& [_, member] : gr.get_members()) {
        member.consensus
          ->write_snapshot(raft::write_snapshot_cfg(
            get_leader_raft(gr)->committed_offset(), iobuf{}))
          .get0();
        BOOST_REQUIRE_EQUAL(
          member.consensus->get_snapshot_size(),
          get_snapshot_size_from_disk(member));
    }
    gr.enable_node(disabled_id);
    success = replicate_random_batches(gr, 5).get0();
    BOOST_REQUIRE(success);
    validate_logs_replication(gr);

    wait_for(
      10s,
      [&gr] { return are_all_commit_indexes_the_same(gr); },
      "After recovery state is consistent");

    validate_logs_replication(gr);
    validate_offset_translation(gr);
    for (auto& [_, member] : gr.get_members()) {
        BOOST_REQUIRE_EQUAL(
          member.consensus->get_snapshot_size(),
          get_snapshot_size_from_disk(member));
    }
};

FIXTURE_TEST(test_snapshot_recovery_last_config, raft_test_fixture) {
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
    // append some entries
    bool success = replicate_random_batches(gr, 5).get0();
    BOOST_REQUIRE(success);
    // step down so last entry in snapshot will be a configuration
    auto leader_raft = get_leader_raft(gr);
    leader_raft->step_down(leader_raft->term() + model::term_id(1), "test")
      .get0();

    validate_logs_replication(gr);
    tests::cooperative_spin_wait_with_timeout(2s, [&gr] {
        auto offset
          = gr.get_members().begin()->second.consensus->committed_offset();
        if (offset <= model::offset(0)) {
            return false;
        }
        return are_all_commit_indexes_the_same(gr);
    }).get0();

    // store snapshot
    for (auto& [_, member] : gr.get_members()) {
        member.consensus
          ->write_snapshot(raft::write_snapshot_cfg(
            get_leader_raft(gr)->committed_offset(), iobuf{}))
          .get0();
        BOOST_REQUIRE_EQUAL(
          member.consensus->get_snapshot_size(),
          get_snapshot_size_from_disk(member));
    }
    gr.enable_node(disabled_id);
    success = replicate_random_batches(gr, 5).get0();
    BOOST_REQUIRE(success);
    validate_logs_replication(gr);

    wait_for(
      10s,
      [&gr] { return are_all_commit_indexes_the_same(gr); },
      "After recovery state is consistent");

    validate_logs_replication(gr);
    validate_offset_translation(gr);
};

FIXTURE_TEST(test_last_visible_offset_relaxed_consistency, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    gr.wait_for_leader().get0();
    // disable two nodes
    std::vector<model::node_id> disabled;
    model::node_id leader_id;
    for (auto& m : gr.get_members()) {
        if (!m.second.consensus->is_elected_leader()) {
            disabled.push_back(m.first);
        } else {
            leader_id = m.first;
        }
    }
    for (auto id : disabled) {
        gr.disable_node(id);
    }
    auto leader_raft = gr.get_member(leader_id).consensus;
    auto last_visible = leader_raft->last_visible_index();

    // replicate some batches with relaxed consistency
    replicate_random_batches(gr, 20, raft::consistency_level::leader_ack)
      .get0();

    // check last visible offset, make sure it is
    auto new_last_visible = leader_raft->last_visible_index();
    // last visible index couldn't change as there is no majority
    BOOST_REQUIRE_EQUAL(last_visible, new_last_visible);
    // enable nodes
    for (auto id : disabled) {
        gr.enable_node(id);
    }

    validate_logs_replication(gr);

    wait_for(
      10s,
      [&gr] { return are_all_consumable_offsets_are_the_same(gr); },
      "After recovery state is consistent");
    auto updated_last_visible = leader_raft->last_visible_index();
    BOOST_REQUIRE_GT(updated_last_visible, last_visible);
};

FIXTURE_TEST(test_mixed_consisteny_levels, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    bool success = false;
    for (int i = 0; i < 10; ++i) {
        auto lvl = random_generators::get_int(0, 10) > 5
                     ? raft::consistency_level::leader_ack
                     : raft::consistency_level::quorum_ack;
        success = replicate_random_batches(gr, 2, lvl).get0();
        BOOST_REQUIRE(success);
    }

    validate_logs_replication(gr);

    wait_for(
      10s,
      [&gr] { return are_all_consumable_offsets_are_the_same(gr); },
      "After recovery state is consistent");
    validate_offset_translation(gr);
};

FIXTURE_TEST(test_linarizable_barrier, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;

    bool success = replicate_random_batches(gr, 5).get0();
    BOOST_REQUIRE(success);

    leader_id = wait_for_group_leader(gr);
    leader_raft = gr.get_member(leader_id).consensus;
    auto r = leader_raft->linearizable_barrier().get();

    std::vector<size_t> sizes;
    if (r) {
        auto logs = gr.read_all_logs();
        for (auto& l : logs) {
            sizes.push_back(l.second.size());
        }
        std::sort(sizes.begin(), sizes.end());
        // at least 2 out of 3 nodes MUST have all entries replicated
        BOOST_REQUIRE_GT(sizes[2], 1);
        BOOST_REQUIRE_EQUAL(sizes[2], sizes[1]);
    }
};

FIXTURE_TEST(test_linarizable_barrier_single_node, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 1);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;

    bool success = replicate_random_batches(gr, 5).get0();
    BOOST_REQUIRE(success);

    leader_id = wait_for_group_leader(gr);
    leader_raft = gr.get_member(leader_id).consensus;
    auto r = leader_raft->linearizable_barrier().get();

    if (r) {
        auto logs = gr.read_all_logs();
        BOOST_REQUIRE(are_logs_the_same_length(logs));
    }
};

FIXTURE_TEST(test_big_batches_replication, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 1);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;

    bool success
      = retry_with_leader(gr, 5, 2s, [](raft_node& leader_node) mutable {
            storage::record_batch_builder builder(
              model::record_batch_type::raft_data, model::offset(0));

            auto value = bytes_to_iobuf(random_generators::get_bytes(3_MiB));
            builder.add_raw_kv({}, std::move(value));

            auto rdr = model::make_memory_record_batch_reader(
              {std::move(builder).build()});
            return leader_node.consensus
              ->replicate(
                std::move(rdr),
                raft::replicate_options(raft::consistency_level::quorum_ack))
              .then([](result<raft::replicate_result> res) {
                  if (!res) {
                      return false;
                  }
                  return true;
              });
        }).get();
    BOOST_REQUIRE(success);

    auto logs = gr.read_all_logs();
    BOOST_REQUIRE(are_logs_the_same_length(logs));
    validate_offset_translation(gr);
}
struct request_ordering_test_fixture : public raft_test_fixture {
    model::record_batch_reader make_indexed_batch_reader(int32_t idx) {
        storage::record_batch_builder builder(
          model::record_batch_type::raft_data, model::offset(0));

        iobuf buf;
        reflection::serialize(buf, idx);
        builder.add_raw_kv({}, std::move(buf));

        return model::make_memory_record_batch_reader(
          {std::move(builder).build()});
    }

    ss::future<> replicate_indexed_batches(
      consensus_ptr c, int count, raft::consistency_level consistency) {
        std::vector<ss::future<result<raft::replicate_result>>> results;
        for (int32_t i = 0; i < 20; ++i) {
            auto r = c->replicate_in_stages(
              make_indexed_batch_reader(i),
              raft::replicate_options(consistency));
            // wait for request to be enqueued before dispatching next one
            // (comenting this out should cause this test to fail)
            r.request_enqueued.get0();
            results.push_back(std::move(r.replicate_finished));
        }
        return ss::when_all_succeed(results.begin(), results.end())
          .discard_result();
    }

    void validate_batch_ordering(int count, raft_group& gr) {
        for (auto& [id, batches] : gr.read_all_logs()) {
            int32_t idx = 0;
            BOOST_REQUIRE_GE(batches.size(), count);
            for (auto& b : batches) {
                if (b.header().type == model::record_batch_type::raft_data) {
                    BOOST_REQUIRE_EQUAL(
                      idx,
                      reflection::from_iobuf<int32_t>(
                        b.copy_records()[0].release_value()));
                    ++idx;
                }
            }
        }
    }
};

FIXTURE_TEST(test_quorum_ack_write_ordering, request_ordering_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 1);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;
    replicate_indexed_batches(
      leader_raft, 20, raft::consistency_level::quorum_ack)
      .get0();
    validate_logs_replication(gr);

    validate_batch_ordering(20, gr);
}

FIXTURE_TEST(test_leader_ack_write_ordering, request_ordering_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 1);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;
    replicate_indexed_batches(
      leader_raft, 20, raft::consistency_level::quorum_ack)
      .get0();
    validate_logs_replication(gr);

    validate_batch_ordering(20, gr);
}
