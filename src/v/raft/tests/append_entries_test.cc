// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "finjector/hbadger.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/record.h"
#include "model/timestamp.h"
#include "raft/consensus_utils.h"
#include "raft/tests/raft_group_fixture.h"
#include "raft/types.h"
#include "random/generators.h"
#include "storage/record_batch_builder.h"
#include "storage/tests/utils/disk_log_builder.h"
#include "storage/tests/utils/random_batch.h"
#include "test_utils/async.h"

#include <system_error>

FIXTURE_TEST(test_entries_are_replicated_to_all_nodes, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();

    bool success = replicate_random_batches(gr, 10).get0();

    BOOST_REQUIRE(success);
    validate_logs_replication(gr);
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
      [this, &gr] { return are_all_commit_indexes_the_same(gr); },
      "State is consistent after replication");
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
    bool success = replicate_random_batches(gr, 8).get0();
    BOOST_REQUIRE(success);
    validate_logs_replication(gr);

    gr.enable_node(disabled_id);

    validate_logs_replication(gr);

    wait_for(
      10s,
      [this, &gr] { return are_all_commit_indexes_the_same(gr); },
      "After recovery state is consistent");

    validate_logs_replication(gr);
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
            m.log
              ->truncate(storage::truncate_config(
                model::offset(0), ss::default_priority_class()))
              .get();
            gr.disable_node(id);
            break;
        }
    }

    gr.enable_node(disabled_id);

    validate_logs_replication(gr);

    wait_for(
      10s,
      [this, &gr] { return are_all_commit_indexes_the_same(gr); },
      "After recovery state is consistent");
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
            // truncate the node log
            m.log
              ->truncate(storage::truncate_config(
                model::offset(0), ss::default_priority_class()))
              .get();
            gr.disable_node(id);
            break;
        }
    }

    gr.enable_node(disabled_id);

    validate_logs_replication(gr);

    wait_for(
      10s,
      [this, &gr] { return are_all_consumable_offsets_are_the_same(gr); },
      "After recovery state is consistent");
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
          ->step_down(leader.consensus->term() + model::term_id(1))
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
        info("disabling node {}", id);
        gr.disable_node(id);
    }
    // append some entries to leader log
    auto leader_raft = gr.get_member(first_leader_id).consensus;
    auto f = leader_raft->replicate(
      random_batches_reader(2), default_replicate_opts);
    // since replicate doesn't accept timeout client have to deal with it.
    auto v = ss::with_timeout(model::timeout_clock::now() + 1s, std::move(f))
               .handle_exception_type([](const ss::timed_out_error&) {
                   return result<raft::replicate_result>(
                     rpc::errc::client_request_timeout);
               })
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
      [this, &gr] { return are_all_commit_indexes_the_same(gr); },
      "After recovery state should be consistent");
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
      [this, &gr] { return are_all_consumable_offsets_are_the_same(gr); },
      "After recovery state is consistent");
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
      [this, &gr] { return are_all_consumable_offsets_are_the_same(gr); },
      "After recovery state is consistent");

    wait_for(
      1s,
      [this, &gr] {
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
      raft::group_id(0),
      3,
      storage::log_config::storage_type::disk,
      model::cleanup_policy_bitflags::compaction,
      10_MiB);

    auto cfg = storage::log_builder_config();
    cfg.base_dir = fmt::format("{}/{}", gr.get_data_dir(), 0);

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
    auto batch = storage::test::make_random_batch(model::offset(0), 1, false);
    builder.add_batch(std::move(batch)).get0();
    // roll term - this was triggering ch1284 (ghost batches influencing
    // segments roll)
    builder.add_segment(model::offset(1), model::term_id(1)).get0();
    batch = storage::test::make_random_batch(model::offset(1), 5, false);
    batch.set_term(model::term_id(1));
    builder.add_batch(std::move(batch)).get0();
    // gap from 6 to 19
    batch = storage::test::make_random_batch(model::offset(20), 30, false);
    batch.set_term(model::term_id(1));
    builder.add_batch(std::move(batch)).get0();
    // gap from 50 to 67, at term boundary
    builder.add_segment(model::offset(68), model::term_id(2)).get0();
    batch = storage::test::make_random_batch(model::offset(68), 11, false);
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
      [this, &gr] { return are_all_commit_indexes_the_same(gr); },
      "After recovery state is consistent");

    validate_logs_replication(gr);
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
      raft::group_id(0),
      3,
      storage::log_config::storage_type::disk,
      model::cleanup_policy_bitflags::deletion,
      1_KiB);

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
    bool res = replicate_compactible_batches(gr, first_ts).get0();

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
            ss::default_priority_class(),
            as,
            storage::debug_sanitize_files::yes))
          .then([] { return true; });
    }).get0();

    gr.enable_node(disabled_id);

    validate_logs_replication(gr);

    wait_for(
      10s,
      [this, &gr] { return are_all_commit_indexes_the_same(gr); },
      "After recovery state is consistent");

    validate_logs_replication(gr);
};
/// FIXME: enable those tests back when we figure out how to prevent then
/// causing build timeout

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
            get_leader_raft(gr)->committed_offset(),
            iobuf{},
            raft::write_snapshot_cfg::should_prefix_truncate::yes))
          .get0();
    }
    gr.enable_node(disabled_id);
    success = replicate_random_batches(gr, 5).get0();
    BOOST_REQUIRE(success);
    validate_logs_replication(gr);

    wait_for(
      10s,
      [this, &gr] { return are_all_commit_indexes_the_same(gr); },
      "After recovery state is consistent");

    validate_logs_replication(gr);
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
    leader_raft->step_down(leader_raft->term() + model::term_id(1)).get0();

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
            get_leader_raft(gr)->committed_offset(),
            iobuf{},
            raft::write_snapshot_cfg::should_prefix_truncate::yes))
          .get0();
    }
    gr.enable_node(disabled_id);
    success = replicate_random_batches(gr, 5).get0();
    BOOST_REQUIRE(success);
    validate_logs_replication(gr);

    wait_for(
      10s,
      [this, &gr] { return are_all_commit_indexes_the_same(gr); },
      "After recovery state is consistent");

    validate_logs_replication(gr);
};

FIXTURE_TEST(test_last_visible_offset_relaxed_consistency, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    gr.wait_for_leader().get0();
    // disable two nodes
    std::vector<model::node_id> disabled;
    model::node_id leader_id;
    for (auto& m : gr.get_members()) {
        if (!m.second.consensus->is_leader()) {
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
      [this, &gr] { return are_all_consumable_offsets_are_the_same(gr); },
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
        success = replicate_random_batches(
                    gr, 2, raft::consistency_level::leader_ack)
                    .get0();
        BOOST_REQUIRE(success);
    }

    validate_logs_replication(gr);

    wait_for(
      10s,
      [this, &gr] { return are_all_consumable_offsets_are_the_same(gr); },
      "After recovery state is consistent");
};
