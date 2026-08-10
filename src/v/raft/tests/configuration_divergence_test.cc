// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "model/record.h"
#include "model/record_batch_reader.h"
#include "raft/consensus.h"
#include "raft/group_configuration.h"
#include "raft/tests/raft_fixture.h"
#include "raft/tests/raft_fixture_retry_policy.h"
#include "raft/types.h"
#include "storage/types.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/coroutine.hh>

#include <algorithm>
#include <chrono>

using namespace raft;

namespace {

/**
 * raft::configuration_manager is only ever told about a configuration from the
 * continuation of consensus::disk_append. When an append fails after a
 * raft_configuration batch already became visible in the log, that continuation
 * never runs: the batch is in the log but the manager never learns about it.
 *
 * These tests demonstrate the consequences. They are all about a single
 * replica's derived state diverging from its own log - the logs themselves stay
 * identical across the group throughout.
 */
struct configuration_divergence_test : raft_fixture {
    /**
     * Fails the append of every raft_configuration batch on the given node,
     * after the batch has become visible in its log. Returns a counter of the
     * failures injected.
     */
    ss::shared_ptr<size_t> drop_configurations_on(model::node_id id) {
        auto dropped = ss::make_shared<size_t>(0);
        node(id).f_injectable_log()->set_post_append_hook(
          [dropped](const model::record_batch& b) -> ss::future<> {
              if (
                b.header().type
                != model::record_batch_type::raft_configuration) {
                  return ss::now();
              }
              (*dropped)++;
              return ss::make_exception_future<>(
                std::runtime_error("injected post-visibility append failure"));
          });
        return dropped;
    }

    void stop_dropping_configurations_on(model::node_id id) {
        node(id).f_injectable_log()->set_post_append_hook(std::nullopt);
    }

    /**
     * Reads the raw log, including the raft_configuration batches that
     * raft_node_instance::read_batches_in_range filters out.
     */
    ss::future<model::record_batch_reader::data_t>
    read_raw_log(model::node_id id) {
        auto raft = node(id).raft();
        auto rdr = co_await raft->make_reader(
          storage::local_log_reader_config(
            raft->start_offset(), model::offset::max()));
        co_return co_await model::consume_reader_to_memory(
          std::move(rdr), default_timeout());
    }

    ss::future<std::vector<model::offset>>
    configuration_offsets_in_log(model::node_id id) {
        std::vector<model::offset> offsets;
        for (const auto& b : co_await read_raw_log(id)) {
            if (
              b.header().type == model::record_batch_type::raft_configuration) {
                offsets.push_back(b.base_offset());
            }
        }
        co_return offsets;
    }

    ss::future<> transfer_leadership_to(model::node_id target) {
        auto start = model::timeout_clock::now();
        while (true) {
            auto current_leader = co_await wait_for_leader(start + 30s);
            if (current_leader == target) {
                co_return;
            }
            auto raft = node(current_leader).raft();
            std::ignore = co_await raft->transfer_leadership(
              {.group = raft->group(), .target = target, .timeout = 10s});
        }
    }

    ss::future<> restart(model::node_id id, std::vector<vnode> initial_nodes) {
        auto dir = node(id).raft()->log()->config().base_directory();
        co_await stop_node(id);
        add_node(id, model::revision_id(0), dir);
        co_await node(id).init_and_start(initial_nodes);
    }

    /**
     * Brings the group to a state where `victim_id` missed a membership change:
     * a fourth node is added to a three node group while every configuration
     * batch append fails on the victim after becoming visible.
     */
    ss::future<> diverge_one_follower() {
        co_await create_simple_group(3);
        leader_id = co_await wait_for_leader(10s);

        auto res = co_await node(leader_id).raft()->replicate(
          make_batches({{"k_0", "v_0"}}),
          replicate_options(consistency_level::quorum_ack));
        ASSERT_TRUE_CORO(res.has_value());

        for (const auto& [id, _] : nodes()) {
            if (id != leader_id) {
                victim_id = id;
                break;
            }
        }
        ASSERT_NE_CORO(victim_id, model::node_id{-1});

        auto dropped = drop_configurations_on(victim_id);

        auto& n3 = add_node(added_node, model::revision_id(0));
        co_await n3.init_and_start({});

        const auto target_voters = all_vnodes().size();
        co_await node(leader_id).raft()->replace_configuration(
          all_vnodes(), model::revision_id(1));

        // the reconfiguration completes on every node but the victim, whose
        // configuration manager never learns about any of the batches
        RPTEST_REQUIRE_EVENTUALLY_CORO(30s, [this, target_voters] {
            const auto& cfg = node(leader_id).raft()->config();
            return cfg.get_state() == configuration_state::simple
                   && cfg.current_config().voters.size() == target_voters;
        });

        // the failed appends leave the batches visible, so the victim's log
        // still catches up with the leader's. Only once it has can the hook be
        // removed without the victim picking a configuration up after the fact.
        RPTEST_REQUIRE_EVENTUALLY_CORO(30s, [this] {
            return node(victim_id).raft()->dirty_offset()
                   == node(leader_id).raft()->dirty_offset();
        });

        // both the joint and the resulting simple configuration were dropped
        ASSERT_GE_CORO(*dropped, 2);
        stop_dropping_configurations_on(victim_id);

        // a failed append never reaches end_of_stream, so the dropped batches
        // are visible but not yet flushed. Appending data behind them makes
        // them durable and readable without telling the victim's configuration
        // manager anything.
        auto tail = co_await node(leader_id).raft()->replicate(
          make_batches({{"k_1", "v_1"}}),
          replicate_options(consistency_level::quorum_ack));
        ASSERT_TRUE_CORO(tail.has_value());
        co_await wait_for_committed_offset(tail.value().last_offset, 30s);
    }

    static constexpr model::node_id added_node{3};

    model::node_id victim_id{-1};
    model::node_id leader_id{-1};
};

} // namespace

/**
 * The core defect: the victim's log holds configuration batches that its own
 * configuration_manager has never seen, so it reports a stale group
 * configuration while its log is byte for byte identical to the leader's.
 */
TEST_F_CORO(
  configuration_divergence_test, configuration_lost_when_append_fails) {
    co_await diverge_one_follower();

    const auto n3 = node(added_node).get_vnode();

    const auto& leader_cfg = node(leader_id).raft()->config();
    ASSERT_EQ_CORO(leader_cfg.current_config().voters.size(), 4);
    ASSERT_TRUE_CORO(leader_cfg.is_voter(n3));

    const auto& victim_cfg = node(victim_id).raft()->config();
    ASSERT_EQ_CORO(victim_cfg.current_config().voters.size(), 3);
    ASSERT_FALSE_CORO(victim_cfg.contains(n3));

    // the divergence is in derived state only - the logs agree
    auto victim_batches = co_await read_raw_log(victim_id);
    auto leader_batches = co_await read_raw_log(leader_id);
    ASSERT_EQ_CORO(victim_batches.size(), leader_batches.size());
    ASSERT_TRUE_CORO(
      std::equal(
        victim_batches.begin(), victim_batches.end(), leader_batches.begin()));

    // and the victim's log contains configurations above the last one its
    // manager knows about, which it will never read again
    auto cfg_offsets = co_await configuration_offsets_in_log(victim_id);
    auto latest_known = node(victim_id).raft()->get_latest_configuration_offset();
    ASSERT_GT_CORO(
      std::ranges::count_if(
        cfg_offsets, [latest_known](auto o) { return o > latest_known; }),
      0);
}

/**
 * Boundary case: nothing persists a highest known offset above the dropped
 * batches, so a plain restart rescans the log from a low offset and heals. The
 * highest known offset - not log retention - is what makes the loss permanent.
 */
TEST_F_CORO(configuration_divergence_test, plain_restart_heals_the_divergence) {
    co_await diverge_one_follower();

    const auto n3 = node(added_node).get_vnode();
    ASSERT_FALSE_CORO(node(victim_id).raft()->config().contains(n3));

    auto vnodes = all_vnodes();
    co_await restart(victim_id, vnodes);

    ASSERT_TRUE_CORO(node(victim_id).raft()->config().contains(n3));
    ASSERT_EQ_CORO(
      node(victim_id).raft()->config().current_config().voters.size(), 4);
}

/**
 * A snapshot raises the persisted highest known offset past the dropped batches
 * and prefix truncates them out of the log. configuration_manager::prefix_
 * truncate migrates the *stale* configuration forward to the truncation point,
 * so after a restart the node has neither the correct configuration nor
 * anything left to reconstruct it from.
 */
TEST_F_CORO(
  configuration_divergence_test, snapshot_makes_the_divergence_permanent) {
    co_await diverge_one_follower();

    const auto n3 = node(added_node).get_vnode();
    auto vnodes = all_vnodes();

    // push the committed offset past the dropped configuration batches
    auto res = co_await node(leader_id).raft()->replicate(
      make_batches({{"k_1", "v_1"}}),
      replicate_options(consistency_level::quorum_ack));
    ASSERT_TRUE_CORO(res.has_value());
    co_await wait_for_committed_offset(res.value().last_offset, 30s);

    auto snapshot_offset = node(victim_id).raft()->committed_offset();
    auto cfg_offsets = co_await configuration_offsets_in_log(victim_id);
    ASSERT_TRUE_CORO(
      std::ranges::all_of(
        cfg_offsets, [snapshot_offset](auto o) { return o <= snapshot_offset; }));

    // quiesce the rest of the group so nothing can push a new configuration to
    // the victim while it snapshots and restarts
    for (auto id : all_ids()) {
        if (id != victim_id) {
            co_await stop_node(id);
        }
    }

    co_await node(victim_id).raft()->write_snapshot(
      write_snapshot_cfg(snapshot_offset, iobuf{}));

    // the configuration batches are gone from the log
    ASSERT_TRUE_CORO(
      (co_await configuration_offsets_in_log(victim_id)).empty());

    co_await restart(victim_id, vnodes);

    // permanently stale: the correct configuration is neither in the manager
    // nor recoverable from the log
    ASSERT_EQ_CORO(
      node(victim_id).raft()->config().current_config().voters.size(), 3);
    ASSERT_FALSE_CORO(node(victim_id).raft()->config().contains(n3));
    ASSERT_TRUE_CORO(
      (co_await configuration_offsets_in_log(victim_id)).empty());
}

/**
 * The amplifier. vote_stm snapshots consensus::config() and, on winning,
 * replicates it as the initial configuration batch of its term. A replica with
 * a stale configuration therefore does not just misjudge quorum locally - it
 * writes the stale membership back into the log as an authoritative
 * configuration change, and every other replica accepts it.
 *
 * It also wins that election counting votes over the stale voter set, i.e. with
 * fewer votes than the real configuration requires.
 */
TEST_F_CORO(
  configuration_divergence_test, stale_replica_regresses_group_as_leader) {
    co_await diverge_one_follower();

    const auto n3 = node(added_node).get_vnode();
    for (auto id : all_ids()) {
        if (id != victim_id) {
            ASSERT_TRUE_CORO(node(id).raft()->config().contains(n3));
        }
    }

    co_await transfer_leadership_to(victim_id);

    // every replica that is still part of the stale configuration accepts it as
    // an authoritative configuration change: the group's membership has gone
    // backwards, revision included
    RPTEST_REQUIRE_EVENTUALLY_CORO(30s, [this, n3] {
        return std::ranges::all_of(all_ids(), [this, n3](model::node_id id) {
            if (id == added_node) {
                return true;
            }
            const auto& cfg = node(id).raft()->config();
            return cfg.get_state() == configuration_state::simple
                   && cfg.current_config().voters.size() == 3
                   && !cfg.contains(n3)
                   && cfg.revision_id() < model::revision_id(1);
        });
    });

    // the removed node is never told. It is absent from the configuration the
    // new leader knows about, so it is dropped from the follower states and
    // nothing is dispatched to it: it goes on believing it is a voter of a four
    // node group, campaigning in a configuration that no longer exists.
    const auto& orphan_cfg = node(added_node).raft()->config();
    ASSERT_EQ_CORO(orphan_cfg.current_config().voters.size(), 4);
    ASSERT_TRUE_CORO(orphan_cfg.is_voter(n3));
    ASSERT_LT_CORO(
      node(added_node).raft()->get_latest_configuration_offset(),
      node(victim_id).raft()->get_latest_configuration_offset());

    // the regression is durable: it is a real configuration batch in the log
    auto cfg_offsets = co_await configuration_offsets_in_log(victim_id);
    auto latest = node(victim_id).raft()->get_latest_configuration_offset();
    ASSERT_TRUE_CORO(
      std::ranges::find(cfg_offsets, latest) != cfg_offsets.end());
}
