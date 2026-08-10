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
#include <seastar/core/sleep.hh>

#include <algorithm>
#include <chrono>

using namespace raft;

namespace {

/**
 * An append can fail after a raft_configuration batch already became visible
 * in the log, e.g. when a later batch of the same append_entries request
 * fails to be written. The configuration manager must still learn about the
 * visible configuration: nothing ever re-delivers it otherwise (a leader
 * does not resend entries that are already present in a follower's log) and
 * the manager's state would silently diverge from the log, breaking the
 * quorum arithmetic that raft's configuration change rules depend on.
 *
 * These tests inject such failures on a single replica (the victim) and
 * verify that its configuration manager stays consistent with its log, in
 * memory, across the partition scenario that would previously lose an
 * acknowledged write, and across a restart.
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
     * Reads the raw log directly from storage, including the
     * raft_configuration batches that
     * raft_node_instance::read_batches_in_range filters out and without
     * capping at the raft visibility offset (which is unset on a freshly
     * restarted node with no leader).
     */
    ss::future<model::record_batch_reader::data_t>
    read_raw_log(model::node_id id) {
        auto raft = node(id).raft();
        auto rdr = co_await node(id).f_injectable_log()->make_reader(
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

    /**
     * Splits the group in two, dropping every message crossing the boundary.
     */
    void partition_group(
      std::vector<model::node_id> a, std::vector<model::node_id> b) {
        auto block_towards = [this](
                               const std::vector<model::node_id>& side,
                               std::vector<model::node_id> unreachable) {
            for (auto id : side) {
                node(id).on_dispatch(
                  [unreachable](model::node_id dest, raft::msg_type) {
                      if (
                        std::ranges::find(unreachable, dest)
                        != unreachable.end()) {
                          throw std::runtime_error("partitioned");
                      }
                      return ss::now();
                  });
            }
        };
        block_towards(a, b);
        block_towards(b, a);
    }

    ss::future<> restart(model::node_id id, std::vector<vnode> initial_nodes) {
        auto dir = node(id).raft()->log()->config().base_directory();
        co_await stop_node(id);
        add_node(id, model::revision_id(0), dir);
        co_await node(id).init_and_start(initial_nodes);
    }

    /**
     * Grows the initial three node group by `extra` nodes. When
     * `drop_on_victim` is set every configuration batch append fails on the
     * victim after the batch became visible in its log.
     */
    ss::future<> grow_group(size_t extra, bool drop_on_victim) {
        co_await create_simple_group(3);
        leader_id = co_await wait_for_leader(10s);

        auto res = co_await node(leader_id).raft()->replicate(
          make_batches({{"k_0", "v_0"}}),
          replicate_options(consistency_level::quorum_ack));
        ASSERT_TRUE_CORO(res.has_value());

        for (const auto& [id, _] : nodes()) {
            original_nodes.push_back(id);
            if (id != leader_id && victim_id == model::node_id{-1}) {
                victim_id = id;
            }
        }
        ASSERT_NE_CORO(victim_id, model::node_id{-1});

        auto dropped = ss::make_shared<size_t>(0);
        if (drop_on_victim) {
            dropped = drop_configurations_on(victim_id);
        }

        for (size_t i = 0; i < extra; ++i) {
            auto& n = add_node(
              model::node_id(int(added_node() + i)), model::revision_id(0));
            co_await n.init_and_start({});
        }

        const auto target_voters = all_vnodes().size();
        co_await node(leader_id).raft()->replace_configuration(
          all_vnodes(), model::revision_id(1));

        RPTEST_REQUIRE_EVENTUALLY_CORO(30s, [this, target_voters] {
            const auto& cfg = node(leader_id).raft()->config();
            return cfg.get_state() == configuration_state::simple
                   && cfg.current_config().voters.size() == target_voters;
        });

        // the failed appends leave the batches visible, so the victim's log
        // still catches up with the leader's. Waiting for that before
        // removing the hook makes the number of injected failures
        // deterministic.
        RPTEST_REQUIRE_EVENTUALLY_CORO(30s, [this] {
            return node(victim_id).raft()->dirty_offset()
                   == node(leader_id).raft()->dirty_offset();
        });

        if (drop_on_victim) {
            // both the joint and the resulting simple configuration appends
            // failed on the victim
            ASSERT_GE_CORO(*dropped, 2);
            stop_dropping_configurations_on(victim_id);
        }

        // a failed append never reaches end_of_stream, so the batches whose
        // append failed are visible but not yet flushed. Appending data
        // behind them makes them durable and readable.
        auto tail = co_await node(leader_id).raft()->replicate(
          make_batches({{"k_1", "v_1"}}),
          replicate_options(consistency_level::quorum_ack));
        ASSERT_TRUE_CORO(tail.has_value());
        co_await wait_for_committed_offset(tail.value().last_offset, 30s);
    }

    // the original group member that is neither the leader nor the victim
    model::node_id third_original() const {
        for (auto id : original_nodes) {
            if (id != leader_id && id != victim_id) {
                return id;
            }
        }
        return model::node_id{-1};
    }

    static constexpr model::node_id added_node{3};

    model::node_id victim_id{-1};
    model::node_id leader_id{-1};
    std::vector<model::node_id> original_nodes;
};

} // namespace

/**
 * The victim's configuration manager tracks its own log even though every
 * configuration batch append on it failed after the batch became visible:
 * the configurations of the visible batches are reconciled into the manager
 * on the failure path, so the victim converges on the five voter
 * configuration like every other replica.
 */
TEST_F_CORO(configuration_divergence_test, configuration_manager_tracks_log) {
    co_await grow_group(2, true);

    RPTEST_REQUIRE_EVENTUALLY_CORO(30s, [this] {
        const auto& cfg = node(victim_id).raft()->config();
        return cfg.get_state() == configuration_state::simple
               && cfg.current_config().voters.size() == 5;
    });

    // the manager's latest configuration is the last configuration batch of
    // the victim's log
    auto log_offsets = co_await configuration_offsets_in_log(victim_id);
    ASSERT_FALSE_CORO(log_offsets.empty());
    ASSERT_EQ_CORO(
      node(victim_id).raft()->get_latest_configuration_offset(),
      log_offsets.back());
}

/**
 * The partition that would previously lose an acknowledged write: the group
 * really has five voters, and a victim that lost the configuration would
 * believe in the original three, elect itself with two votes and acknowledge
 * a write no quorum of the real configuration ever saw. With the
 * configuration manager consistent with the log the victim knows it needs
 * three of five voters, cannot get them, and no write is acknowledged.
 */
TEST_F_CORO(
  configuration_divergence_test, no_split_brain_after_append_failures) {
    co_await grow_group(2, true);

    // the victim learned the five voter configuration despite the failures
    ASSERT_EQ_CORO(
      node(victim_id).raft()->config().current_config().voters.size(), 5);
    ASSERT_EQ_CORO(
      node(leader_id).raft()->config().current_config().voters.size(), 5);

    // the victim plus one other original node is a majority of the original
    // three node configuration and no majority at all of the real one
    const std::vector<model::node_id> stale_side{victim_id, third_original()};
    const std::vector<model::node_id> real_side{
      leader_id, added_node, model::node_id(added_node() + 1)};

    // if the victim were to campaign with a stale configuration, make sure
    // it is the one to win on its side of the partition
    node(third_original()).raft()->block_new_leadership();
    partition_group(stale_side, real_side);

    // give the victim several election timeouts worth of opportunity to
    // elect itself with a stale quorum
    co_await ss::sleep(5s);
    ASSERT_FALSE_CORO(node(victim_id).raft()->is_elected_leader());

    // two of five voters can neither elect nor commit
    auto res = co_await node(victim_id).raft()->replicate(
      make_batches({{"lost", "v"}}),
      replicate_options(consistency_level::quorum_ack, 5s));
    ASSERT_TRUE_CORO(res.has_error());
    ASSERT_FALSE_CORO(node(victim_id).raft()->is_leader());
}

/**
 * Control: the same partition with no failure injection behaves identically,
 * demonstrating that the injected failures no longer make a difference.
 */
TEST_F_CORO(
  configuration_divergence_test, no_write_is_acknowledged_when_intact) {
    co_await grow_group(2, false);

    ASSERT_EQ_CORO(
      node(victim_id).raft()->config().current_config().voters.size(), 5);

    const std::vector<model::node_id> stale_side{victim_id, third_original()};
    const std::vector<model::node_id> real_side{
      leader_id, added_node, model::node_id(added_node() + 1)};

    node(third_original()).raft()->block_new_leadership();
    partition_group(stale_side, real_side);

    // two of five voters can neither elect nor commit
    auto res = co_await node(victim_id).raft()->replicate(
      make_batches({{"lost", "v"}}),
      replicate_options(consistency_level::quorum_ack, 5s));
    ASSERT_TRUE_CORO(res.has_error());
    ASSERT_FALSE_CORO(node(victim_id).raft()->is_leader());
}

/**
 * The reconciliation is durable: the configuration manager only persists a
 * highest known offset covering offsets whose configurations it has
 * processed, so a restart of the victim recovers a configuration consistent
 * with its log even though later successful appends advanced the persisted
 * state past the batches whose appends failed.
 */
TEST_F_CORO(
  configuration_divergence_test, configuration_survives_victim_restart) {
    co_await grow_group(2, true);

    RPTEST_REQUIRE_EVENTUALLY_CORO(30s, [this] {
        const auto& cfg = node(victim_id).raft()->config();
        return cfg.get_state() == configuration_state::simple
               && cfg.current_config().voters.size() == 5;
    });

    co_await restart(victim_id, all_vnodes());

    const auto& cfg = node(victim_id).raft()->config();
    ASSERT_EQ_CORO(cfg.current_config().voters.size(), 5);
    auto log_offsets = co_await configuration_offsets_in_log(victim_id);
    ASSERT_FALSE_CORO(log_offsets.empty());
    ASSERT_EQ_CORO(
      node(victim_id).raft()->get_latest_configuration_offset(),
      log_offsets.back());
}
