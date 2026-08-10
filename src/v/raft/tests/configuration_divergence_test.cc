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
#include "serde/rw/rw.h"
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

    ss::future<bool>
    log_contains_key(model::node_id id, const ss::sstring& key) {
        const auto expected = serde::to_iobuf(key);
        for (auto& b : co_await read_raw_log(id)) {
            if (b.header().type != model::record_batch_type::raft_data) {
                continue;
            }
            for (auto& r : b.copy_records()) {
                if (r.key() == expected) {
                    co_return true;
                }
            }
        }
        co_return false;
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
     * victim after becoming visible, so it never learns the new configuration.
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

        if (drop_on_victim) {
            // both the joint and the resulting simple configuration were
            // dropped
            ASSERT_GE_CORO(*dropped, 2);
            stop_dropping_configurations_on(victim_id);
        }

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

    ss::future<> diverge_one_follower() { return grow_group(1, true); }

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
 *
 * The group really has five voters, so a commit needs three of them. The victim
 * lost that configuration and still believes in the original three, so it needs
 * only two. Partitioning the group so that the victim has a majority of the
 * configuration it believes in, but not of the real one, lets it acknowledge a
 * write to a client that no quorum of the real configuration ever saw. When the
 * victim's side goes away that write is gone, while the surviving three nodes
 * are a legitimate majority of the real configuration and carry on serving.
 *
 * Both sides can commit at the same time: the two acknowledging sets are
 * disjoint, which is exactly what raft's configuration change rules exist to
 * make impossible.
 */
TEST_F_CORO(configuration_divergence_test, acknowledged_write_is_lost) {
    co_await grow_group(2, true);

    // the victim still believes in the original three voters
    ASSERT_EQ_CORO(
      node(victim_id).raft()->config().current_config().voters.size(), 3);
    ASSERT_EQ_CORO(
      node(leader_id).raft()->config().current_config().voters.size(), 5);

    // the victim plus one other original node is a majority of the stale
    // configuration and no majority at all of the real one
    const std::vector<model::node_id> stale_side{victim_id, third_original()};
    const std::vector<model::node_id> real_side{
      leader_id, added_node, model::node_id(added_node() + 1)};

    // make sure the victim, not its ally, wins on the stale side
    node(third_original()).raft()->block_new_leadership();
    partition_group(stale_side, real_side);

    RPTEST_REQUIRE_EVENTUALLY_CORO(
      30s, [this] { return node(victim_id).raft()->is_elected_leader(); });

    // two of three acknowledge, so the write is committed and acknowledged to
    // the caller with quorum_ack
    auto lost = co_await node(victim_id).raft()->replicate(
      make_batches({{"lost", "v"}}),
      replicate_options(consistency_level::quorum_ack, 10s));
    ASSERT_TRUE_CORO(lost.has_value());
    RPTEST_REQUIRE_EVENTUALLY_CORO(30s, [this, lost] {
        return node(victim_id).raft()->committed_offset()
               >= lost.value().last_offset;
    });

    // the real side, holding a majority of the real configuration, never saw it
    // and keeps committing on its own
    for (auto id : real_side) {
        ASSERT_FALSE_CORO(co_await log_contains_key(id, "lost"));
    }

    // the victim's side goes away, as the node that acknowledged the write
    // would on any ordinary failure
    for (auto id : stale_side) {
        co_await stop_node(id);
    }
    for (auto id : real_side) {
        node(id).reset_dispatch_handlers();
    }

    // three of five voters are alive: a legitimate majority of the real
    // configuration, which elects and commits normally
    co_await wait_for_leader(30s);
    auto after = co_await retry_with_leader(
      model::timeout_clock::now() + 30s, [this](raft_node_instance& leader) {
          return leader.raft()->replicate(
            make_batches({{"after", "v"}}),
            replicate_options(consistency_level::quorum_ack, 10s));
      });
    ASSERT_TRUE_CORO(after.has_value());

    // the acknowledged write is on no surviving replica
    for (auto id : real_side) {
        ASSERT_FALSE_CORO(co_await log_contains_key(id, "lost"));
    }
}

/**
 * Control: without the lost configuration the very same partition acknowledges
 * nothing. The victim knows it needs three of five voters, cannot get them, and
 * fails the write instead of losing it.
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
