// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "model/record.h"
#include "raft/tests/raft_fixture.h"
#include "raft/tests/raft_fixture_retry_policy.h"
#include "raft/types.h"
#include "random/generators.h"
#include "storage/record_batch_builder.h"
#include "test_utils/async.h"
#include "test_utils/test.h"

#include <seastar/core/circular_buffer.hh>
#include <seastar/util/defer.hh>

#include <algorithm>
#include <chrono>
#include <ranges>

using namespace raft;

/**
 * Some basic Raft tests validating if Raft test fixture is working correctly
 */

TEST_F_CORO(raft_fixture, test_single_node_can_elect_leader) {
    auto& n0 = add_node(model::node_id(0), model::revision_id(0));
    co_await n0.init_and_start({n0.get_vnode()});
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

// Empty writes should crash rather than passing silently with incorrect
// results.
TEST_F(raft_fixture, test_empty_writes) {
    create_simple_group(5).get();
    auto leader = wait_for_leader(10s).get();

    auto replicate = [&](chunked_vector<model::record_batch> batches) {
        return node(leader).raft()->replicate(
          std::move(batches), replicate_options{consistency_level::quorum_ack});
    };

    // no records
    storage::record_batch_builder builder(
      model::record_batch_type::raft_data, model::offset(0));

    // Catch the error when appending.
    auto res = replicate(
                 chunked_vector<model::record_batch>::single(
                   std::move(builder).build()))
                 .get();
    ASSERT_TRUE(res.has_error());
    ASSERT_EQ(res.error(), errc::leader_append_failed);

    // In this case there are no batches at all so we don't go to storage, and
    // catch the error in Raft.
    res = replicate(make_batches({})).get();
    ASSERT_TRUE(res.has_error());
    ASSERT_EQ(res.error(), errc::invalid_input_records);
}

TEST_F_CORO(raft_fixture, test_stuck_append_entries) {
    co_await create_simple_group(3);
    auto leader = co_await wait_for_leader(10s);

    for (auto& [_, node] : nodes()) {
        node->on_dispatch([](model::node_id, raft::msg_type t) {
            if (t == raft::msg_type::append_entries) {
                return ss::sleep(2s);
            }
            return ss::now();
        });
    }
    // hold up heartbeats
    auto& leader_node = node(leader);
    auto raft = leader_node.raft();
    auto term_before = raft->term();
    auto result = co_await raft->replicate(
      make_batches({{"k", "v"}}),
      replicate_options(consistency_level::quorum_ack));
    ASSERT_TRUE_CORO(!result.has_error());
    ASSERT_EQ_CORO(term_before, raft->term());
}

// After ensure_disconnect replaces the cached transport for a peer,
// reset_heartbeat_failures must zero heartbeats_failed for every follower of
// this group whose node_id matches; failure history on the prior transport
// must not carry over to the fresh one. See should_reconnect_follower.
TEST_F_CORO(raft_fixture, test_reset_heartbeat_failures) {
    co_await create_simple_group(3);
    auto leader_id = co_await wait_for_leader(10s);
    auto leader = node(leader_id).raft();

    auto& fstates = leader->get_follower_states();
    ASSERT_GE_CORO(fstates.size(), 2u);
    auto it = fstates.begin();
    auto follower_a = it->first;
    ++it;
    auto follower_b = it->first;

    constexpr size_t kFailuresA = 5;
    constexpr size_t kFailuresB = 3;
    for (size_t i = 0; i < kFailuresA; ++i) {
        leader->update_heartbeat_status(follower_a, false);
    }
    for (size_t i = 0; i < kFailuresB; ++i) {
        leader->update_heartbeat_status(follower_b, false);
    }
    ASSERT_EQ_CORO(
      leader->get_follower_states().get(follower_a).heartbeats_failed,
      kFailuresA);
    ASSERT_EQ_CORO(
      leader->get_follower_states().get(follower_b).heartbeats_failed,
      kFailuresB);

    // Reset only follower_a's node. follower_b must be untouched.
    leader->reset_heartbeat_failures(follower_a.id());
    ASSERT_EQ_CORO(
      leader->get_follower_states().get(follower_a).heartbeats_failed, 0u);
    ASSERT_EQ_CORO(
      leader->get_follower_states().get(follower_b).heartbeats_failed,
      kFailuresB);

    // Reset_heartbeat_failures on an unknown node_id is a no-op.
    leader->reset_heartbeat_failures(model::node_id{999});
    ASSERT_EQ_CORO(
      leader->get_follower_states().get(follower_b).heartbeats_failed,
      kFailuresB);
}

struct test_parameters {
    consistency_level c_lvl;
    bool write_caching;

    friend std::ostream&
    operator<<(std::ostream& os, const test_parameters& tp) {
        return os << "{consistency level: " << tp.c_lvl
                  << " write_caching: " << tp.write_caching << "}";
    }
};

class all_acks_fixture
  : public raft_fixture
  , public ::testing::WithParamInterface<test_parameters> {};

class relaxed_acks_fixture
  : public raft_fixture
  , public ::testing::WithParamInterface<test_parameters> {};

class quorum_acks_fixture
  : public raft_fixture
  , public ::testing::WithParamInterface<test_parameters> {};

TEST_P_CORO(all_acks_fixture, validate_replication) {
    co_await create_simple_group(5);

    auto params = GetParam();
    co_await set_write_caching(params.write_caching);

    auto leader = co_await wait_for_leader(10s);
    auto& leader_node = node(leader);

    auto result = co_await leader_node.raft()->replicate(
      make_batches({{"k_1", "v_1"}, {"k_2", "v_2"}, {"k_3", "v_3"}}),
      replicate_options(params.c_lvl));
    ASSERT_TRUE_CORO(result.has_value());

    // wait for committed offset to propagate
    co_await wait_for_committed_offset(result.value().last_offset, 5s);
    auto all_batches = co_await leader_node.read_all_data_batches();

    ASSERT_EQ_CORO(all_batches.size(), 3);

    ASSERT_TRUE_CORO(co_await assert_logs_equal());
}

TEST_P_CORO(all_acks_fixture, single_node_replication) {
    co_await create_simple_group(1);

    auto params = GetParam();
    co_await set_write_caching(params.write_caching);

    auto leader = co_await wait_for_leader(10s);
    auto& leader_node = node(leader);

    auto result = co_await leader_node.raft()->replicate(
      make_batches({{"k_1", "v_1"}}), replicate_options(params.c_lvl));
    ASSERT_TRUE_CORO(result.has_value());

    // wait for committed offset to propagate
    co_await wait_for_committed_offset(result.value().last_offset, 5s);
    ASSERT_TRUE_CORO(co_await assert_logs_equal());
}

TEST_P_CORO(all_acks_fixture, validate_recovery) {
    co_await create_simple_group(5);
    auto leader = co_await wait_for_leader(10s);

    auto params = GetParam();
    co_await set_write_caching(params.write_caching);

    // stop one of the nodes
    co_await stop_node(model::node_id(3));

    leader = co_await wait_for_leader(10s);
    auto& leader_node = node(leader);

    // replicate batches
    auto result = co_await leader_node.raft()->replicate(
      make_batches({{"k_1", "v_1"}, {"k_2", "v_2"}, {"k_3", "v_3"}}),
      replicate_options(params.c_lvl));
    ASSERT_TRUE_CORO(result.has_value());

    auto& new_n3 = add_node(model::node_id(3), model::revision_id(0));
    co_await new_n3.init_and_start(all_vnodes());

    co_await wait_for_committed_offset(result.value().last_offset, 5s);

    auto all_batches = co_await leader_node.read_all_data_batches();

    ASSERT_EQ_CORO(all_batches.size(), 3);

    ASSERT_TRUE_CORO(co_await assert_logs_equal());
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
    co_await n1.init_and_start({});
    co_await n2.init_and_start({});

    // update group configuration
    co_await leader_node.raft()->replace_configuration(
      all_vnodes(), model::revision_id(0));

    // wait for committed offset to propagate
    auto committed_offset = leader_node.raft()->committed_offset();

    // wait for committed offset to propagate
    co_await wait_for_committed_offset(committed_offset, 10s);

    auto all_batches = co_await leader_node.read_all_data_batches();

    ASSERT_EQ_CORO(all_batches.size(), 3);

    ASSERT_TRUE_CORO(co_await assert_logs_equal());
}

TEST_P_CORO(
  relaxed_acks_fixture, validate_committed_offset_advancement_after_log_flush) {
    co_await create_simple_group(3);

    auto params = GetParam();
    co_await set_write_caching(params.write_caching);

    // wait for leader
    auto leader = co_await wait_for_leader(10s);
    auto& leader_node = node(leader);

    co_await disable_background_flushing();

    // replicate batches with acks=1 and validate that committed offset did not
    // advance
    auto committed_offset_before = leader_node.raft()->committed_offset();
    auto result = co_await leader_node.raft()->replicate(
      make_batches(10, 10, 128), replicate_options(params.c_lvl));

    ASSERT_TRUE_CORO(result.has_value());
    // wait for batches to be replicated on all of the nodes
    co_await tests::cooperative_spin_wait_with_timeout(
      10s, [this, expected = result.value().last_offset] {
          return std::all_of(
            nodes().begin(), nodes().end(), [expected](const auto& p) {
                return p.second->raft()->last_visible_index() == expected;
            });
      });
    ASSERT_EQ_CORO(
      committed_offset_before, leader_node.raft()->committed_offset());

    ASSERT_TRUE_CORO(co_await assert_logs_equal());

    vlog(logger().info, "Reset-ing background flushing..");

    co_await reset_background_flushing();

    co_await wait_for_committed_offset(result.value().last_offset, 10s);
}

TEST_P_CORO(
  relaxed_acks_fixture, test_last_visible_offset_monitor_relaxed_consistency) {
    // This tests a property of the visible offset monitor that the fetch path
    // relies on to work correctly. Even with relaxed consistency.

    co_await create_simple_group(3);
    auto params = GetParam();
    co_await set_write_caching(params.write_caching);
    // wait for leader
    auto leader = co_await wait_for_leader(10s);
    auto& leader_node = node(leader);
    auto leader_raft = leader_node.raft();

    auto waiter = leader_raft->visible_offset_monitor().wait(
      model::offset{50}, model::timeout_clock::now() + 10s, {});

    // replicate some batches with relaxed consistency
    auto result = co_await leader_node.raft()->replicate(
      make_batches(10, 10, 128), replicate_options(params.c_lvl));

    ASSERT_TRUE_CORO(result.has_value());

    vlog(logger().info, "waiting for offset: {}", result.value().last_offset);

    co_await std::move(waiter);
};

/**
 * This tests validates if visible offset moves backward. The invariant that the
 * last visible offset does not move backward should be guaranteed by Raft even
 * if using relaxed consistency level.
 *
 * This is possible as the protocol waits for the majority of nodes to
 * acknowledge receiving the message before making it visible.
 */
TEST_P_CORO(
  relaxed_acks_fixture,
  validate_relaxed_consistency_visible_offset_advancement) {
    co_await create_simple_group(3);
    // wait for leader
    co_await wait_for_leader(10s);

    auto params = GetParam();
    co_await set_write_caching(params.write_caching);

    for (auto& [_, node] : nodes()) {
        node->on_dispatch([](model::node_id, raft::msg_type t) {
            if (
              t == raft::msg_type::append_entries
              && random_generators::get_int(1000) > 800) {
                return ss::sleep(1s);
            }

            return ss::now();
        });
    }
    bool stop = false;

    auto produce_fiber = ss::do_until(
      [&stop] { return stop; },
      [this, &params] {
          ss::lw_shared_ptr<consensus> raft;
          for (auto& n : nodes()) {
              if (n.second->raft()->is_leader()) {
                  raft = n.second->raft();
                  break;
              }
          }

          if (!raft) {
              return ss::sleep(100ms);
          }
          return raft
            ->replicate(
              make_batches(10, 10, 128), replicate_options(params.c_lvl))
            .then([this](result<replicate_result> result) {
                if (result.has_error()) {
                    vlog(
                      logger().info,
                      "error(replicating): {}",
                      result.error().message());
                }
            });
      });
    int transfers = 200;
    auto l_transfer_fiber = ss::do_until(
      [&transfers, &stop] { return transfers-- <= 0 || stop; },
      [this] {
          std::vector<raft::vnode> not_leaders;
          ss::lw_shared_ptr<consensus> raft;
          for (auto& n : nodes()) {
              if (n.second->raft()->is_leader()) {
                  raft = n.second->raft();
              } else {
                  not_leaders.push_back(n.second->get_vnode());
              }
          }

          if (!raft) {
              return ss::sleep(100ms);
          }
          auto target = random_generators::random_choice(not_leaders);
          return raft
            ->transfer_leadership(
              transfer_leadership_request{
                .group = raft->group(),
                .target = target.id(),
                .timeout = 25ms,
              })
            .then([this](transfer_leadership_reply r) {
                if (r.result != raft::errc::success) {
                    vlog(logger().info, "error(transferring): {}", r);
                }
            })
            .then([] { return ss::sleep(200ms); });
      });

    absl::node_hash_map<model::node_id, model::offset> last_visible;
    auto validator_fiber = ss::do_until(
      [&stop] { return stop; },
      [this, &last_visible] {
          for (auto& [id, node] : nodes()) {
              auto o = node->raft()->last_visible_index();

              auto dirty_offset = node->raft()->dirty_offset();
              vassert(
                o <= dirty_offset,
                "last visible offset {} on node {} can not be larger than log "
                "end offset {}",
                o,
                id,
                dirty_offset);
              last_visible[id] = o;
          }
          return ss::sleep(10ms);
      });

    co_await ss::sleep(30s);
    stop = true;
    vlog(logger().info, "Stopped");
    co_await std::move(produce_fiber);
    vlog(logger().info, "Stopped produce");
    co_await std::move(l_transfer_fiber);
    vlog(logger().info, "Stopped transfer");
    co_await std::move(validator_fiber);
    vlog(logger().info, "Stopped validator");

    for (auto& n : nodes()) {
        auto r = n.second->raft();
        vlog(
          logger().info,
          "leader: {} log_end: {}, visible: {} \n",
          r->is_leader(),
          r->dirty_offset(),
          r->last_visible_index());
        if (r->is_leader()) {
            for (auto& fs : r->get_follower_states()) {
                vlog(logger().info, "follower: {}", fs.second);
            }
        }
    }
}

/**
 * Ensures that the produce request can correctly detect truncation
 * and make progress rather than being blocked forever waiting for
 * the offsets to appear.
 */
TEST_P_CORO(quorum_acks_fixture, test_progress_on_truncation) {
    /**
     * Truncation detection test is expected to experience a log truncation,
     * hence we disable longest log detection
     */
    set_enable_longest_log_detection(false);
    co_await create_simple_group(3);
    auto leader_id = co_await wait_for_leader(10s);
    auto params = GetParam();
    co_await set_write_caching(params.write_caching);

    // inject delay into append entries requests from the leader to
    // open up a window for leadership change and a subsequent
    // truncation.
    for (auto& [id, node] : nodes()) {
        if (id == leader_id) {
            node->on_dispatch(
              [](model::node_id, raft::msg_type) { return ss::sleep(5s); });
        }
    }

    auto leader_raft = nodes().at(leader_id)->raft();
    ASSERT_TRUE_CORO(leader_raft->is_leader());

    // Append a big-ish batch, spanning multiple offsets,
    // this is delayed in append entries due to sleep injection.
    // the sleep also triggers a leadership change due to
    // hb supression in that window.
    auto produce_f = leader_raft->replicate(
      make_batches(10, 10, 128), replicate_options(params.c_lvl));

    // This should never timeout if the truncation detection works
    // as expected.
    auto result = co_await ss::with_timeout(
      model::timeout_clock::now() + 10s, std::move(produce_f));

    ASSERT_TRUE_CORO(!leader_raft->is_leader());
    ASSERT_TRUE_CORO(result.has_error());
    ASSERT_EQ_CORO(result.error(), raft::errc::replicated_entry_truncated);
}

INSTANTIATE_TEST_SUITE_P(
  test_with_all_acks,
  all_acks_fixture,
  testing::Values(
    test_parameters{.c_lvl = consistency_level::no_ack, .write_caching = false},
    test_parameters{
      .c_lvl = consistency_level::leader_ack, .write_caching = false},
    test_parameters{
      .c_lvl = consistency_level::quorum_ack, .write_caching = false},
    test_parameters{
      .c_lvl = consistency_level::quorum_ack, .write_caching = true}));

INSTANTIATE_TEST_SUITE_P(
  test_with_relaxed_acks,
  relaxed_acks_fixture,
  testing::Values(
    test_parameters{.c_lvl = consistency_level::no_ack, .write_caching = false},
    test_parameters{
      .c_lvl = consistency_level::leader_ack, .write_caching = false},
    test_parameters{
      .c_lvl = consistency_level::quorum_ack, .write_caching = true}));

INSTANTIATE_TEST_SUITE_P(
  test_with_quorum_acks,
  quorum_acks_fixture,
  testing::Values(
    test_parameters{
      .c_lvl = consistency_level::quorum_ack, .write_caching = false},
    test_parameters{
      .c_lvl = consistency_level::quorum_ack, .write_caching = true}));

TEST_F_CORO(raft_fixture, test_prioritizing_longest_log) {
    co_await create_simple_group(3);

    /**
     * Enable write
     */
    co_await set_write_caching(true);
    auto r = co_await retry_with_leader(
      10s + model::timeout_clock::now(),
      [this](raft_node_instance& leader_node) {
          return leader_node.raft()->replicate(
            make_batches(10, 10, 128),
            replicate_options(consistency_level::quorum_ack));
      });
    ASSERT_TRUE_CORO(r.has_value());
    /**
     * wait for all nodes
     */
    auto visible_offset = r.value().last_offset;
    co_await wait_for_visible_offset(visible_offset, 10s);

    /**
     * Stop all nodes
     */
    auto ids_set = all_ids();
    std::vector<model::node_id> ids(ids_set.begin(), ids_set.end());
    auto survivor = random_generators::random_choice(ids);

    for (auto& id : ids) {
        auto data_dir = node(id).raft()->log()->config().base_directory();
        co_await stop_node(
          id, survivor == id ? remove_data_dir::no : remove_data_dir::yes);
        add_node(id, model::revision_id(0), std::move(data_dir));
    }

    for (auto& [id, n] : nodes()) {
        co_await n->init_and_start(all_vnodes());
    }

    co_await wait_for_leader(10s);

    co_await wait_for_visible_offset(visible_offset, 10s);
}

TEST_F_CORO(raft_fixture, test_delayed_snapshot_request) {
    // a struct to handle which nodes to stop, start, reconfigure, etc
    struct reconfiguration_helper {
        const std::vector<vnode> vnodes;

        static model::revision_id increment_rid(model::revision_id id) {
            return static_cast<model::revision_id>(static_cast<long>(id) + 1);
        }

        vnode get_designated_survivor() const { return vnodes.at(0); }

        // get all nodes that aren't the designated survivor
        std::vector<vnode> get_nodes_to_remove() const {
            return vnodes | std::views::filter([this](const vnode& node) {
                       return node != get_designated_survivor();
                   })
                   | std::ranges::to<std::vector<vnode>>();
        }

        // version increment the nodes which were previously stopped s.t. they
        // can be restarted with a new revision number
        std::vector<vnode> get_nodes_to_start() const {
            return get_nodes_to_remove()
                   | std::views::transform([](const vnode& node) {
                         return vnode{
                           node.id(), increment_rid(node.revision())};
                     })
                   | std::ranges::to<std::vector<vnode>>();
        }

        // new configuration should be restarted nodes plus the designated
        // survivor which never left the raft group
        std::vector<vnode> get_restarted_vnode_configuration() const {
            auto restarted_nodes = get_nodes_to_start();
            restarted_nodes.emplace_back(get_designated_survivor());
            return restarted_nodes;
        }
    };

    // setup
    co_await create_simple_group(3);
    reconfiguration_helper reconfiguration_helper{.vnodes = all_vnodes()};

    auto replicate_some_data = [&] {
        return retry_with_leader(
                 10s + model::timeout_clock::now(),
                 [this](raft_node_instance& leader_node) {
                     return leader_node.raft()->replicate(
                       make_batches(10, 10, 128),
                       replicate_options(consistency_level::quorum_ack));
                 })
          .then([&](result<replicate_result> result) {
              if (result) {
                  vlog(
                    logger().info,
                    "replication result last offset: {}",
                    result.value().last_offset);
              } else {
                  vlog(
                    logger().info,
                    "replication error: {}",
                    result.error().message());
              }
          });
    };

    co_await replicate_some_data();

    // the reconfiguration process looks like
    // 1. reconfigure from 3 nodes to 1 node
    // 2. wait for reconfiguration
    // 3. stop the dropped nodes

    // pick one node to remain in the group
    co_await retry_with_leader(
      10s + model::timeout_clock::now(),
      [&reconfiguration_helper](raft_node_instance& leader_node) {
          return leader_node.raft()->replace_configuration(
            {reconfiguration_helper.get_designated_survivor()},
            model::revision_id{1});
      });

    // wait for reconfiguration
    auto wait_for_reconfiguration = [&](size_t expected_nodes) {
        return tests::cooperative_spin_wait_with_timeout(
          10s, [&, expected_nodes] {
              return std::all_of(
                nodes().begin(),
                nodes().end(),
                [expected_nodes](const auto& p) {
                    return p.second->raft()->config().all_nodes().size()
                             == expected_nodes
                           && p.second->raft()->config().get_state()
                                == configuration_state::simple;
                });
          });
    };
    co_await wait_for_reconfiguration(1);

    // stop the nodes removed from the group
    for (const auto node_to_stop :
         reconfiguration_helper.get_nodes_to_remove()) {
        co_await stop_node(node_to_stop.id());
    }

    auto leader_node_id = get_leader();
    ASSERT_TRUE_CORO(leader_node_id.has_value());
    auto& leader_node = node(leader_node_id.value());
    ASSERT_EQ_CORO(leader_node.raft()->config().all_nodes().size(), 1);

    co_await replicate_some_data();

    // the reconfiguration process back to 3 looks like
    // 1. add & start new nodes
    // 2. reconfigure to attatch the new nodes
    // 3. wait for reconfiguration

    // build new nodes s.t. the cluster can reconfigure back to 3 nodes
    for (const vnode node : reconfiguration_helper.get_nodes_to_start()) {
        auto& added_node = add_node(node.id(), node.revision());
        co_await added_node.init_and_start(/*no initial nodes*/ {});
    }

    // dispatch the reconfiguration to join new nodes to the raft group
    co_await retry_with_leader(
      10s + model::timeout_clock::now(),
      [&reconfiguration_helper](raft_node_instance& leader_node) {
          return leader_node.raft()->replace_configuration(
            reconfiguration_helper.get_restarted_vnode_configuration(),
            model::revision_id{2});
      });

    // wait for reconfiguration
    co_await wait_for_reconfiguration(3);

    co_await replicate_some_data();

    auto new_leader_node_id = get_leader();
    ASSERT_TRUE_CORO(new_leader_node_id.has_value());
    auto& new_leader_node = node(new_leader_node_id.value());
    ASSERT_EQ_CORO(new_leader_node.raft()->config().all_nodes().size(), 3);

    const auto& p = std::find_if(nodes().begin(), nodes().end(), [&](auto& p) {
        return p.second->get_vnode() != new_leader_node.get_vnode();
    });
    auto& follower_node = p->second;
    auto leader_proto = new_leader_node.get_buffered_protocol();
    // simulate delayed install snapshot request send to follower
    install_snapshot_request request;
    request.target_node_id = follower_node->get_vnode();
    request.node_id = leader_node.get_vnode();
    request.group = follower_node->raft()->group();

    /**
     * A snapshot request represent a state from the point in time when group
     * had only one member. Currently the follower is already using
     * configuration with 3 members
     */
    auto last_included = model::offset(random_generators::get_int(105, 199));
    request.last_included_index = last_included;
    request.dirty_offset = leader_node.raft()->dirty_offset();
    request.term = leader_node.raft()->term();

    snapshot_metadata metadata{
      .last_included_index = request.last_included_index,
      .last_included_term = leader_node.raft()->term(),
      .latest_configuration = raft::group_configuration(
        {all_vnodes()[0]}, model::revision_id(1)),
      .log_start_delta = offset_translator_delta(2),
    };

    iobuf snapshot;
    // using snapshot writer to populate all relevant snapshot metadata i.e.
    // header and crc
    storage::snapshot_writer writer(make_iobuf_ref_output_stream(snapshot));

    co_await writer.write_metadata(reflection::to_iobuf(std::move(metadata)));
    co_await write_iobuf_to_output_stream(iobuf{}, writer.output());
    co_await writer.close();
    request.chunk = snapshot.copy();
    request.file_offset = 0;
    request.done = true;

    auto reply = co_await leader_proto->install_snapshot(
      follower_node->get_vnode().id(),
      std::move(request),
      rpc::client_opts(10s));
    ASSERT_TRUE_CORO(reply.has_value());
    vlog(logger().info, "snapshot reply from follower: {}", reply.value());

    // the snapshot contains a configuration with one node which is older than
    // the current one the follower has. latest configuration MUST remain
    // unchanged

    ASSERT_EQ_CORO(follower_node->raft()->config().all_nodes().size(), 3);
    EXPECT_EQ(follower_node->raft()->get_follower_states().size(), 2);
    // entries in follower log should be truncated.
    ASSERT_EQ_CORO(
      follower_node->raft()->start_offset(), model::next_offset(last_included));

    /**
     * Make sure the leader steps down when it receives an install snapshot
     * request
     */

    auto follower_proto = follower_node->get_buffered_protocol();
    install_snapshot_request request_for_leader;

    request_for_leader.group = follower_node->raft()->group();
    request_for_leader.target_node_id = new_leader_node.get_vnode();
    request_for_leader.node_id = follower_node->get_vnode();
    request_for_leader.last_included_index = model::offset(
      random_generators::get_int(105, 199));
    request_for_leader.dirty_offset = leader_node.raft()->dirty_offset();
    request_for_leader.term = leader_node.raft()->term();
    request_for_leader.chunk = std::move(snapshot);
    request_for_leader.done = true;
    auto term_snapshot = leader_node.raft()->term();
    auto leader_reply = co_await follower_proto->install_snapshot(
      new_leader_node.get_vnode().id(),
      std::move(request_for_leader),
      rpc::client_opts(10s));

    ASSERT_TRUE_CORO(leader_reply.has_value());
    vlog(logger().info, "snapshot reply from leader: {}", leader_reply.value());
    co_await tests::cooperative_spin_wait_with_timeout(10s, [&] {
        return nodes().begin()->second->raft()->term() > term_snapshot;
    });
}

TEST_F_CORO(raft_fixture, leadership_transfer_delay) {
    set_election_timeout(1500ms);
    co_await create_simple_group(4);
    auto replicate_some_data = [&] {
        return retry_with_leader(
                 10s + model::timeout_clock::now(),
                 [this](raft_node_instance& leader_node) {
                     return leader_node.raft()->replicate(
                       make_batches(10, 10, 128),
                       replicate_options(consistency_level::quorum_ack));
                 })
          .then([&](result<replicate_result> result) {
              if (result) {
                  vlog(
                    logger().info,
                    "replication result last offset: {}",
                    result.value().last_offset);
              } else {
                  vlog(
                    logger().info,
                    "replication error: {}",
                    result.error().message());
              }
          });
    };
    using clock_t = std::chrono::high_resolution_clock;
    co_await replicate_some_data();
    struct leadership_changed_event {
        model::node_id node;
        leadership_status status;
        clock_t::time_point timestamp;
    };
    ss::circular_buffer<leadership_changed_event> events;

    register_leader_callback([&](model::node_id id, leadership_status status) {
        events.push_back(
          leadership_changed_event{
            .node = id,
            .status = status,
            .timestamp = clock_t::now(),
          });
    });
    auto leader_id = get_leader().value();
    auto& leader_node = node(leader_id);
    auto current_term = leader_node.raft()->term();
    auto r = co_await leader_node.raft()->transfer_leadership(
      transfer_leadership_request{.group = leader_node.raft()->group()});
    ASSERT_TRUE_CORO(r.success);
    // here we wait for all the replicas to notify about the leadership changes,
    // each replica will notify two times, one when there is no leader, second
    // time when the leader is elected. We have 4 replicas so in total we expect
    // 8 notifications to be fired.
    co_await tests::cooperative_spin_wait_with_timeout(
      10s, [&] { return events.size() >= 8; });

    // calculate the time needed to transfer leadership, in our case it is the
    // time between first notification reporting no leader and first reporting
    // new leader.
    auto new_leader_reported_ev = std::find_if(
      events.begin(), events.end(), [&](leadership_changed_event& ev) {
          return ev.status.current_leader.has_value()
                 && ev.status.term > current_term;
      });

    auto transfer_time = new_leader_reported_ev->timestamp
                         - events.begin()->timestamp;
    vlog(
      logger().info,
      "leadership_transfer - new leader reported after: {} ms",
      (transfer_time) / 1ms);
    events.clear();
    // now remove the current leader from the raft group
    leader_id = get_leader().value();
    auto new_nodes = all_vnodes() | std::views::filter([&](vnode n) {
                         return n.id() != leader_id;
                     });
    auto& new_leader_node = node(leader_id);
    current_term = new_leader_node.raft()->term();
    co_await new_leader_node.raft()->replace_configuration(
      std::vector<vnode>{new_nodes.begin(), new_nodes.end()},
      model::revision_id(2));
    // analogically to the previous case we wait for 6 notifications as
    // currently the group has only 3 replicas
    co_await tests::cooperative_spin_wait_with_timeout(
      10s, [&] { return events.size() >= 6; });

    auto leader_reported_after_reconfiguration = std::find_if(
      events.begin(), events.end(), [&](leadership_changed_event& ev) {
          return ev.status.current_leader.has_value()
                 && ev.status.term > current_term;
      });

    auto election_time = leader_reported_after_reconfiguration->timestamp
                         - events.begin()->timestamp;
    vlog(
      logger().info,
      "reconfiguration - new leader reported after: {} ms",
      (election_time) / 1ms);

    for (auto& vn : all_vnodes()) {
        co_await stop_node(vn.id());
    }

    auto tolerance_multiplier = 1.7;
    /**
     * Validate that election time after reconfiguration is simillar to the
     * time needed for leadership transfer
     */
    ASSERT_LE_CORO(election_time * 1.0, transfer_time * tolerance_multiplier);
    ASSERT_GE_CORO(election_time * 1.0, transfer_time / tolerance_multiplier);
}

TEST_F_CORO(raft_fixture, test_no_stepdown_on_append_entries_timeout) {
    config::shard_local_cfg().replicate_append_timeout_ms.set_value(1s);
    co_await create_simple_group(3);
    auto leader_id = co_await wait_for_leader(10s);
    for (auto& [id, n] : nodes()) {
        if (id != leader_id) {
            n->f_injectable_log()->set_append_delay([]() { return 5s; });
        }
    }

    auto& leader_node = node(leader_id);
    auto term_before = leader_node.raft()->term();
    auto r = co_await leader_node.raft()->replicate(
      make_batches(1, 10, 128),
      replicate_options(consistency_level::quorum_ack, 10s));
    ASSERT_FALSE_CORO(r.has_error());
    for (auto& [_, n] : nodes()) {
        n->f_injectable_log()->set_append_delay(std::nullopt);
    }

    leader_id = co_await wait_for_leader(10s);
    auto& new_leader_node = node(leader_id);
    ASSERT_EQ_CORO(term_before, new_leader_node.raft()->term());
    ASSERT_TRUE_CORO(new_leader_node.raft()->is_leader());
}

/**
 * This synthetic test is there to trigger a situation in which follower
 * receives an append entries request which contains only batches that matches
 * its log. This trigger a condition in which the follower should reply with
 * success to the leader so the leader can continue recovery process.
 *
 * The test uses reply interception to 'trick' the leader to send the append
 * entries with the batches that the follower already has.
 */
TEST_F_CORO(raft_fixture, test_redelivery_of_matching_logs) {
    co_await create_simple_group(3);
    auto term_1_leader_id = co_await wait_for_leader(10s);
    for (auto& [id, n] : nodes()) {
        n->set_default_recovery_read_size(1);
    }
    auto term_1_follower_id = random_follower_id().value();
    auto& t1_leader_node = node(term_1_leader_id);
    /**
     * Replicate data to all nodes
     */
    auto r = co_await t1_leader_node.raft()->replicate(
      make_batches(200, 1, 10),
      replicate_options(consistency_level::quorum_ack, 10s));
    co_await wait_for_committed_offset(r.value().last_offset, 5s);
    auto term_1_match_offset = node(term_1_follower_id).raft()->dirty_offset();

    /**
     * Prevent any nodes from receiveing append entry requests from the leader
     */
    t1_leader_node.on_dispatch([](model::node_id, raft::msg_type mt) {
        if (mt == raft::msg_type::append_entries) {
            throw std::runtime_error("error");
        }
        return ss::now();
    });

    /**
     * Replicate some data with the leader ack consistency level so the current
     * leader has the longest log
     */
    r = co_await t1_leader_node.raft()->replicate(
      make_batches(20, 1, 10),
      replicate_options(consistency_level::leader_ack, 10s));

    ASSERT_FALSE_CORO(r.has_error());
    // leader has longest log, prevent follower from sending vote requests to
    // term 1 leader.
    node(term_1_follower_id)
      .on_dispatch([term_1_leader_id](model::node_id id, raft::msg_type) {
          if (term_1_leader_id == id) {
              throw std::runtime_error("error");
          }
          return ss::now();
      });

    // stepdown to trigger another leader election
    t1_leader_node.raft()->block_new_leadership();
    co_await t1_leader_node.raft()->step_down(
      model::term_id(2), "test step down");

    auto new_leader_id = co_await wait_for_leader(10s);
    // this is the only candidate as the other one will receive information from
    // term 1 leader that it has the longest log.
    ASSERT_EQ_CORO(new_leader_id, term_1_follower_id);

    auto& new_leader_node = node(new_leader_id);
    logger().info(
      "new leader offsets: {}, term_1_match: {}",
      new_leader_node.raft()->log()->offsets(),
      term_1_match_offset);

    /**
     * Trick the leader right at the offset where the leader and follower log
     * would match
     */
    ss::condition_variable reply_intercepted;
    size_t intercept_count = 0;
    new_leader_node.set_reply_interceptor(
      [&, term_1_match_offset](reply_variant reply, model::node_id) {
          return ss::visit(
            std::move(reply),
            [&, term_1_match_offset](append_entries_reply a_r) {
                if (
                  a_r.last_dirty_log_index
                  == model::next_offset(term_1_match_offset)) {
                    a_r.result = reply_result::failure;
                    intercept_count++;
                    reply_intercepted.signal();
                }
                return ss::make_ready_future<reply_variant>(a_r);
            },
            [](auto r) {
                return ss::make_ready_future<reply_variant>(std::move(r));
            });
      });
    /**
     * Recover communication and wait for the intercept to trigger
     */
    new_leader_node.reset_dispatch_handlers();
    co_await reply_intercepted.wait([&] { return intercept_count > 5; });
    new_leader_node.reset_reply_interceptor();

    co_await wait_for_committed_offset(
      new_leader_node.raft()->dirty_offset(), 5s);
}

TEST_F_CORO(raft_fixture, test_term_conditional_replication) {
    co_await create_simple_group(3);
    auto leader_id = co_await wait_for_leader(10s);
    auto& leader_node = node(leader_id);
    auto term = leader_node.raft()->term();
    // this should succeed as there were no leadership changes
    auto result = co_await leader_node.raft()->replicate(
      make_batches(10, 10, 128),
      replicate_options(consistency_level::quorum_ack, term, 10s));

    ASSERT_TRUE_CORO(result.has_value());
    /**
     * Make sure the current leader will be re-elected
     */
    for (auto& [id, node] : nodes()) {
        if (id != leader_id) {
            node->raft()->block_new_leadership();
        }
    }
    co_await leader_node.raft()->step_down("test-step-down");

    auto new_leader_id = co_await wait_for_leader(10s);
    ASSERT_EQ_CORO(new_leader_id, leader_id);
    auto result_with_term = co_await leader_node.raft()->replicate(
      make_batches(10, 10, 128),
      replicate_options(consistency_level::quorum_ack, term, 10s));

    // Replication must fail as the term was increased in leader election
    ASSERT_TRUE_CORO(result_with_term.has_error());
    // No term provided, replication will succeed
    auto result_without_term = co_await leader_node.raft()->replicate(
      make_batches(10, 10, 128),
      replicate_options(consistency_level::quorum_ack, 10s));
    ASSERT_FALSE_CORO(result_without_term.has_error());
}

TEST_F_CORO(raft_fixture, test_replicate_abort_source) {
    co_await create_simple_group(3);
    auto leader_id = co_await wait_for_leader(10s);
    auto& leader_node = node(leader_id);
    // this should succeed as there were no leadership changes
    auto result = co_await leader_node.raft()->replicate(
      make_batches(10, 10, 128),
      replicate_options(consistency_level::quorum_ack));
    ASSERT_FALSE_CORO(result.has_error());

    /**
     * Block all append entries from leader to followers to prevent any
     * subsequent replicate calls from finishing
     */
    leader_node.on_dispatch([](model::node_id, raft::msg_type mt) {
        if (mt == raft::msg_type::append_entries) {
            throw std::runtime_error("error");
        }
        return ss::now();
    });
    /**
     * Abort after replicate
     */
    ss::abort_source as_1;
    auto as_1_f = leader_node.raft()->replicate(
      make_batches(1, 1, 128),
      replicate_options(consistency_level::quorum_ack, std::ref(as_1)));
    co_await ss::sleep(500ms);
    ASSERT_FALSE_CORO(as_1_f.available());

    as_1.request_abort();
    auto r_1 = co_await std::move(as_1_f);
    ASSERT_TRUE_CORO(r_1.has_error());
    ASSERT_EQ_CORO(r_1.error(), raft::errc::shutting_down);
    /**
     * Already aborted
     */
    auto r_2 = co_await leader_node.raft()->replicate(
      make_batches(1, 1, 128),
      replicate_options(consistency_level::quorum_ack, std::ref(as_1)));

    ASSERT_TRUE_CORO(r_2.has_error());
    ASSERT_EQ_CORO(r_2.error(), raft::errc::replicate_first_stage_exception);
}

TEST_F_CORO(raft_fixture, test_leadership_blocked_replicas_can_elect_leader) {
    co_await create_simple_group(3);
    auto leader_id = co_await wait_for_leader(10s);
    auto& leader_node = node(leader_id);
    // replicate some batches to all replicas
    auto res = co_await leader_node.raft()->replicate(
      make_batches(1, 1, 128),
      replicate_options(consistency_level::quorum_ack));
    ASSERT_FALSE_CORO(res.has_error());
    auto blocked_follower = random_follower_id().value();
    /**
     * Block append entries to one of the followers
     */
    leader_node.on_dispatch([blocked_follower](
                              model::node_id node, raft::msg_type mt) {
        if (mt == raft::msg_type::append_entries && blocked_follower == node) {
            throw std::runtime_error("error");
        }
        return ss::now();
    });
    // replicate more batches.
    res = co_await leader_node.raft()->replicate(
      make_batches(1, 1, 128),
      replicate_options(consistency_level::quorum_ack));
    ASSERT_FALSE_CORO(res.has_error());
    /**
     * Since append entries to one of the followers is blocked the two other
     * replicas has the log longer than the blocked one.
     *
     * Now block leadership on all replicas but not the follower with the
     * shortest log. The follower with shortest log can not be elected as a
     * leader as it would cause truncation.
     *
     * Even tho the other replicas are blocked from being a leaders the raft
     * group should finally elect a leader when enough failed election will
     * cause the target priority to go down to 1.
     */

    for (auto& [nid, node] : nodes()) {
        if (nid != blocked_follower) {
            node->raft()->block_new_leadership();
        }
    }
    co_await leader_node.raft()->step_down("test-step-down");

    auto leader = co_await wait_for_leader(60s);
    ASSERT_NE_CORO(leader, blocked_follower);
}

/**
 * An append entries request from the current term leader must refresh the
 * follower election timer even when the request is rejected because the
 * follower log is behind (gap) or the previous log entry term does not
 * match. Without the refresh a follower that stays in contact with a live
 * leader, but can not append because it is still being recovered, keeps
 * starting elections that deterministically fail the longest log check.
 * The resulting (pre-)vote storm starves recovery, a livelock.
 *
 * Note that during active follower recovery lightweight heartbeats are
 * rejected to force full heartbeats, so rejected full append entries
 * requests can be the dominant form of leader contact.
 */
TEST_F_CORO(raft_fixture, test_rejected_append_entries_refresh_hbeat) {
    set_election_timeout(1s);
    co_await create_simple_group(3);
    auto leader_id = co_await wait_for_leader(10s);
    auto& leader_node = node(leader_id);

    auto res = co_await leader_node.raft()->replicate(
      make_batches(10, 1, 128),
      replicate_options(consistency_level::quorum_ack, 10s));
    ASSERT_FALSE_CORO(res.has_error());
    co_await wait_for_committed_offset(res.value().last_offset, 10s);

    auto follower_id = random_follower_id().value();
    auto& follower_node = node(follower_id);
    auto follower = follower_node.raft();
    const auto term = follower->term();

    auto cleanup = ss::defer([&] {
        leader_node.reset_dispatch_handlers();
        follower_node.reset_dispatch_handlers();
    });

    // Cut the follower off from real leader traffic. The requests crafted
    // below are its only contact with the leader.
    leader_node.on_dispatch([follower_id](model::node_id target, msg_type) {
        if (target == follower_id) {
            throw std::runtime_error("blocked by test");
        }
        return ss::now();
    });

    size_t follower_vote_requests = 0;
    follower_node.on_dispatch(
      [&follower_vote_requests](model::node_id, msg_type type) {
          if (type == msg_type::vote) {
              ++follower_vote_requests;
          }
          return ss::now();
      });

    auto make_request =
      [&](model::offset prev_log_index, model::term_id prev_log_term) {
          return append_entries_request(
            leader_node.get_vnode(),
            follower_node.get_vnode(),
            protocol_metadata{
              .group = follower->group(),
              .commit_index = follower->committed_offset(),
              .term = term,
              .prev_log_index = prev_log_index,
              .prev_log_term = prev_log_term,
              .last_visible_index = follower->last_visible_index(),
              .dirty_offset = prev_log_index,
            },
            chunked_vector<model::record_batch>{},
            0,
            flush_after_append::no);
      };

    // let the timestamp of the last real leader contact age
    co_await ss::sleep(200ms);

    // rejected because it would leave a gap in the follower log
    auto hbeat_before = follower->last_heartbeat();
    auto reply = co_await follower->append_entries(
      make_request(model::offset(follower->dirty_offset()() + 1000), term));
    ASSERT_EQ_CORO(reply.result, reply_result::failure);
    ASSERT_GT_CORO(follower->last_heartbeat(), hbeat_before);

    co_await ss::sleep(200ms);

    // rejected because the previous log entry term does not match
    hbeat_before = follower->last_heartbeat();
    auto lstats = follower->log()->offsets();
    reply = co_await follower->append_entries(make_request(
      lstats.dirty_offset, model::term_id(lstats.dirty_offset_term() - 1)));
    ASSERT_EQ_CORO(reply.result, reply_result::failure);
    ASSERT_GT_CORO(follower->last_heartbeat(), hbeat_before);

    /**
     * Keep the follower in contact with the leader through rejected append
     * entries only, for several election timeouts (with margin for the
     * voter priority decay rounds preceding the first vote dispatch). The
     * follower must not attempt an election.
     */
    auto deadline = model::timeout_clock::now() + 8s;
    while (model::timeout_clock::now() < deadline) {
        reply = co_await follower->append_entries(
          make_request(model::offset(follower->dirty_offset()() + 1000), term));
        ASSERT_EQ_CORO(reply.result, reply_result::failure);
        co_await ss::sleep(100ms);
    }
    ASSERT_EQ_CORO(follower_vote_requests, 0);
    ASSERT_EQ_CORO(follower->term(), term);
}

/**
 * Livelock reproducer: a follower in active recovery only refreshes its
 * election timer on full append entries requests, lightweight heartbeats
 * are rejected without a refresh to make the leader fall back to a full
 * heartbeat. That fallback is suppressed while any append request to the
 * follower is in flight. With replication or recovery requests stalled in
 * flight (slow network or overloaded follower), every heartbeat round
 * observes an in-flight append and picks a lightweight heartbeat again,
 * starving the follower of election timer refreshes while it stays in
 * continuous contact with a live leader.
 *
 * Liveness property asserted: such a follower must not start elections.
 */
TEST_F_CORO(raft_fixture, test_no_election_storm_with_stalled_appends) {
    set_election_timeout(1s);
    co_await create_simple_group(3);
    auto leader_id = co_await wait_for_leader(10s);
    auto& leader_node = node(leader_id);

    auto res = co_await leader_node.raft()->replicate(
      make_batches(10, 1, 128),
      replicate_options(consistency_level::quorum_ack, 10s));
    ASSERT_FALSE_CORO(res.has_error());
    co_await wait_for_committed_offset(res.value().last_offset, 10s);

    auto follower_id = random_follower_id().value();
    auto& follower_node = node(follower_id);
    auto follower = follower_node.raft();
    const auto term = follower->term();

    ss::abort_source stall_as;
    auto cleanup = ss::defer([&] {
        stall_as.request_abort();
        leader_node.reset_reply_interceptor();
        leader_node.reset_dispatch_handlers();
        follower_node.reset_dispatch_handlers();
    });

    // Stall replication and recovery traffic to the follower: requests
    // stay in flight for the whole observation window, keeping an
    // in-flight append guard alive at every heartbeat round.
    leader_node.on_dispatch(
      [follower_id, &stall_as](model::node_id target, msg_type t) {
          if (target == follower_id && t == msg_type::append_entries) {
              return ss::sleep_abortable(8s, stall_as);
          }
          return ss::now();
      });

    res = co_await leader_node.raft()->replicate(
      make_batches(10, 1, 128),
      replicate_options(consistency_level::leader_ack, 10s));
    ASSERT_FALSE_CORO(res.has_error());

    // Put the follower into an active recovery state with a crafted
    // recovery-shaped request (dirty offset ahead of the previous offset
    // means the leader considers this follower already recovering).
    auto gap_prev = model::offset(follower->dirty_offset()() + 1000);
    auto reply = co_await follower->append_entries(append_entries_request(
      leader_node.get_vnode(),
      follower_node.get_vnode(),
      protocol_metadata{
        .group = follower->group(),
        .commit_index = follower->committed_offset(),
        .term = term,
        .prev_log_index = gap_prev,
        .prev_log_term = term,
        .last_visible_index = follower->last_visible_index(),
        .dirty_offset = model::offset(gap_prev() + 100),
      },
      chunked_vector<model::record_batch>{},
      0,
      flush_after_append::no));
    ASSERT_EQ_CORO(reply.result, reply_result::failure);
    ASSERT_TRUE_CORO(reply.may_recover);

    // A lightweight heartbeat rejected because recovery is active must
    // still count as leader contact. Pause heartbeats so the last contact
    // timestamp ages, then deliver one directly. Dispatch handlers
    // accumulate, the append stall above stays active.
    leader_node.on_dispatch([follower_id](model::node_id target, msg_type t) {
        if (target == follower_id && t == msg_type::heartbeat_v2) {
            throw std::runtime_error("paused by test");
        }
        return ss::now();
    });
    co_await ss::sleep(200ms);
    auto hbeat_before = follower->last_heartbeat();
    ASSERT_EQ_CORO(
      follower->lightweight_heartbeat(leader_id, follower_id),
      reply_result::failure);
    ASSERT_GT_CORO(follower->last_heartbeat(), hbeat_before);

    leader_node.reset_dispatch_handlers();
    leader_node.on_dispatch(
      [follower_id, &stall_as](model::node_id target, msg_type t) {
          if (target == follower_id && t == msg_type::append_entries) {
              return ss::sleep_abortable(8s, stall_as);
          }
          return ss::now();
      });

    size_t follower_vote_requests = 0;
    follower_node.on_dispatch(
      [&follower_vote_requests](model::node_id, msg_type type) {
          if (type == msg_type::vote) {
              ++follower_vote_requests;
          }
          return ss::now();
      });

    // The full heartbeat fallback after a lightweight heartbeat failure
    // must not be suppressed by the stalled in-flight appends.
    size_t follower_full_heartbeat_replies = 0;
    leader_node.set_reply_interceptor(
      [&follower_full_heartbeat_replies,
       follower_id](reply_variant reply, model::node_id from) {
          return ss::visit(
            std::move(reply),
            [&follower_full_heartbeat_replies, follower_id, from](
              heartbeat_reply_v2 hb) {
                if (from == follower_id && !hb.full_replies().empty()) {
                    ++follower_full_heartbeat_replies;
                }
                return ss::make_ready_future<reply_variant>(std::move(hb));
            },
            [](auto r) {
                return ss::make_ready_future<reply_variant>(std::move(r));
            });
      });

    co_await ss::sleep(6s);

    ASSERT_TRUE_CORO(node(leader_id).raft()->is_leader());
    ASSERT_EQ_CORO(follower->term(), term);
    ASSERT_EQ_CORO(follower_vote_requests, 0);
    ASSERT_GT_CORO(follower_full_heartbeat_replies, 0);
}
