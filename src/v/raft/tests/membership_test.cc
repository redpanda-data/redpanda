// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/metadata.h"
#include "raft/errc.h"
#include "raft/group_configuration.h"
#include "raft/tests/raft_group_fixture.h"
#include "storage/api.h"
#include "test_utils/async.h"

#include <boost/test/tools/old/interface.hpp>

#include <system_error>

FIXTURE_TEST(add_one_node_to_single_node_cluster, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 1);
    gr.enable_all();
    auto res = replicate_random_batches(gr, 1).get();
    BOOST_REQUIRE(res);
    auto new_node = gr.create_new_node(model::node_id(2));
    res = retry_with_leader(gr, 5, 1s, [new_node](raft_node& leader) {
              return leader.consensus
                ->add_group_member(
                  raft::vnode(new_node.id(), model::revision_id(0)),
                  model::revision_id(0))
                .then([](std::error_code ec) { return !ec; });
          }).get();

    validate_logs_replication(gr);
    auto& leader = gr.get_member(gr.get_leader_id().value());
    BOOST_REQUIRE_EQUAL(leader.consensus->config().all_nodes().size(), 2);
};

/**
 * After removing a node, check that the remaining nodes
 * have all advanced past it, to the same offset.
 *
 * @param removed_offset The offset of the just-removed group.
 */
void verify_removed_node_is_behind(
  raft_group& gr, model::offset removed_offset) {
    tests::cooperative_spin_wait_with_timeout(3s, [&gr, removed_offset] {
        return ss::async([&gr, removed_offset] {
            auto leader_id = gr.get_leader_id();
            if (!leader_id) {
                return false;
            }

            auto leader_offset = gr.get_member(leader_id.value())
                                   .consensus->last_visible_index();

            // Leader: the removed node must be behind this
            if (removed_offset >= leader_offset) {
                return false;
            }

            for (const auto& i : gr.get_members()) {
                if (i.first == leader_id) {
                    continue;
                } else {
                    // Follower: must be caught up to leader
                    if (
                      i.second.consensus->last_visible_index()
                      != leader_offset) {
                        return false;
                    }
                }
            }

            return true;
        });
    }).get();
}

FIXTURE_TEST(remove_non_leader, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto res = replicate_random_batches(gr, 2).get();
    BOOST_REQUIRE(res);
    auto& members = gr.get_members();
    auto non_leader_id = std::find_if(
                           members.begin(),
                           members.end(),
                           [](raft_group::members_t::value_type& p) {
                               return !p.second.consensus->is_elected_leader();
                           })
                           ->first;
    res = retry_with_leader(gr, 5, 1s, [non_leader_id](raft_node& leader) {
              return leader.consensus
                ->remove_member(
                  raft::vnode(non_leader_id, model::revision_id{0}),
                  model::revision_id(0))
                .then([](std::error_code ec) { return !ec; });
          }).get();
    BOOST_REQUIRE(res);

    tests::cooperative_spin_wait_with_timeout(5s, [&gr] {
        auto leader_id = gr.get_leader_id();
        if (!leader_id) {
            return false;
        }
        auto& leader = gr.get_member(*leader_id);
        return leader.consensus->config().all_nodes().size() == 2;
    }).get();

    auto write_ok = replicate_random_batches(gr, 2).get();
    BOOST_REQUIRE(write_ok);
    auto removed_offset
      = gr.get_member(non_leader_id).consensus->last_visible_index();

    // Emulate what the controller would do: tear down the removed
    // consensus instance.
    gr.disable_node(non_leader_id);

    verify_removed_node_is_behind(gr, removed_offset);
}

FIXTURE_TEST(remove_current_leader, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto res = replicate_random_batches(gr, 2).get();
    auto old_leader_id = wait_for_group_leader(gr);
    res = retry_with_leader(gr, 5, 1s, [old_leader_id](raft_node& leader) {
              return leader.consensus
                ->remove_member(
                  raft::vnode(old_leader_id, model::revision_id{0}),
                  model::revision_id(0))
                .then([](std::error_code ec) { return !ec; });
          }).get();

    tests::cooperative_spin_wait_with_timeout(5s, [&gr, old_leader_id] {
        auto leader_id = gr.get_leader_id();
        if (!leader_id) {
            return false;
        }
        auto& leader = gr.get_member(*leader_id);
        return leader.consensus->config().all_nodes().size() == 2
               && leader_id != old_leader_id;
    }).get();

    BOOST_REQUIRE_NE(gr.get_leader_id(), old_leader_id);
    res = replicate_random_batches(gr, 2).get();
    BOOST_REQUIRE(res);
    auto removed_offset
      = gr.get_member(old_leader_id).consensus->last_visible_index();

    gr.disable_node(old_leader_id);

    verify_removed_node_is_behind(gr, removed_offset);
    validate_offset_translation(gr);
}

FIXTURE_TEST(replace_whole_group, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    info("replicating some batches");
    auto res = replicate_random_batches(gr, 5).get();
    // wait for all group members to have the same log
    wait_for(
      10s,
      [&] { return are_all_commit_indexes_the_same(gr); },
      "initially all replicas have the same committed offsets");

    // all nodes are replaced with new node
    gr.create_new_node(model::node_id(5));
    std::vector<raft::vnode> new_members;
    new_members.reserve(1);
    new_members.emplace_back(model::node_id(5), model::revision_id(0));
    info("replacing configuration");
    res = retry_with_leader(gr, 5, 5s, [new_members](raft_node& leader) {
              return leader.consensus
                ->replace_configuration(new_members, model::revision_id(0))
                .then([](std::error_code ec) {
                    info("configuration replace result: {}", ec.message());
                    return !ec
                           || ec
                                == raft::errc::configuration_change_in_progress;
                });
          }).get();
    // if we failed to update configuration do nothing
    if (!res) {
        return;
    }
    info("waiting for all nodes to catch up");
    wait_for(
      5s,
      [&gr]() {
          using v_t = raft_group::logs_t::value_type;
          auto logs = gr.read_all_logs();
          raft_group::logs_t old_nodes_log;
          raft_group::logs_t new_nodes_log;
          for (auto& p : logs) {
              if (p.first > model::node_id(4)) {
                  new_nodes_log.emplace(p.first, std::move(p.second));
              } else {
                  old_nodes_log.emplace(p.first, std::move(p.second));
              }
          }
          if (
            old_nodes_log.begin()->second.empty()
            || new_nodes_log.begin()->second.empty()) {
              return false;
          }
          auto last_old = old_nodes_log.begin()->second.back().last_offset();
          auto last_new = new_nodes_log.begin()->second.back().last_offset();
          if (last_new < last_old) {
              return false;
          }
          return std::all_of(
            new_nodes_log.begin(), new_nodes_log.end(), [last_new](v_t& p) {
                return last_new == p.second.back().last_offset();
            });
      },
      "new nodes are up to date");

    wait_for(
      5s,
      [&gr]() {
          info("waiting for new group leader");
          auto new_leader_id = wait_for_group_leader(gr);

          return new_leader_id >= model::node_id(4);
      },
      "one of new nodes is a leader");

    auto new_leader_id = gr.get_leader_id();
    if (new_leader_id) {
        auto& new_leader = gr.get_member(*new_leader_id);
        BOOST_REQUIRE_EQUAL(
          new_leader.consensus->config().all_nodes().size(), 1);
    }
    validate_offset_translation(gr);
}

FIXTURE_TEST(
  make_sure_group_is_writable_during_config_replace, raft_test_fixture) {
    // raft group with single replica
    raft_group gr = raft_group(raft::group_id(0), 1);
    gr.enable_all();

    info("replicating some batches");
    auto res = replicate_random_batches(gr, 5).get();
    gr.create_new_node(model::node_id(5));
    auto broker = gr.get_member(model::node_id(5)).broker;
    gr.disable_node(model::node_id(5));
    std::vector<raft::vnode> new_members;
    new_members.reserve(1);
    new_members.emplace_back(model::node_id(5), model::revision_id(0));
    // replace configuration with other node, the target node is stopped
    // to keep the transient state in which the old node is the only voter in
    // raft group
    info("replacing configuration");
    res = retry_with_leader(gr, 5, 5s, [new_members](raft_node& leader) {
              return leader.consensus
                ->replace_configuration(new_members, model::revision_id(0))
                .then([](std::error_code ec) {
                    info("configuration replace result: {}", ec.message());
                    return !ec
                           || ec
                                == raft::errc::configuration_change_in_progress;
                });
          }).get();

    BOOST_REQUIRE(res);

    auto logs_before = gr.read_all_logs();
    // replicate more data, partition should be writable
    res = replicate_random_batches(gr, 5).get();
    BOOST_REQUIRE(res);
    auto logs_after = gr.read_all_logs();
    BOOST_REQUIRE_GT(
      logs_after.begin()->second.size(), logs_before.begin()->second.size());
    validate_offset_translation(gr);
}

FIXTURE_TEST(abort_configuration_change, raft_test_fixture) {
    // raft group with single replica
    raft_group gr = raft_group(raft::group_id(0), 1);
    gr.enable_all();
    auto res = replicate_random_batches(gr, 5).get();
    // try to move raft group to
    std::vector<raft::vnode> new_members;
    new_members.reserve(1);
    new_members.emplace_back(model::node_id(10), model::revision_id(0));
    res = retry_with_leader(gr, 5, 5s, [new_members](raft_node& leader) {
              return leader.consensus
                ->replace_configuration(new_members, model::revision_id(0))
                .then([](std::error_code ec) {
                    info("configuration replace result: {}", ec.message());
                    return !ec
                           || ec
                                == raft::errc::configuration_change_in_progress;
                });
          }).get();

    res = retry_with_leader(gr, 5, 5s, [new_members](raft_node& leader) {
              return leader.consensus
                ->abort_configuration_change(model::revision_id(0))
                .then([](std::error_code ec) {
                    info("configuration abort result: {}", ec.message());
                    return !ec
                           || ec
                                == raft::errc::configuration_change_in_progress;
                });
          }).get();

    BOOST_REQUIRE(res);

    auto current_cfg = gr.get_member(model::node_id(0)).consensus->config();
    BOOST_REQUIRE_EQUAL(current_cfg.all_nodes().size(), 1);
    BOOST_REQUIRE(current_cfg.get_state() == raft::configuration_state::simple);

    auto logs_before = gr.read_all_logs();
    // replicate more data, partition should be writable
    res = replicate_random_batches(gr, 5).get();
    BOOST_REQUIRE(res);
    auto logs_after = gr.read_all_logs();
    BOOST_REQUIRE_GT(
      logs_after.begin()->second.size(), logs_before.begin()->second.size());
    validate_offset_translation(gr);
}

FIXTURE_TEST(revert_configuration_change, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    info("replicating some batches");
    auto res = replicate_random_batches(gr, 5).get();
    // all nodes are replaced with new node
    gr.create_new_node(model::node_id(5));
    std::vector<raft::vnode> new_members;
    new_members.reserve(1);
    new_members.emplace_back(model::node_id(5), model::revision_id(0));
    info("replacing configuration");
    res = retry_with_leader(gr, 5, 5s, [new_members](raft_node& leader) {
              return leader.consensus
                ->replace_configuration(new_members, model::revision_id(0))
                .then([](std::error_code ec) {
                    info("configuration replace result: {}", ec.message());
                    return !ec
                           || ec
                                == raft::errc::configuration_change_in_progress;
                });
          }).get();

    BOOST_REQUIRE(res);

    res = retry_with_leader(gr, 5, 5s, [new_members](raft_node& leader) {
              return leader.consensus
                ->cancel_configuration_change(model::revision_id(0))
                .then([](std::error_code ec) {
                    info("configuration revert result: {}", ec.message());
                    return !ec;
                });
          }).get();

    wait_for(
      5s,
      [&gr]() {
          auto leader_id = gr.get_leader_id();
          if (!leader_id) {
              return false;
          }
          return gr.get_member(*leader_id).consensus->config().get_state()
                 == raft::configuration_state::simple;
      },
      "new nodes are up to date");

    auto new_leader_id = gr.get_leader_id();
    if (new_leader_id) {
        auto& new_leader = gr.get_member(*new_leader_id);
        BOOST_REQUIRE_EQUAL(
          new_leader.consensus->config().all_nodes().size(), 3);
    }
    validate_offset_translation(gr);
}
