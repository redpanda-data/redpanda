// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/metadata.h"
#include "raft/errc.h"
#include "raft/tests/raft_group_fixture.h"
#include "test_utils/async.h"

FIXTURE_TEST(add_one_node_to_single_node_cluster, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 1);
    gr.enable_all();
    auto res = replicate_random_batches(gr, 1).get0();
    BOOST_REQUIRE(res);
    auto new_node = gr.create_new_node(model::node_id(2));
    res = retry_with_leader(gr, 5, 1s, [new_node](raft_node& leader) {
              return leader.consensus->add_group_members({new_node})
                .then([](std::error_code ec) { return !ec; });
          }).get0();

    validate_logs_replication(gr);
    auto& leader = gr.get_member(gr.get_leader_id().value());
    BOOST_REQUIRE_EQUAL(leader.consensus->config().brokers().size(), 2);
};

FIXTURE_TEST(add_two_nodes_to_the_cluster, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 1);
    gr.enable_all();
    auto res = replicate_random_batches(gr, 1).get0();
    BOOST_REQUIRE(res);
    auto new_node_1 = gr.create_new_node(model::node_id(2));
    auto new_node_2 = gr.create_new_node(model::node_id(3));
    res = retry_with_leader(
            gr,
            5,
            1s,
            [new_node_1, new_node_2](raft_node& leader) {
                return leader.consensus
                  ->add_group_members({new_node_1, new_node_2})
                  .then([](std::error_code ec) { return !ec; });
            })
            .get0();

    validate_logs_replication(gr);
    auto& leader = gr.get_member(gr.get_leader_id().value());
    BOOST_REQUIRE_EQUAL(leader.consensus->config().brokers().size(), 3);
};

void verify_node_is_behind(raft_group& gr, model::node_id removed) {
    tests::cooperative_spin_wait_with_timeout(3s, [&gr, removed] {
        return ss::async([&gr, removed] {
            auto leader_id = gr.get_leader_id();
            if (!leader_id) {
                return false;
            }
            auto logs = gr.read_all_logs();
            auto& leader_log = logs[*leader_id];
            if (leader_log.empty()) {
                return false;
            }
            auto leader_offset = leader_log.back().last_offset();

            auto& removed_log = logs[removed];

            // removed node is behind
            if (
              !removed_log.empty()
              && leader_offset <= logs[removed].back().last_offset()) {
                return false;
            }

            // other nodes have all the data
            logs.erase(removed);
            return are_logs_the_same_length(logs);
        });
    }).get0();
}

FIXTURE_TEST(remove_non_leader, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto res = replicate_random_batches(gr, 2).get0();
    auto& members = gr.get_members();
    auto non_leader_id = std::find_if(
                           members.begin(),
                           members.end(),
                           [](raft_group::members_t::value_type& p) {
                               return !p.second.consensus->is_leader();
                           })
                           ->first;
    res = retry_with_leader(gr, 5, 1s, [non_leader_id](raft_node& leader) {
              return leader.consensus->remove_members({non_leader_id})
                .then([](std::error_code ec) { return !ec; });
          }).get0();
    BOOST_REQUIRE(res);
    tests::cooperative_spin_wait_with_timeout(5s, [&gr] {
        auto leader_id = gr.get_leader_id();
        if (!leader_id) {
            return false;
        }
        auto& leader = gr.get_member(*leader_id);
        return leader.consensus->config().brokers().size() == 2;
    }).get0();

    verify_node_is_behind(gr, non_leader_id);
}

FIXTURE_TEST(remove_current_leader, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto res = replicate_random_batches(gr, 2).get0();
    auto old_leader_id = wait_for_group_leader(gr);
    res = retry_with_leader(gr, 5, 1s, [old_leader_id](raft_node& leader) {
              return leader.consensus->remove_members({old_leader_id})
                .then([](std::error_code ec) { return !ec; });
          }).get0();

    tests::cooperative_spin_wait_with_timeout(5s, [&gr, old_leader_id] {
        auto leader_id = gr.get_leader_id();
        if (!leader_id) {
            return false;
        }
        auto& leader = gr.get_member(*leader_id);
        return leader.consensus->config().brokers().size() == 2
               && leader_id != old_leader_id;
    }).get0();

    BOOST_REQUIRE_NE(gr.get_leader_id(), old_leader_id);
    res = replicate_random_batches(gr, 2).get0();

    verify_node_is_behind(gr, old_leader_id);
}

FIXTURE_TEST(remove_multiple_members, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto res = replicate_random_batches(gr, 2).get0();
    auto old_leader_id = wait_for_group_leader(gr);
    auto& members = gr.get_members();
    auto non_leader_id = std::find_if(
                           members.begin(),
                           members.end(),
                           [](raft_group::members_t::value_type& p) {
                               return !p.second.consensus->is_leader();
                           })
                           ->first;
    res = retry_with_leader(
            gr,
            5,
            1s,
            [old_leader_id, non_leader_id](raft_node& leader) {
                return leader.consensus
                  ->remove_members({old_leader_id, non_leader_id})
                  .then([](std::error_code ec) { return !ec; });
            })
            .get0();

    tests::cooperative_spin_wait_with_timeout(2s, [&gr, old_leader_id] {
        auto leader_id = gr.get_leader_id();
        if (!leader_id) {
            return false;
        }
        auto& leader = gr.get_member(*leader_id);
        return leader.consensus->config().brokers().size() == 1
               && leader_id != old_leader_id;
    }).get0();

    res = replicate_random_batches(gr, 2).get0();
    BOOST_REQUIRE(res);
}

FIXTURE_TEST(try_remove_all_voters, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;
    // remove all members
    auto result = leader_raft
                    ->remove_members(
                      {model::node_id(0), model::node_id(1), model::node_id(2)})
                    .get0();

    BOOST_REQUIRE_EQUAL(result, raft::errc::invalid_configuration_update);
}

// TODO: Fix failing test. For more details see: #342
#if 0
FIXTURE_TEST(replace_whole_group, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    info("replicating some batches");
    auto res = replicate_random_batches(gr, 5).get0();
    // all nodes are replaced
    gr.create_new_node(model::node_id(5));
    gr.create_new_node(model::node_id(6));
    gr.create_new_node(model::node_id(7));
    std::vector<model::broker> new_members;
    new_members.reserve(3);
    new_members.push_back(gr.get_member(model::node_id(5)).broker);
    new_members.push_back(gr.get_member(model::node_id(6)).broker);
    new_members.push_back(gr.get_member(model::node_id(7)).broker);
    bool success = false;
    info("replacing configuration");
    res = retry_with_leader(gr, 5, 5s, [new_members](raft_node& leader) {
              return leader.consensus->replace_configuration(new_members)
                .then([](std::error_code ec) {
                    info("configuration replace result: {}", ec.message());
                    return !ec
                           || ec
                                == raft::errc::configuration_change_in_progress;
                });
          }).get0();
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
          if (last_new <= last_old) {
              return false;
          }
          return std::all_of(
            new_nodes_log.begin(), new_nodes_log.end(), [last_new](v_t& p) {
                return last_new == p.second.back().last_offset();
            });
      },
      "new nodes are up to date");

    info("waiting for new group leader");
    auto new_leader_id = wait_for_group_leader(gr);
    auto& new_leader = gr.get_member(new_leader_id);

    BOOST_REQUIRE_GT(new_leader_id, model::node_id(4));
    BOOST_REQUIRE_EQUAL(new_leader.consensus->config().brokers().size(), 3);
}
#endif
