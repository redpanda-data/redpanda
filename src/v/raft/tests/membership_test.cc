#include "model/metadata.h"
#include "raft/tests/raft_group_fixture.h"
#include "test_utils/async.h"

FIXTURE_TEST(add_one_node_to_single_node_cluster, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 1);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;
    auto res = leader_raft
                 ->replicate(random_batches_entry(1), default_replicate_opts)
                 .get0();
    auto new_node = gr.create_new_node(model::node_id(2));
    leader_raft->add_group_members({new_node}).get0();
    validate_logs_replication(gr);

    BOOST_REQUIRE_EQUAL(leader_raft->config().brokers().size(), 2);
};

FIXTURE_TEST(add_two_nodes_to_the_cluster, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 1);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;
    auto res = leader_raft
                 ->replicate(random_batches_entry(1), default_replicate_opts)
                 .get0();
    auto new_node_1 = gr.create_new_node(model::node_id(2));
    auto new_node_2 = gr.create_new_node(model::node_id(3));
    leader_raft->add_group_members({new_node_1, new_node_2}).get0();

    validate_logs_replication(gr);
    BOOST_REQUIRE_EQUAL(leader_raft->config().brokers().size(), 3);
};

void verify_node_is_behind(raft_group& gr, model::node_id removed) {
    auto leader_id = gr.get_leader_id();
    BOOST_REQUIRE(leader_id.has_value());
    tests::cooperative_spin_wait_with_timeout(
      3s,
      [&gr, removed, lid = *leader_id] {
          return ss::async([&gr, removed, lid] {
              auto logs = gr.read_all_logs();
              auto leader_offset = logs[lid].back().last_offset();

              // removed node is behind
              if (leader_offset <= logs[removed].back().last_offset()) {
                  return false;
              }
              // other nodes have all the data
              logs.erase(removed);
              return are_logs_the_same_length(logs);
          });
      })
      .get0();
}

FIXTURE_TEST(remove_non_leader, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;
    auto res = leader_raft
                 ->replicate(random_batches_entry(1), default_replicate_opts)
                 .get0();
    auto& members = gr.get_members();
    auto non_leader_id = std::find_if(
                           members.begin(),
                           members.end(),
                           [](raft_group::members_t::value_type& p) {
                               return !p.second.consensus->is_leader();
                           })
                           ->first;

    leader_raft->remove_members({non_leader_id}).get0();
    res = leader_raft
            ->replicate(random_batches_entry(1), default_replicate_opts)
            .get0();

    BOOST_REQUIRE_EQUAL(leader_raft->config().brokers().size(), 2);
    verify_node_is_behind(gr, non_leader_id);
}

FIXTURE_TEST(remove_current_leader, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto old_leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(old_leader_id).consensus;
    auto res = leader_raft
                 ->replicate(random_batches_entry(1), default_replicate_opts)
                 .get0();
    leader_raft->remove_members({old_leader_id}).get0();
    auto new_leader_id = wait_for_group_leader(gr);
    leader_raft = gr.get_member(new_leader_id).consensus;
    BOOST_REQUIRE_EQUAL(leader_raft->config().brokers().size(), 2);
    BOOST_REQUIRE_NE(new_leader_id, old_leader_id);
    res = leader_raft
            ->replicate(random_batches_entry(1), default_replicate_opts)
            .get0();

    verify_node_is_behind(gr, old_leader_id);
}

FIXTURE_TEST(remove_multiple_members, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto old_leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(old_leader_id).consensus;
    auto res = leader_raft
                 ->replicate(random_batches_entry(1), default_replicate_opts)
                 .get0();
    auto& members = gr.get_members();
    auto non_leader_id = std::find_if(
                           members.begin(),
                           members.end(),
                           [](raft_group::members_t::value_type& p) {
                               return !p.second.consensus->is_leader();
                           })
                           ->first;
    // remove two members
    leader_raft->remove_members({old_leader_id, non_leader_id}).get0();
    auto new_leader_id = wait_for_group_leader(gr);
    leader_raft = gr.get_member(new_leader_id).consensus;
    BOOST_REQUIRE_EQUAL(leader_raft->config().brokers().size(), 1);
    BOOST_REQUIRE_NE(new_leader_id, old_leader_id);
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

FIXTURE_TEST(replace_whole_group, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 3);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;
    auto res = leader_raft
                 ->replicate(random_batches_entry(5), default_replicate_opts)
                 .get0();
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
    while (!success) {
        auto leader = gr.get_leader_id();
        if (!leader) {
            continue;
        }
        auto errc = gr.get_member(*leader)
                      .consensus->replace_configuration(std::move(new_members))
                      .get0();
        if (!errc) {
            success = true;
        }
    }
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

    auto new_leader_id = wait_for_group_leader(gr);
    auto& new_leader = gr.get_member(new_leader_id);

    BOOST_REQUIRE_GT(new_leader_id, model::node_id(4));
    BOOST_REQUIRE_EQUAL(new_leader.consensus->config().brokers().size(), 3);
}
