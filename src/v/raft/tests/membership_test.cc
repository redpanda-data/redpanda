#include "model/metadata.h"
#include "raft/tests/raft_group_fixture.h"

FIXTURE_TEST(add_one_node_to_single_node_cluster, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 1);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;
    auto res = leader_raft->replicate(random_batches_entry(1)).get0();
    auto new_node = gr.create_new_node(model::node_id(2));
    leader_raft->add_group_member(new_node).get0();
    validate_logs_replication(gr);

    BOOST_REQUIRE_EQUAL(leader_raft->config().nodes.size(), 2);
};

FIXTURE_TEST(
  add_two_nodes_one_after_another_to_the_cluster, raft_test_fixture) {
    raft_group gr = raft_group(raft::group_id(0), 1);
    gr.enable_all();
    auto leader_id = wait_for_group_leader(gr);
    auto leader_raft = gr.get_member(leader_id).consensus;
    auto res = leader_raft->replicate(random_batches_entry(1)).get0();
    auto new_node_1 = gr.create_new_node(model::node_id(2));
    auto new_node_2 = gr.create_new_node(model::node_id(3));
    leader_raft->add_group_member(new_node_1).get0();
    leader_raft->add_group_member(new_node_2).get0();

    validate_logs_replication(gr);
    BOOST_REQUIRE_EQUAL(leader_raft->config().nodes.size(), 3);
};
