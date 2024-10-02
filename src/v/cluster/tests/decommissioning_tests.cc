
#include "cluster/tests/rebalancing_tests_fixture.h"

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>
#include <boost/test/tools/old/interface.hpp>

FIXTURE_TEST(test_single_node_decomissioning, rebalancing_tests_fixture) {
    start_cluster(3);
    create_topic(create_topic_cfg("test-1", 3, 1));
    create_topic(create_topic_cfg("test-2", 4, 3));
    create_topic(create_topic_cfg("test-3", 1, 3));
    create_topic(create_topic_cfg("test-4", 2, 3));
    populate_all_topics_with_data();
    // decomission single-node
    auto res = (*get_leader_node_application())
                 ->controller->get_members_frontend()
                 .local()
                 .decommission_node(model::node_id(0))
                 .get();
    // we expect that decommissioning will be in progress since we haven't
    // yet added any new nodes
    BOOST_REQUIRE(!res);

    add_node(10);
    wait_for_node_decommissioned(0).get();
}

// TODO: enable when after we investigate issues on aarch_64
#if 0
FIXTURE_TEST(test_two_nodes_decomissioning, rebalancing_tests_fixture) {
    create_topic(create_topic_cfg("test-1", 3, 1));
    create_topic(create_topic_cfg("test-2", 4, 3));
    create_topic(create_topic_cfg("test-3", 1, 3));
    create_topic(create_topic_cfg("test-4", 2, 3));
    populate_all_topics_with_data();
    // decomission single-node
    info("decommissioning node - 0");
    auto res_1 = (*get_leader_node_application())
                   ->controller->get_members_frontend()
                   .local()
                   .decommission_node(model::node_id(0))
                   .get();
    BOOST_REQUIRE(!res_1);
    info("decommissioning node - 1");
    auto res_2 = (*get_leader_node_application())
                   ->controller->get_members_frontend()
                   .local()
                   .decommission_node(model::node_id(1))
                   .get();
    BOOST_REQUIRE(!res_2);

    add_node(10);
    add_node(11);
    wait_for_node_decommissioned(0).get();
    wait_for_node_decommissioned(1).get();
}
#endif
