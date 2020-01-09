#include "cluster/cluster_utils.h"
#include "cluster/types.h"
#include "model/metadata.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/testing/thread_test_case.hh>

cluster::broker_ptr test_broker(int32_t id) {
    return ss::make_lw_shared<model::broker>(
      model::node_id{id},
      unresolved_address("127.0.0.1", 9092),
      unresolved_address("127.0.0.1", 1234),
      std::nullopt,
      model::broker_properties{.cores = 32});
}

SEASTAR_THREAD_TEST_CASE(test_group_cfg_difference) {
    auto broker_1 = test_broker(1); // intact
    auto broker_2 = test_broker(2); // updated
    auto broker_3 = test_broker(3); // removed
    auto broker_4 = test_broker(4); // new
    auto broker_5 = test_broker(5); // updated

    cluster::broker_ptr broker_2_updated = ss::make_lw_shared<model::broker>(
      model::node_id{2},
      unresolved_address("127.0.0.1", 9092),
      unresolved_address("172.168.1.1", 1234),
      std::nullopt,
      model::broker_properties{.cores = 32});

    cluster::broker_ptr broker_5_updated = ss::make_lw_shared<model::broker>(
      model::node_id{5},
      unresolved_address("127.0.0.1", 9092),
      unresolved_address("127.0.0.1", 6060),
      std::nullopt,
      model::broker_properties{.cores = 32});

    auto diff = cluster::calculate_changed_brokers(
      {broker_1, broker_2_updated, broker_4, broker_5_updated},
      {broker_1, broker_2, broker_3, broker_5});

    BOOST_REQUIRE_EQUAL(diff.removed.size(), 1);
    BOOST_REQUIRE_EQUAL(diff.removed[0]->id(), model::node_id(3));
    BOOST_REQUIRE_EQUAL(diff.updated.size(), 3);
    BOOST_REQUIRE_EQUAL(diff.updated[0]->id(), model::node_id(2));
    BOOST_REQUIRE_EQUAL(diff.updated[1]->id(), model::node_id(4));
    BOOST_REQUIRE_EQUAL(diff.updated[2]->id(), model::node_id(5));
    BOOST_REQUIRE_EQUAL(diff.updated[0]->rpc_address().host(), "172.168.1.1");
    BOOST_REQUIRE_EQUAL(diff.updated[2]->rpc_address().port(), 6060);
}