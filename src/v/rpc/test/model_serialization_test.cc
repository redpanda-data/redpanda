#include "model/fundamental.h"
#include "model/metadata.h"
#include "rpc/models.h"
#include "test_utils/rpc.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/tools/old/interface.hpp>

SEASTAR_THREAD_TEST_CASE(topic_metadata_rt_test) {
    model::partition_metadata p_md0(model::partition_id(0));
    p_md0.leader_node = model::node_id(0);
    p_md0.replicas = {model::broker_shard{model::node_id(0), 1},
                      model::broker_shard{model::node_id(1), 2},
                      model::broker_shard{model::node_id(2), 3}};
    model::partition_metadata p_md1(model::partition_id(0));
    p_md1.leader_node = model::node_id(3);
    p_md1.replicas = {model::broker_shard{model::node_id(3), 4},
                      model::broker_shard{model::node_id(4), 5},
                      model::broker_shard{model::node_id(5), 6}};

    model::topic_metadata t_md(model::topic("topic_1"));
    t_md.partitions = {p_md0, p_md1};

    auto r = serialize_roundtrip_rpc(std::move(t_md));
    BOOST_REQUIRE_EQUAL(r.tp, model::topic("topic_1"));
    BOOST_REQUIRE_EQUAL(
      r.partitions[0].leader_node.value(), p_md0.leader_node.value());
    BOOST_REQUIRE_EQUAL(
      r.partitions[1].leader_node.value(), p_md1.leader_node.value());
    for (int i = 0; i < 3; i++) {
        BOOST_REQUIRE_EQUAL(
          r.partitions[0].replicas[i].node_id, p_md0.replicas[i].node_id);
        BOOST_REQUIRE_EQUAL(
          r.partitions[0].replicas[i].shard, p_md0.replicas[i].shard);
        BOOST_REQUIRE_EQUAL(
          r.partitions[1].replicas[i].node_id, p_md1.replicas[i].node_id);
        BOOST_REQUIRE_EQUAL(
          r.partitions[1].replicas[i].shard, p_md1.replicas[i].shard);
    }
}