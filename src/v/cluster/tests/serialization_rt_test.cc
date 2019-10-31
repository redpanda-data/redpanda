#include "cluster/types.h"
#include "test_utils/rpc.h"

#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(topic_config_rt_test) {
    cluster::topic_configuration cfg(
      model::ns("test"), model::topic{"a_topic"}, 3, 1);

    cfg.compaction = model::topic_partition::compaction::yes;
    cfg.compression = model::compression::snappy;
    cfg.retention_bytes = 4;
    using namespace std::chrono_literals;
    cfg.retention = 10h;
    cfg.retention_bytes = (1 >> 30);
    auto d = serialize_roundtrip_rpc(std::move(cfg)).get0();

    BOOST_REQUIRE_EQUAL(model::ns("test"), d.ns);
    BOOST_REQUIRE_EQUAL(model::topic("a_topic"), d.topic);
    BOOST_REQUIRE_EQUAL(3, d.partition_count);
    BOOST_REQUIRE_EQUAL(1, d.replication_factor);
    BOOST_REQUIRE_EQUAL(model::topic_partition::compaction::yes, d.compaction);
    BOOST_REQUIRE_EQUAL(model::compression::snappy, d.compression);
    BOOST_CHECK(10h == d.retention);
    BOOST_REQUIRE_EQUAL((1 >> 30), d.retention_bytes);
}

SEASTAR_THREAD_TEST_CASE(broker_metadata_rt_test) {
    model::broker b(model::node_id(0), "localhost", 9092, "test");
    auto d = serialize_roundtrip_rpc(std::move(b)).get0();

    BOOST_REQUIRE_EQUAL(d.id(), model::node_id(0));
    BOOST_REQUIRE_EQUAL(d.host(), "localhost");
    BOOST_REQUIRE_EQUAL(d.port(), 9092);
    BOOST_CHECK(d.rack() == std::optional<sstring>("test"));
}

SEASTAR_THREAD_TEST_CASE(partition_assignment_rt_test) {
    model::ntp test_ntp{model::ns("test"),
                        {
                          .topic = model::topic("topic"),
                          .partition = model::partition_id(3),
                        }};
    cluster::partition_assignment p_as{
      .group = raft::group_id(2),
      .ntp = test_ntp,
      .replicas = {{.node_id = model::node_id(0), .shard = 1}}};

    auto d = serialize_roundtrip_rpc(std::move(p_as)).get0();

    BOOST_REQUIRE_EQUAL(d.group, raft::group_id(2));
    BOOST_REQUIRE_EQUAL(d.ntp, test_ntp);
    BOOST_REQUIRE_EQUAL(d.replicas.size(), 1);
    BOOST_REQUIRE_EQUAL(d.replicas[0].node_id(), 0);
    BOOST_REQUIRE_EQUAL(d.replicas[0].shard, 1);
}
