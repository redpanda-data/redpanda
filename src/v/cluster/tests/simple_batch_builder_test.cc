#include "cluster/simple_batch_builder.h"
#include "cluster/tests/batch_utils.h"
#include "rpc/serialize.h"

#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

cluster::partition_assignment create_test_assignment(uint32_t p) {
    return cluster::partition_assignment{
      .shard = 2,
      .group = raft::group_id(p),
      .ntp = model::ntp{.ns = model::ns("test"),
                        .tp = {model::topic("a_topic"),
                               model::partition_id(p)}},
      .broker = model::broker(
        model::node_id(1), "localhost", 9092, std::nullopt)};
}

SEASTAR_THREAD_TEST_CASE(simple_batch_builder_batch_test) {
    auto pa_key = cluster::log_record_key{
      cluster::log_record_key::type::partition_assignment};
    auto batch = std::move(
                   cluster::simple_batch_builder(model::record_batch_type(3))
                     .add_kv(
                       cluster::log_record_key{
                         cluster::log_record_key::type::topic_configuration},
                       cluster::topic_configuration(
                         model::ns("test"), model::topic{"a_topic"}, 3, 1))
                     .add_kv(pa_key, create_test_assignment(0))
                     .add_kv(pa_key, create_test_assignment(1))
                     .add_kv(pa_key, create_test_assignment(2)))
                   .build();

    BOOST_REQUIRE_EQUAL(batch.size(), 4);
    BOOST_REQUIRE_EQUAL(batch.last_offset_delta(), 3);
    BOOST_REQUIRE_EQUAL(batch.size_bytes(), 106);

    BOOST_REQUIRE_EQUAL(batch.crc(), checksum_batch(batch));
}