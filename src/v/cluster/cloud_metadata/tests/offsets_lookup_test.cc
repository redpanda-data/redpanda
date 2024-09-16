/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage/tests/s3_imposter.h"
#include "cluster/cloud_metadata/offsets_lookup.h"
#include "cluster/types.h"
#include "random/generators.h"
#include "redpanda/tests/fixture.h"

#include <seastar/core/scheduling.hh>
#include <seastar/core/smp.hh>
#include <seastar/util/log.hh>

namespace {
ss::logger logger("offsets_lookup_test");
static ss::abort_source never_abort;
auto topic_name = model::topic("tapioca");
auto ntp = model::ntp(model::kafka_namespace, topic_name, 0);
} // anonymous namespace

using cluster::cloud_metadata::offsets_lookup;
using cluster::cloud_metadata::offsets_lookup_request;

class offsets_lookup_fixture
  : public s3_imposter_fixture
  , public redpanda_thread_fixture
  , public enable_cloud_storage_fixture {
public:
    offsets_lookup_fixture()
      : redpanda_thread_fixture(
          redpanda_thread_fixture::init_cloud_storage_tag{},
          httpd_port_number()) {
        set_expectations_and_listen({});
        wait_for_controller_leadership().get();
        RPTEST_REQUIRE_EVENTUALLY(5s, [this] {
            return app.storage.local().get_cluster_uuid().has_value();
        });
        _node_id = config::node().node_id().value();

        cluster::topic_properties props;
        props.shadow_indexing = model::shadow_indexing_mode::full;
        props.retention_local_target_bytes = tristate<size_t>(1);
        props.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::deletion;
        add_topic({model::kafka_namespace, topic_name}, 1, props).get();
        wait_for_leader(ntp).get();
        _offsets_server = &app.offsets_lookup.local();
    }

    rpc::transport_configuration client_config() const {
        return rpc::transport_configuration{
          .server_addr = config::node().advertised_rpc_api(),
          .credentials = nullptr};
    }

    model::node_id _node_id;
    offsets_lookup* _offsets_server;
};

FIXTURE_TEST(test_list_offsets_basic, offsets_lookup_fixture) {
    offsets_lookup_request req;
    req.node_id = _node_id;
    auto n = random_generators::get_int(1, 10);
    for (int i = 0; i < n; i++) {
        req.ntps.emplace_back(ntp);
    }
    auto reply = _offsets_server->lookup(std::move(req)).get();
    BOOST_REQUIRE_EQUAL(reply.node_id, _node_id);
    BOOST_REQUIRE_EQUAL(n, reply.ntp_and_offset.size());
    BOOST_REQUIRE_EQUAL(kafka::offset{0}, reply.ntp_and_offset[0].offset);
}

FIXTURE_TEST(test_list_offsets_empty_req, offsets_lookup_fixture) {
    offsets_lookup_request req;
    req.node_id = _node_id;
    auto reply = _offsets_server->lookup(std::move(req)).get();
    BOOST_REQUIRE_EQUAL(reply.node_id, _node_id);
    BOOST_REQUIRE_EQUAL(0, reply.ntp_and_offset.size());
}

FIXTURE_TEST(test_list_offsets_missing_topic, offsets_lookup_fixture) {
    offsets_lookup_request req;
    req.node_id = _node_id;
    auto bad_topic_ntp = ntp;
    bad_topic_ntp.tp.topic = model::topic{"missing"};
    auto n = random_generators::get_int(1, 10);
    for (int i = 0; i < n; i++) {
        req.ntps.emplace_back(bad_topic_ntp);
    }

    auto reply = _offsets_server->lookup(std::move(req)).get();
    BOOST_REQUIRE_EQUAL(reply.node_id, _node_id);
    BOOST_REQUIRE_EQUAL(0, reply.ntp_and_offset.size());
}

FIXTURE_TEST(test_list_offsets_missing_partition, offsets_lookup_fixture) {
    offsets_lookup_request req;
    req.node_id = _node_id;
    auto bad_partition_ntp = ntp;
    bad_partition_ntp.tp.partition = model::partition_id(999);
    auto n = random_generators::get_int(1, 10);
    for (int i = 0; i < n; i++) {
        req.ntps.emplace_back(bad_partition_ntp);
    }

    auto reply = _offsets_server->lookup(std::move(req)).get();
    BOOST_REQUIRE_EQUAL(reply.node_id, _node_id);
    BOOST_REQUIRE_EQUAL(0, reply.ntp_and_offset.size());
}

FIXTURE_TEST(test_list_offsets_bad_node_id, offsets_lookup_fixture) {
    offsets_lookup_request req;
    auto bad_node_id = model::node_id{_node_id() + 1};
    req.node_id = bad_node_id;
    auto n = random_generators::get_int(1, 10);
    for (int i = 0; i < n; i++) {
        req.ntps.emplace_back(ntp);
    }

    auto reply = _offsets_server->lookup(std::move(req)).get();
    BOOST_REQUIRE_EQUAL(reply.node_id, _node_id());
    BOOST_REQUIRE_EQUAL(0, reply.ntp_and_offset.size());
}
