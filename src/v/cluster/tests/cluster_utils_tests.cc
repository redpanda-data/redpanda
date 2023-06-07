// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/cluster_utils.h"
#include "cluster/members_table.h"
#include "cluster/persisted_stm.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "net/unresolved_address.h"
#include "storage/tests/kvstore_fixture.h"
#include "test_utils/fixture.h"
#include "utils/prefix_logger.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(test_has_local_replicas) {
    model::node_id id{2};

    std::vector<model::broker_shard> replicas_1{
      model::broker_shard{model::node_id(1), 2},
      model::broker_shard{model::node_id(2), 1},
      model::broker_shard{model::node_id(3), 0}};

    std::vector<model::broker_shard> replicas_2{
      model::broker_shard{model::node_id(4), 2},
      model::broker_shard{model::node_id(2), 0}, // local replica
    };

    BOOST_REQUIRE_EQUAL(cluster::has_local_replicas(id, replicas_1), false);
    BOOST_REQUIRE_EQUAL(cluster::has_local_replicas(id, replicas_2), true);
}

SEASTAR_THREAD_TEST_CASE(test_check_result_configuration) {
    cluster::members_table::cache_t members;

    model::broker b_0(
      model::node_id(0),
      {model::broker_endpoint(
         "internal", net::unresolved_address("localhost", 9092)),
       model::broker_endpoint(
         "external", net::unresolved_address("192.168.1.10", 19092))},
      net::unresolved_address("localhost", 31345),
      std::nullopt,
      model::broker_properties{.cores = 10});

    model::broker b_1(
      model::node_id(1),
      {model::broker_endpoint(
         "internal", net::unresolved_address("localhost", 9093)),
       model::broker_endpoint(
         "external", net::unresolved_address("192.168.1.11", 19093))},
      net::unresolved_address("localhost", 31346),
      std::nullopt,
      model::broker_properties{.cores = 10});

    model::broker b_2(
      model::node_id(2),
      {model::broker_endpoint(
         "internal", net::unresolved_address("localhost", 9094)),
       model::broker_endpoint(
         "external", net::unresolved_address("192.168.1.12", 19094))},
      net::unresolved_address("localhost", 31347),
      std::nullopt,
      model::broker_properties{.cores = 10});

    members.emplace(
      model::node_id(0),
      cluster::node_metadata{.broker = b_0, .state = cluster::broker_state{}});
    members.emplace(
      model::node_id(1),
      cluster::node_metadata{.broker = b_1, .state = cluster::broker_state{}});
    members.emplace(
      model::node_id(2),
      cluster::node_metadata{.broker = b_2, .state = cluster::broker_state{}});

    // try updating broker with exactly the same configuration
    auto error = cluster::check_result_configuration(members, b_0);
    BOOST_REQUIRE(!error);

    // change rpc listener address to different value

    model::broker b_1_changed_rpc(
      model::node_id(1),
      {model::broker_endpoint(
         "internal", net::unresolved_address("localhost", 9093)),
       model::broker_endpoint(
         "external", net::unresolved_address("192.168.1.11", 19093))},
      net::unresolved_address("localhost", 41346),
      std::nullopt,
      model::broker_properties{.cores = 10});

    error = cluster::check_result_configuration(members, b_1_changed_rpc);
    BOOST_REQUIRE(!error);

    model::broker b_1_changed_rpc_same_value(
      model::node_id(1),
      {model::broker_endpoint(
         "internal", net::unresolved_address("localhost", 9092)),
       model::broker_endpoint(
         "external", net::unresolved_address("192.168.1.11", 19093))},
      net::unresolved_address("localhost", 31345),
      std::nullopt,
      model::broker_properties{.cores = 10});

    error = cluster::check_result_configuration(
      members, b_1_changed_rpc_same_value);
    BOOST_REQUIRE(error);

    model::broker b_1_changed_listener(
      model::node_id(1),
      {model::broker_endpoint(
         "internal", net::unresolved_address("localhost", 9093)),
       model::broker_endpoint(
         "external", net::unresolved_address("192.168.1.11", 29093))},
      net::unresolved_address("localhost", 31346),
      std::nullopt,
      model::broker_properties{.cores = 10});

    error = cluster::check_result_configuration(members, b_1_changed_listener);
    BOOST_REQUIRE(!error);

    model::broker b_1_changed_listener_same_value(
      model::node_id(1),
      {model::broker_endpoint(
         "internal", net::unresolved_address("localhost", 9093)),
       model::broker_endpoint(
         "external", net::unresolved_address("192.168.1.10", 19092))},
      net::unresolved_address("localhost", 31346),
      std::nullopt,
      model::broker_properties{.cores = 10});

    error = cluster::check_result_configuration(
      members, b_1_changed_listener_same_value);
    BOOST_REQUIRE(error);
    model::broker b_1_add_listener(
      model::node_id(1),
      {model::broker_endpoint(
         "internal", net::unresolved_address("localhost", 9093)),
       model::broker_endpoint(
         "external", net::unresolved_address("192.168.1.10", 19093)),
       model::broker_endpoint(
         "new", net::unresolved_address("192.168.2.10", 29092))},
      net::unresolved_address("localhost", 31346),
      std::nullopt,
      model::broker_properties{.cores = 10});

    error = cluster::check_result_configuration(members, b_1_add_listener);
    BOOST_REQUIRE(!error);

    model::broker b_1_add_listener_the_same(
      model::node_id(1),
      {model::broker_endpoint(
         "internal", net::unresolved_address("localhost", 9093)),
       model::broker_endpoint(
         "external", net::unresolved_address("192.168.1.10", 19092)),
       model::broker_endpoint(
         "new", net::unresolved_address("192.168.1.11", 19092))},
      net::unresolved_address("localhost", 31346),
      std::nullopt,
      model::broker_properties{.cores = 10});

    error = cluster::check_result_configuration(
      members, b_1_add_listener_the_same);

    BOOST_REQUIRE(error);
}

FIXTURE_TEST(persisted_stm_kvstore_test, kvstore_test_fixture) {
    ss::logger test_logger("test_logger");
    auto kvstore = make_kvstore();
    kvstore->start().get();
    auto make_snapshotter = [&kvstore, &test_logger]() {
        auto ntp = model::ntp(
          model::kafka_namespace,
          model::topic(random_generators::gen_alphanum_string(10)),
          model::partition_id(0));
        prefix_logger logger(
          test_logger, ssx::sformat("[{} ({})]", ntp, "test.snapshot"));
        return cluster::kvstore_backed_stm_snapshot(
          "test.snapshot", logger, ntp, *(kvstore.get()));
    };

    /// Make multiple snapshots to ensure keyspaces are not overlapping
    std::vector<cluster::kvstore_backed_stm_snapshot> snapshotters;
    snapshotters.reserve(10);
    for (auto i = 0; i < 10; ++i) {
        snapshotters.push_back(make_snapshotter());
    }
    /// Make some snapshots, and retrieve from disk asserting validity
    std::vector<cluster::stm_snapshot> copies;
    for (auto i = 0; i < 10; ++i) {
        cluster::stm_snapshot ss;
        ss.header = cluster::stm_snapshot_header{
          .version = cluster::stm_snapshot_version,
          .offset = model::offset(random_generators::get_int(256))};
        ss.data = random_generators::make_iobuf(
          random_generators::get_int(256));
        ss.header.snapshot_size = ss.data.size_bytes();
        copies.push_back(
          cluster::stm_snapshot{.header = ss.header, .data = ss.data.copy()});
        snapshotters[i].persist_snapshot(std::move(ss)).get();
    }
    /// Read data from snapshot, asserting all data matches stored copy
    for (auto i = 0; i < 10; ++i) {
        auto snapshot = snapshotters[i].load_snapshot().get();
        auto& copied_snapshot = copies[i];
        BOOST_CHECK(snapshot);
        BOOST_CHECK_EQUAL(
          snapshot->header.offset, copied_snapshot.header.offset);
        BOOST_CHECK_EQUAL(
          snapshot->header.version, copied_snapshot.header.version);
        BOOST_CHECK_EQUAL(
          snapshot->header.snapshot_size, copied_snapshot.header.snapshot_size);
        BOOST_CHECK_EQUAL(snapshot->data, copied_snapshot.data);
    }
    /// Finally, clear all snapshots assert they do not exist
    for (auto i = 0; i < 10; ++i) {
        snapshotters[i].remove_persistent_state().get();
        auto snapshot = snapshotters[i].load_snapshot().get();
        BOOST_CHECK(!snapshot);
    }
    kvstore->stop().get();
}
