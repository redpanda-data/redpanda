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
#include "cluster/types.h"
#include "model/metadata.h"
#include "net/unresolved_address.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/testing/thread_test_case.hh>

cluster::broker_ptr test_broker(int32_t id) {
    return ss::make_lw_shared<model::broker>(
      model::node_id{id},
      net::unresolved_address("127.0.0.1", 9092),
      net::unresolved_address("127.0.0.1", 1234),
      std::nullopt,
      model::broker_properties{.cores = 32});
}

SEASTAR_THREAD_TEST_CASE(test_group_cfg_difference) {
    auto broker_1 = test_broker(1); // intact
    auto broker_2 = test_broker(2); // additions
    auto broker_3 = test_broker(3); // deletions
    auto broker_4 = test_broker(4); // new
    auto broker_5 = test_broker(5); // additions

    cluster::broker_ptr broker_2_additions = ss::make_lw_shared<model::broker>(
      model::node_id{2},
      net::unresolved_address("127.0.0.1", 9092),
      net::unresolved_address("172.168.1.1", 1234),
      std::nullopt,
      model::broker_properties{.cores = 32});

    cluster::broker_ptr broker_5_additions = ss::make_lw_shared<model::broker>(
      model::node_id{5},
      net::unresolved_address("127.0.0.1", 9092),
      net::unresolved_address("127.0.0.1", 6060),
      std::nullopt,
      model::broker_properties{.cores = 32});

    auto diff = cluster::calculate_changed_brokers(
      {broker_1, broker_2_additions, broker_4, broker_5_additions},
      {broker_1, broker_2, broker_3, broker_5});

    BOOST_REQUIRE_EQUAL(diff.deletions.size(), 1);
    BOOST_REQUIRE_EQUAL(diff.deletions[0]->id(), model::node_id(3));
    BOOST_REQUIRE_EQUAL(diff.additions.size(), 3);
    BOOST_REQUIRE_EQUAL(diff.additions[0]->id(), model::node_id(2));
    BOOST_REQUIRE_EQUAL(diff.additions[1]->id(), model::node_id(4));
    BOOST_REQUIRE_EQUAL(diff.additions[2]->id(), model::node_id(5));
    BOOST_REQUIRE_EQUAL(diff.additions[0]->rpc_address().host(), "172.168.1.1");
    BOOST_REQUIRE_EQUAL(diff.additions[2]->rpc_address().port(), 6060);
}

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
    cluster::members_table::broker_cache_t members;

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

    members.emplace(model::node_id(0), ss::make_lw_shared<model::broker>(b_0));
    members.emplace(model::node_id(1), ss::make_lw_shared<model::broker>(b_1));
    members.emplace(model::node_id(2), ss::make_lw_shared<model::broker>(b_2));

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
