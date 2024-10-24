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
#include "storage/tests/kvstore_fixture.h"
#include "test_utils/fixture.h"
#include "utils/prefix_logger.h"
#include "utils/unresolved_address.h"

#include <seastar/core/shared_ptr.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>
#include <seastar/testing/thread_test_case.hh>

SEASTAR_THREAD_TEST_CASE(test_find_shard_on_node) {
    model::node_id id{1};

    std::vector<model::broker_shard> replicas_1{
      model::broker_shard{model::node_id(1), 2},
      model::broker_shard{model::node_id(2), 1},
      model::broker_shard{model::node_id(3), 0}};

    std::vector<model::broker_shard> replicas_2{
      model::broker_shard{model::node_id(4), 2},
      model::broker_shard{model::node_id(2), 0}, // local replica
    };

    BOOST_REQUIRE(cluster::find_shard_on_node(replicas_1, id) == 2);
    BOOST_REQUIRE(cluster::find_shard_on_node(replicas_2, id) == std::nullopt);
}

SEASTAR_THREAD_TEST_CASE(test_placement_target_on_node) {
    raft::group_id group{111};

    cluster::replicas_t orig_replicas{
      model::broker_shard{model::node_id(1), 2},
      model::broker_shard{model::node_id(2), 1},
      model::broker_shard{model::node_id(3), 0}};

    cluster::partition_assignment orig_assignment(
      group, model::partition_id(23), orig_replicas);

    cluster::topic_table::partition_meta partition_meta{
      .replicas_revisions = {
        {model::node_id(1), model::revision_id{11}},
        {model::node_id(2), model::revision_id{22}},
        {model::node_id(3), model::revision_id{33}},
      },
      .last_update_finished_revision = model::revision_id{123},
    };

    // node_id, log_revision, shard_id
    using expected_list_t = std::vector<std::tuple<int32_t, int64_t, uint32_t>>;

    auto check = [group](
                   std::string_view case_id,
                   const cluster::topic_table::partition_replicas_view& rv,
                   expected_list_t expected_list) {
        for (auto [node_id, log_revision, shard_id] : expected_list) {
            cluster::shard_placement_target expected{
              group, model::revision_id(log_revision), shard_id};
            auto actual = cluster::placement_target_on_node(
              rv, model::node_id(node_id));
            BOOST_REQUIRE_MESSAGE(
              actual == expected,
              fmt::format(
                "case '{}': placement target mismatch for node {}, "
                "expected: {}, got: {}",
                case_id,
                node_id,
                expected,
                actual));
        }
    };

    {
        // case 1: no update
        cluster::topic_table::partition_replicas_view rv{
          .partition_meta = partition_meta,
          .assignment = orig_assignment,
          .update = nullptr};

        BOOST_REQUIRE(
          cluster::placement_target_on_node(rv, model::node_id(0))
          == std::nullopt);

        check(
          "no update",
          rv,
          {
            {1, 11, 2},
            {2, 22, 1},
            {3, 33, 0},
          });
    }

    cluster::replicas_t updated_replicas{
      model::broker_shard{model::node_id(1), 2},
      model::broker_shard{model::node_id(2), 3},
      model::broker_shard{model::node_id(4), 5}};

    auto updated_assignment = orig_assignment;
    updated_assignment.replicas = updated_replicas;

    {
        // case 2: in-progress update

        cluster::topic_table::in_progress_update update(
          orig_replicas,
          updated_replicas,
          cluster::reconfiguration_state::in_progress,
          model::revision_id(145),
          cluster::reconfiguration_policy::full_local_retention,
          nullptr);

        cluster::topic_table::partition_replicas_view rv{
          .partition_meta = partition_meta,
          .assignment = updated_assignment,
          .update = &update};

        BOOST_REQUIRE(
          cluster::placement_target_on_node(rv, model::node_id(0))
          == std::nullopt);

        check(
          "in-progress update",
          rv,
          {
            {1, 11, 2},
            {2, 22, 3},
            {3, 33, 0},
            {4, 145, 5},
          });
    }

    {
        // case 3: cancelled update

        cluster::topic_table::in_progress_update update(
          orig_replicas,
          updated_replicas,
          cluster::reconfiguration_state::in_progress,
          model::revision_id(145),
          cluster::reconfiguration_policy::full_local_retention,
          nullptr);
        update.set_state(
          cluster::reconfiguration_state::cancelled, model::revision_id(167));

        cluster::topic_table::partition_replicas_view rv{
          .partition_meta = partition_meta,
          .assignment
          = orig_assignment, // orig_assignment reflects the cancelled update
          .update = &update};

        BOOST_REQUIRE(
          cluster::placement_target_on_node(rv, model::node_id(0))
          == std::nullopt);

        check(
          "cancelled update",
          rv,
          {
            {1, 11, 2},
            {2, 22, 1},
            {3, 33, 0},
            {4, 145, 5},
          });
    }
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
