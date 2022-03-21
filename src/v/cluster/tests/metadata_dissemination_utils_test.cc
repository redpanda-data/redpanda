// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"

#include <boost/test/tools/old/interface.hpp>

#include <vector>
#define BOOST_TEST_MODULE cluster
#include "cluster/metadata_dissemination_utils.h"
#include "model/metadata.h"

#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_CASE(test_calculation_non_overlapping_nodes) {
    std::vector<model::node_id> cluster_nodes{
      model::node_id{0},
      model::node_id{1},
      model::node_id{2},
      model::node_id{3},
      model::node_id{4}};
    std::vector<model::node_id> single_node{model::node_id{3}};
    std::vector<model::node_id> three_nodes{
      model::node_id{0}, model::node_id{3}, model::node_id{4}};

    auto non_overlapping_1 = cluster::calculate_non_overlapping_nodes(
      single_node, cluster_nodes);
    std::vector<model::node_id> expected_1{
      model::node_id{0},
      model::node_id{1},
      model::node_id{2},
      model::node_id{4}};

    BOOST_REQUIRE_EQUAL(non_overlapping_1, expected_1);

    auto non_overlapping_2 = cluster::calculate_non_overlapping_nodes(
      three_nodes, cluster_nodes);
    std::vector<model::node_id> expected_2{
      model::node_id{1},
      model::node_id{2},
    };

    BOOST_REQUIRE_EQUAL(non_overlapping_2, expected_2);

    auto non_overlapping_3 = cluster::calculate_non_overlapping_nodes(
      cluster_nodes, cluster_nodes);

    BOOST_REQUIRE_EQUAL(non_overlapping_3.size(), 0);
};

BOOST_AUTO_TEST_CASE(test_get_partition_members) {
    model::topic_metadata tp_md(
      model::topic_namespace(model::ns("test-ns"), model::topic("test_tp")));

    auto p0 = model::partition_metadata(model::partition_id(0));
    p0.replicas = {
      model::broker_shard{model::node_id{0}, 1},
      model::broker_shard{model::node_id{1}, 1},
      model::broker_shard{model::node_id{2}, 1},
    };
    auto p1 = model::partition_metadata(model::partition_id(1));
    p1.replicas = {
      model::broker_shard{model::node_id{3}, 1},
      model::broker_shard{model::node_id{4}, 1},
      model::broker_shard{model::node_id{5}, 1},
    };
    tp_md.partitions = {p0, p1};

    auto members = cluster::get_partition_members(
      model::partition_id{1}, tp_md);
    std::vector<model::node_id> expected = {
      model::node_id{3}, model::node_id{4}, model::node_id{5}};

    BOOST_REQUIRE_EQUAL(members, expected);
};