// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "model/fundamental.h"
#include "random/generators.h"

#include <boost/test/tools/old/interface.hpp>

#include <vector>
#define BOOST_TEST_MODULE cluster
#include "cluster/metadata_dissemination_utils.h"
#include "model/metadata.h"

#include <boost/test/unit_test.hpp>

cluster::partition_assignment
make_assignment(const std::vector<model::node_id>& nodes) {
    cluster::partition_assignment p_as{
      .group = raft::group_id(1), .id = model::partition_id(1)};
    p_as.replicas.reserve(nodes.size());
    for (auto n : nodes) {
        p_as.replicas.push_back(model::broker_shard{
          .node_id = n, .shard = random_generators::get_int<uint32_t>(10)});
    }

    return p_as;
}

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
      make_assignment(single_node), cluster_nodes);
    std::vector<model::node_id> expected_1{
      model::node_id{0},
      model::node_id{1},
      model::node_id{2},
      model::node_id{4}};

    BOOST_REQUIRE_EQUAL(non_overlapping_1, expected_1);

    auto non_overlapping_2 = cluster::calculate_non_overlapping_nodes(
      make_assignment(three_nodes), cluster_nodes);
    std::vector<model::node_id> expected_2{
      model::node_id{1},
      model::node_id{2},
    };

    BOOST_REQUIRE_EQUAL(non_overlapping_2, expected_2);

    auto non_overlapping_3 = cluster::calculate_non_overlapping_nodes(
      make_assignment(cluster_nodes), cluster_nodes);

    BOOST_REQUIRE_EQUAL(non_overlapping_3.size(), 0);
};
