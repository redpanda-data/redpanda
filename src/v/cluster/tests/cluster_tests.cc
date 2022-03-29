// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/metadata_cache.h"
#include "cluster/shard_table.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "config/configuration.h"
#include "net/unresolved_address.h"
#include "test_utils/fixture.h"

using namespace std::chrono_literals; // NOLINT

FIXTURE_TEST(test_join_single_node, cluster_test_fixture) {
    model::node_id id{0};
    auto app = create_node_application(id);
    wait_for_controller_leadership(id).get();

    wait_for_all_members(3s).get();

    auto brokers = get_local_cache(model::node_id{0}).all_brokers();

    // single broker
    BOOST_REQUIRE_EQUAL(brokers.size(), 1);
    BOOST_REQUIRE_EQUAL(brokers[0]->id(), model::node_id(0));
}

FIXTURE_TEST(test_two_node_cluster, cluster_test_fixture) {
    auto n1 = create_node_application(model::node_id{0});
    auto n2 = create_node_application(model::node_id{1});
    // Check if all brokers were registered
    wait_for_all_members(3s).get();
}

FIXTURE_TEST(test_three_node_cluster, cluster_test_fixture) {
    auto n1 = create_node_application(model::node_id{0});
    auto n2 = create_node_application(model::node_id{1});
    auto n3 = create_node_application(model::node_id{2});

    wait_for_all_members(3s).get();
}
