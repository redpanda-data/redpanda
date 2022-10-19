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
    create_node_application(id);
    wait_for_controller_leadership(id).get();

    wait_for_all_members(3s).get();

    auto brokers = get_local_cache(model::node_id{0}).all_brokers();

    // single broker
    BOOST_REQUIRE_EQUAL(brokers.size(), 1);
    BOOST_REQUIRE_EQUAL(brokers[0]->id(), model::node_id(0));
}

FIXTURE_TEST(test_two_node_cluster, cluster_test_fixture) {
    create_node_application(model::node_id{0});
    create_node_application(model::node_id{1});
    // Check if all brokers were registered
    wait_for_all_members(3s).get();
}

FIXTURE_TEST(test_three_node_cluster, cluster_test_fixture) {
    create_node_application(model::node_id{0});
    create_node_application(model::node_id{1});
    create_node_application(model::node_id{2});

    wait_for_all_members(3s).get();
}

FIXTURE_TEST(test_auto_assign_node_id, cluster_test_fixture) {
    create_node_application(model::node_id{0}, configure_node_id::no);
    BOOST_REQUIRE_EQUAL(0, *config::node().node_id());

    create_node_application(model::node_id{1}, configure_node_id::no);
    BOOST_REQUIRE_EQUAL(1, *config::node().node_id());

    create_node_application(model::node_id{2}, configure_node_id::no);
    BOOST_REQUIRE_EQUAL(2, *config::node().node_id());

    wait_for_all_members(3s).get();
}

FIXTURE_TEST(test_auto_assign_non_seeds, cluster_test_fixture) {
    create_node_application(model::node_id{0});
    BOOST_REQUIRE_EQUAL(0, *config::node().node_id());

    create_node_application(model::node_id{1}, configure_node_id::no);
    BOOST_REQUIRE_EQUAL(1, *config::node().node_id());

    create_node_application(model::node_id{2}, configure_node_id::no);
    BOOST_REQUIRE_EQUAL(2, *config::node().node_id());

    wait_for_all_members(3s).get();
}

FIXTURE_TEST(test_auto_assign_with_explicit_node_id, cluster_test_fixture) {
    create_node_application(model::node_id{0});
    BOOST_REQUIRE_EQUAL(0, *config::node().node_id());

    // Explicitly assign node ID 2. Node ID assignment should assign around it.
    create_node_application(model::node_id{2});
    BOOST_REQUIRE_EQUAL(2, *config::node().node_id());

    create_node_application(model::node_id{1}, configure_node_id::no);
    BOOST_REQUIRE_EQUAL(1, *config::node().node_id());

    create_node_application(model::node_id{3}, configure_node_id::no);
    BOOST_REQUIRE_EQUAL(3, *config::node().node_id());

    wait_for_all_members(3s).get();
}

FIXTURE_TEST(
  test_seed_driven_cluster_bootstrap_single_node, cluster_test_fixture) {
    const model::node_id id0{0};
    create_node_application(
      id0, configure_node_id::no, empty_seed_starts_cluster::no);
    BOOST_REQUIRE_EQUAL(0, *config::node().node_id());
    wait_for_controller_leadership(id0).get();
    wait_for_all_members(3s).get();
    auto brokers = get_local_cache(id0).all_brokers();

    // single broker
    BOOST_REQUIRE_EQUAL(brokers.size(), 1);
    BOOST_REQUIRE_EQUAL(brokers[0]->id(), id0);
}
