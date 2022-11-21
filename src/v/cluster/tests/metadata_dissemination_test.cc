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
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "net/unresolved_address.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"

#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>

#include <vector>
using namespace std::chrono_literals; // NOLINT

std::vector<model::node_id>
wait_for_leaders_updates(int id, cluster::metadata_cache& cache) {
    std::vector<model::node_id> leaders;
    tests::cooperative_spin_wait_with_timeout(
      std::chrono::seconds(10),
      [&cache, &leaders] {
          leaders.clear();
          const model::topic_namespace tn(
            model::ns("default"), model::topic("test_1"));
          auto tp_md = cache.get_topic_metadata(tn);

          if (!tp_md) {
              return false;
          }
          if (tp_md->get_assignments().size() != 3) {
              return false;
          }
          for (auto& p_md : tp_md->get_assignments()) {
              auto leader_id = cache.get_leader_id(tn, p_md.id);
              if (!leader_id) {
                  return false;
              }
              leaders.push_back(*leader_id);
          }
          return true;
      })
      .get0();
    return leaders;
}

FIXTURE_TEST(
  test_metadata_dissemination_from_single_partition, cluster_test_fixture) {
    model::node_id n_1(0);
    model::node_id n_2(1);
    model::node_id n_3(2);
    auto cntrl_0 = create_node_application(n_1);
    create_node_application(n_2);
    create_node_application(n_3);

    auto& cache_0 = get_local_cache(n_1);
    auto& cache_1 = get_local_cache(n_2);
    auto& cache_2 = get_local_cache(n_3);

    tests::cooperative_spin_wait_with_timeout(
      std::chrono::seconds(10),
      [&cache_1, &cache_2] {
          return cache_1.node_count() == 3 && cache_2.node_count() == 3;
      })
      .get0();

    // Make sure we have 3 working nodes
    BOOST_REQUIRE_EQUAL(cache_0.node_count(), 3);
    BOOST_REQUIRE_EQUAL(cache_1.node_count(), 3);
    BOOST_REQUIRE_EQUAL(cache_2.node_count(), 3);

    // Create topic with replication factor 1
    std::vector<cluster::topic_configuration> topics;
    topics.emplace_back(model::ns("default"), model::topic("test_1"), 3, 1);
    cntrl_0->controller->get_topics_frontend()
      .local()
      .create_topics(
        cluster::without_custom_assignments(std::move(topics)),
        model::no_timeout)
      .get0();

    auto leaders_0 = wait_for_leaders_updates(0, cache_0);
    auto leaders_1 = wait_for_leaders_updates(1, cache_1);
    auto leaders_2 = wait_for_leaders_updates(2, cache_2);

    BOOST_REQUIRE_EQUAL(leaders_0, leaders_1);
    BOOST_REQUIRE_EQUAL(leaders_0, leaders_2);
}

FIXTURE_TEST(test_metadata_dissemination_joining_node, cluster_test_fixture) {
    model::node_id n_1(0);
    model::node_id n_2(1);
    auto cntrl_0 = create_node_application(n_1);
    create_node_application(n_2);

    auto& cache_0 = get_local_cache(n_1);
    auto& cache_1 = get_local_cache(n_2);

    tests::cooperative_spin_wait_with_timeout(
      std::chrono::seconds(10),
      [&cache_1] { return cache_1.node_count() == 2; })
      .get0();
    // Make sure we have 2 working nodes
    BOOST_REQUIRE_EQUAL(cache_0.node_count(), 2);
    BOOST_REQUIRE_EQUAL(cache_1.node_count(), 2);

    // Create topic with replication factor 1
    std::vector<cluster::topic_configuration> topics;
    topics.emplace_back(model::ns("default"), model::topic("test_1"), 3, 1);
    cntrl_0->controller->get_topics_frontend()
      .local()
      .create_topics(
        cluster::without_custom_assignments(std::move(topics)),
        model::no_timeout)
      .get0();

    // Add new now to the cluster
    create_node_application(model::node_id{2});
    auto& cache_2 = get_local_cache(model::node_id{2});
    // Wait for node to join the cluster
    tests::cooperative_spin_wait_with_timeout(
      std::chrono::seconds(10),
      [&cache_1, &cache_2] {
          return cache_1.node_count() == 3 && cache_2.node_count() == 3;
      })
      .get0();

    auto leaders_0 = wait_for_leaders_updates(0, cache_0);
    auto leaders_1 = wait_for_leaders_updates(1, cache_1);
    auto leaders_2 = wait_for_leaders_updates(2, cache_2);
}
