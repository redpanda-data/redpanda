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
#include "test_utils/async.h"
#include "test_utils/fixture.h"

#include <boost/test/tools/old/interface.hpp>

#include <chrono>
#include <vector>

std::vector<cluster::topic_configuration> test_topics_configuration(
  cluster::replication_factor rf = cluster::replication_factor{1}) {
    return std::vector<cluster::topic_configuration>{
      cluster::topic_configuration(test_ns, model::topic("tp-1"), 10, rf),
      cluster::topic_configuration(test_ns, model::topic("tp-2"), 10, rf),
      cluster::topic_configuration(test_ns, model::topic("tp-3"), 10, rf),
    };
}

void validate_topic_metadata(cluster::metadata_cache& cache) {
    cluster::replication_factor expected_replication_factor
      = cluster::replication_factor{1};
    auto expected_topics = test_topics_configuration(
      expected_replication_factor);

    for (auto& t_cfg : expected_topics) {
        auto tp_md = cache.get_topic_metadata(t_cfg.tp_ns);
        BOOST_REQUIRE_EQUAL(tp_md.has_value(), true);
        BOOST_REQUIRE_EQUAL(
          tp_md->get_assignments().size(), t_cfg.partition_count);
        auto cfg = cache.get_topic_cfg(t_cfg.tp_ns);
        BOOST_REQUIRE_EQUAL(cfg->tp_ns, t_cfg.tp_ns);
        BOOST_REQUIRE_EQUAL(cfg->partition_count, t_cfg.partition_count);
        BOOST_REQUIRE_EQUAL(
          tp_md->get_replication_factor(), expected_replication_factor);
        BOOST_REQUIRE_EQUAL(
          cfg->properties.compaction_strategy,
          t_cfg.properties.compaction_strategy);
        BOOST_REQUIRE_EQUAL(
          cfg->properties.compression, t_cfg.properties.compression);
    }
}

void wait_for_metadata(
  cluster::metadata_cache& md_cache,
  const std::vector<cluster::topic_result>& results) {
    tests::cooperative_spin_wait_with_timeout(2s, [&results, &md_cache] {
        return std::all_of(
          results.begin(),
          results.end(),
          [&md_cache](const cluster::topic_result& r) {
              return md_cache.get_topic_metadata(r.tp_ns);
          });
    }).get0();
}

FIXTURE_TEST(create_single_topic_test_at_current_broker, cluster_test_fixture) {
    model::node_id id{0};
    auto app = create_node_application(id);
    wait_for_controller_leadership(id).get();

    std::vector<cluster::topic_result> results;
    bool success = false;
    while (!success) {
        results = app->controller->get_topics_frontend()
                    .local()
                    .autocreate_topics(
                      test_topics_configuration(), std::chrono::seconds(10))
                    .get0();
        success = std::all_of(
          results.begin(), results.end(), [](const cluster::topic_result& r) {
              return r.ec == cluster::errc::success;
          });
    }

    BOOST_REQUIRE_EQUAL(results.size(), 3);
    wait_for_metadata(app->metadata_cache.local(), results);
    for (auto& r : results) {
        BOOST_REQUIRE_EQUAL(r.ec, cluster::errc::success);
        auto md = app->metadata_cache.local().get_topic_metadata(r.tp_ns);

        BOOST_REQUIRE_EQUAL(md.has_value(), true);
        BOOST_REQUIRE_EQUAL(md->get_configuration().tp_ns, r.tp_ns);
    }
}

FIXTURE_TEST(test_autocreate_on_non_leader, cluster_test_fixture) {
    // root cluster node
    model::node_id n_1(0);
    model::node_id n_2(1);

    // first controller
    auto app_0 = create_node_application(n_1);
    create_node_application(n_2);

    wait_for_controller_leadership(n_1).get();

    // Wait for cluster to reach stable state
    tests::cooperative_spin_wait_with_timeout(10s, [this] {
        return get_local_cache(model::node_id(0)).node_count() == 2
               && get_local_cache(model::node_id(1)).node_count() == 2;
    }).get();

    std::vector<cluster::topic_result> results;
    bool success = false;
    while (!success) {
        results = app_0->controller->get_topics_frontend()
                    .local()
                    .autocreate_topics(
                      test_topics_configuration(), std::chrono::seconds(10))
                    .get0();
        success = std::all_of(
          results.begin(), results.end(), [](const cluster::topic_result& r) {
              return r.ec == cluster::errc::success;
          });
    }
    wait_for_metadata(get_local_cache(n_1), results);
    wait_for_metadata(get_local_cache(n_2), results);
    for (auto& r : results) {
        BOOST_REQUIRE_EQUAL(r.ec, cluster::errc::success);
        auto md = get_local_cache(n_1).get_topic_metadata(r.tp_ns);
        BOOST_REQUIRE_EQUAL(md.has_value(), true);
        BOOST_REQUIRE_EQUAL(md->get_configuration().tp_ns, r.tp_ns);
    }
    // Make sure caches are the same
    validate_topic_metadata(get_local_cache(n_1));
    validate_topic_metadata(get_local_cache(n_2));
}
