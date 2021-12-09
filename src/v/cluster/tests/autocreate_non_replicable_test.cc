// Copyright 2020 Vectorized, Inc.
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

static const model::ns tp1 = model::ns("tp-1");
static const model::ns tp2 = model::ns("tp-2");
static const model::ns tp3 = model::ns("tp-3");

std::vector<cluster::topic_configuration> source_topics_configuration() {
    return std::vector<cluster::topic_configuration>{
      cluster::topic_configuration(test_ns, model::topic("test"), 10, 1),
    };
}

std::vector<cluster::non_replicable_topic> test_topics() {
    return std::vector<cluster::non_replicable_topic>{
      cluster::non_replicable_topic{
        .source = model::topic_namespace{test_ns, model::topic("test")},
        .name = model::topic_namespace{tp1, model::topic("tp-1")}},
      cluster::non_replicable_topic{
        .source = model::topic_namespace{test_ns, model::topic("test")},
        .name = model::topic_namespace{tp2, model::topic("tp-2")}},
      cluster::non_replicable_topic{
        .source = model::topic_namespace{test_ns, model::topic("test")},
        .name = model::topic_namespace{tp3, model::topic("tp-3")}},
    };
}

FIXTURE_TEST(
  create_single_non_replicable_topic_test_at_current_broker,
  cluster_test_fixture) {
    model::node_id id{0};
    auto app = create_node_application(id);
    wait_for_controller_leadership(id).get();

    std::vector<cluster::topic_result> sources;
    bool success = false;
    while (!success) {
        sources = app->controller->get_topics_frontend()
                    .local()
                    .autocreate_topics(
                      source_topics_configuration(), std::chrono::seconds(10))
                    .get0();
        success = std::all_of(
          sources.begin(), sources.end(), [](const cluster::topic_result& r) {
              return r.ec == cluster::errc::success;
          });
    }

    std::vector<cluster::topic_result> results;
    success = false;
    while (!success) {
        results = app->controller->get_topics_frontend()
                    .local()
                    .autocreate_non_replicable_topics(
                      test_topics(),
                      model::timeout_clock::now() + std::chrono::seconds(10))
                    .get0();
        success = std::all_of(
          results.begin(), results.end(), [](const cluster::topic_result& r) {
              return r.ec == cluster::errc::success;
          });
    }

    BOOST_REQUIRE_EQUAL(results.size(), 3);
}

FIXTURE_TEST(
  test_autocreate_non_replicable_on_non_leader, cluster_test_fixture) {
    // root cluster node
    model::node_id n_1(0);
    model::node_id n_2(1);

    // first controller
    auto app_0 = create_node_application(n_1);
    auto app_1 = create_node_application(n_2);

    wait_for_controller_leadership(n_1).get();

    // Wait for cluster to reach stable state
    tests::cooperative_spin_wait_with_timeout(10s, [this] {
        return get_local_cache(model::node_id(0)).all_brokers().size() == 2
               && get_local_cache(model::node_id(1)).all_brokers().size() == 2;
    }).get();

    std::vector<cluster::topic_result> sources;
    bool success = false;
    while (!success) {
        sources = app_0->controller->get_topics_frontend()
                    .local()
                    .autocreate_topics(
                      source_topics_configuration(), std::chrono::seconds(10))
                    .get0();
        success = std::all_of(
          sources.begin(), sources.end(), [](const cluster::topic_result& r) {
              return r.ec == cluster::errc::success;
          });
    }

    std::vector<cluster::topic_result> results;
    success = false;
    while (!success) {
        results = app_0->controller->get_topics_frontend()
                    .local()
                    .autocreate_non_replicable_topics(
                      test_topics(),
                      model::timeout_clock::now() + std::chrono::seconds(10))
                    .get0();
        success = std::all_of(
          results.begin(), results.end(), [](const cluster::topic_result& r) {
              return r.ec == cluster::errc::success;
          });
    }
}
