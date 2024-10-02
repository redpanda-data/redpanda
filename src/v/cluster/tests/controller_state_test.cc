// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/cluster_utils.h"
#include "cluster/metadata_cache.h"
#include "cluster/shard_table.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "cluster/tests/utils.h"
#include "cluster/topics_frontend.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"
#include "utils/unresolved_address.h"

#include <seastar/core/future-util.hh>
#include <seastar/core/sstring.hh>

#include <bits/stdint-intn.h>
#include <boost/test/tools/old/interface.hpp>
using namespace std::chrono_literals; // NOLINT

cluster::topic_configuration
create_topic_cfg(ss::sstring topic, int32_t p, int16_t r) {
    return cluster::topic_configuration(
      test_ns, model::topic(std::move(topic)), p, r);
}

FIXTURE_TEST(test_creating_same_topic_twice, cluster_test_fixture) {
    // add three nodes
    auto node_0 = create_node_application(model::node_id{0});
    wait_for_controller_leadership(node_0->controller->self()).get();
    create_node_application(model::node_id{1});
    create_node_application(model::node_id{2});

    // wait for cluster to be stable
    tests::cooperative_spin_wait_with_timeout(5s, [this] {
        return get_local_cache(model::node_id{0}).node_count() == 3
               && get_local_cache(model::node_id{1}).node_count() == 3
               && get_local_cache(model::node_id{2}).node_count() == 3;
    }).get();

    std::vector<ss::future<std::vector<cluster::topic_result>>> futures;

    futures.push_back(
      node_0->controller->get_topics_frontend().local().create_topics(
        cluster::without_custom_assignments({create_topic_cfg("test-1", 1, 3)}),
        model::timeout_clock::now() + 10s));

    futures.push_back(
      node_0->controller->get_topics_frontend().local().create_topics(
        cluster::without_custom_assignments({create_topic_cfg("test-1", 2, 3)}),
        model::timeout_clock::now() + 10s));

    ss::when_all_succeed(futures.begin(), futures.end())
      .then([](std::vector<std::vector<cluster::topic_result>> results) {
          BOOST_REQUIRE_EQUAL(results.size(), 2);
          size_t successes = 0;
          for (const auto& v : results) {
              for (const auto& inner : v) {
                  if (inner.ec == cluster::errc::success) {
                      ++successes;
                  }
              }
          }
          BOOST_REQUIRE_EQUAL(successes, 1);
      })
      .get();
}
