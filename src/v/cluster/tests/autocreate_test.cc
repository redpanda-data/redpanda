// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/metadata_cache.h"
#include "cluster/simple_batch_builder.h"
#include "cluster/tests/cluster_test_fixture.h"
#include "cluster/tests/controller_test_fixture.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/timeout_clock.h"
#include "test_utils/async.h"
#include "test_utils/fixture.h"

#include <boost/test/tools/old/interface.hpp>

#include <chrono>
#include <vector>

std::vector<cluster::topic_configuration> test_topics_configuration() {
    return std::vector<cluster::topic_configuration>{
      cluster::topic_configuration(test_ns, model::topic("tp-1"), 10, 1),
      cluster::topic_configuration(test_ns, model::topic("tp-2"), 10, 1),
      cluster::topic_configuration(test_ns, model::topic("tp-3"), 10, 1),
    };
}

void validate_topic_metadata(cluster::metadata_cache& cache) {
    auto expected_topics = test_topics_configuration();

    for (auto& t_cfg : expected_topics) {
        auto tp_md = cache.get_topic_metadata(t_cfg.tp_ns);
        BOOST_REQUIRE_EQUAL(tp_md.has_value(), true);
        BOOST_REQUIRE_EQUAL(tp_md->partitions.size(), t_cfg.partition_count);
        auto cfg = cache.get_topic_cfg(t_cfg.tp_ns);
        BOOST_REQUIRE_EQUAL(cfg->tp_ns, t_cfg.tp_ns);
        BOOST_REQUIRE_EQUAL(cfg->partition_count, t_cfg.partition_count);
        BOOST_REQUIRE_EQUAL(cfg->replication_factor, t_cfg.replication_factor);
        BOOST_REQUIRE_EQUAL(
          cfg->compaction_strategy, t_cfg.compaction_strategy);
        BOOST_REQUIRE_EQUAL(cfg->compression, t_cfg.compression);
    }
}

void wait_for_leaders(
  cluster::partition_leaders_table& leader_table,
  const model::topic_metadata md) {
    std::vector<ss::future<>> f;
    f.reserve(md.partitions.size());
    for (const auto& p : md.partitions) {
        model::ntp ntp(md.tp_ns.ns, md.tp_ns.tp, p.id);

        f.push_back(leader_table
                      .wait_for_leader(
                        ntp, model::timeout_clock::now() + 5s, std::nullopt)
                      .discard_result());
    }

    ss::when_all_succeed(f.begin(), f.end()).get();
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

FIXTURE_TEST(
  create_single_topic_test_at_current_broker, controller_tests_fixture) {
    auto cntrl = get_controller();
    cntrl->start().get0();
    wait_for_leadership(cntrl->get_partition_leaders().local());

    std::vector<cluster::topic_result> results;
    bool success = false;
    while (!success) {
        results = cntrl->get_topics_frontend()
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
    wait_for_metadata(get_local_cache(), results);
    for (auto& r : results) {
        BOOST_REQUIRE_EQUAL(r.ec, cluster::errc::success);
        auto md = get_local_cache().get_topic_metadata(r.tp_ns);

        BOOST_REQUIRE_EQUAL(md.has_value(), true);
        wait_for_leaders(cntrl->get_partition_leaders().local(), *md);
        BOOST_REQUIRE_EQUAL(md.value().tp_ns, r.tp_ns);
    }
}

FIXTURE_TEST(test_autocreate_on_non_leader, cluster_test_fixture) {
    // root cluster node
    model::node_id n_1(1);
    model::node_id n_2(2);

    add_controller(n_1, ss::smp::count, 9092, 11000, {});
    add_controller(
      n_2,
      ss::smp::count,
      9093,
      11001,
      {{.addr = unresolved_address("127.0.0.1", 11000)}});

    // first controller
    auto cntrl_0 = get_controller(n_1);
    cntrl_0->start().get0();
    wait_for_leadership(cntrl_0->get_partition_leaders().local());

    auto cntrl_1 = get_controller(n_2);
    cntrl_1->start().get0();

    // Wait for cluster to reach stable state
    tests::cooperative_spin_wait_with_timeout(10s, [this] {
        return get_local_cache(model::node_id(1)).all_brokers().size() == 2
               && get_local_cache(model::node_id(2)).all_brokers().size() == 2;
    }).get();

    std::vector<cluster::topic_result> results;
    bool success = false;
    while (!success) {
        results = cntrl_1->get_topics_frontend()
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
        wait_for_leaders(cntrl_0->get_partition_leaders().local(), *md);
        BOOST_REQUIRE_EQUAL(md.value().tp_ns, r.tp_ns);
    }
    // Make sure caches are the same
    validate_topic_metadata(get_local_cache(n_1));
    validate_topic_metadata(get_local_cache(n_2));
}
