/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/vectorizedio/redpanda/blob/master/licenses/rcl.md
 */

#include "coproc/partition_manager.h"
#include "coproc/tests/fixtures/coproc_cluster_fixture.h"
#include "test_utils/async.h"

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

ss::future<bool> partition_exists(application* app, const model::ntp& ntp) {
    /// A non_replicable partition is considered to exist if it exists in the
    /// shard table, partition manager and storage
    auto shard = app->shard_table.local().shard_for(ntp);
    if (!shard) {
        co_return false;
    }
    auto p_exists = co_await app->cp_partition_manager.invoke_on(
      *shard,
      [ntp](coproc::partition_manager& pm) { return (bool)pm.get(ntp); });
    if (!p_exists) {
        co_return false;
    }
    auto log_exists = co_await app->storage.invoke_on(
      *shard,
      [ntp](storage::api& api) { return (bool)api.log_mgr().get(ntp); });
    if (!log_exists) {
        co_return false;
    }
    co_return true;
}

FIXTURE_TEST(autocreate_non_replicable_topic_test, coproc_cluster_fixture) {
    /// Start the test with a new cluster with 3 nodes
    auto n = 3;
    for (auto i = 0; i < n; ++i) {
        create_node_application(model::node_id(i));
    }
    wait_for_all_members(5s).get();
    for (auto i = 0; i < n; ++i) {
        wait_for_controller_leadership(model::node_id(i)).get();
    }

    /// Find a node thats NOT the leader and make the request there
    auto leader_id = get_node_application(model::node_id(0))
                       ->controller->get_partition_leaders()
                       .local()
                       .get_leader(model::controller_ntp);
    BOOST_REQUIRE(leader_id.has_value());
    auto non_leader_id = model::node_id((*leader_id + 1) % n);
    auto leader = get_node_application(*leader_id);
    auto non_leader = get_node_application(non_leader_id);

    /// Create test topic
    cluster::non_replicable_topic nrt{
      .source = model::topic_namespace(
        model::kafka_namespace, model::topic("test")),
      .name = model::topic_namespace(
        model::kafka_namespace, model::topic("test-materialized"))};
    auto n_partitions = 3;
    cluster::topic_configuration cfg(nrt.source.ns, nrt.source.tp, 3, 1);
    auto secf
      = non_leader->controller->get_topics_frontend().local().autocreate_topics(
        {cfg}, 10s);
    auto ec = secf.get();
    BOOST_REQUIRE_EQUAL(ec.size(), 1);
    BOOST_REQUIRE_EQUAL(ec[0].tp_ns, nrt.source);
    BOOST_REQUIRE_EQUAL(ec[0].ec, cluster::errc::success);

    /// Create non_replicable topic on a node thats NOT the leader
    auto ecf = non_leader->controller->get_topics_frontend()
                 .local()
                 .autocreate_non_replicable_topics({nrt}, 25s);
    ec = ecf.get();
    BOOST_REQUIRE_EQUAL(ec.size(), 1);
    BOOST_REQUIRE_EQUAL(ec[0].tp_ns, nrt.name);
    BOOST_REQUIRE_EQUAL(ec[0].ec, cluster::errc::success);

    /// Where to look for the partitions
    std::vector<std::pair<model::node_id, model::ntp>> partition_mapping;
    auto& leaders_table = leader->controller->get_partition_leaders().local();
    for (auto i = 0; i < n_partitions; ++i) {
        model::ntp src_ntp(
          nrt.source.ns, nrt.source.tp, model::partition_id(i));
        model::ntp nrt_ntp(nrt.name.ns, nrt.name.tp, model::partition_id(i));
        /// Quick verify that non_replicable topics match their sources
        auto src_leader = leaders_table.get_leader(src_ntp);
        auto nrt_leader = leaders_table.get_leader(src_ntp);
        BOOST_REQUIRE(src_leader.has_value());
        BOOST_REQUIRE(nrt_leader.has_value());
        BOOST_REQUIRE_EQUAL(*src_leader, *nrt_leader);
        partition_mapping.emplace_back(*nrt_leader, nrt_ntp);
    }
    tests::cooperative_spin_wait_with_timeout(15s, [this, partition_mapping]() {
        return ss::map_reduce(
          partition_mapping.begin(),
          partition_mapping.end(),
          [this](const auto& p) {
              return partition_exists(get_node_application(p.first), p.second);
          },
          true,
          std::logical_and<>());
    }).get();
}
