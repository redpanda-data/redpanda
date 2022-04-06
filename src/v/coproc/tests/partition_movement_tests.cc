/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "coproc/partition_manager.h"
#include "coproc/tests/fixtures/coproc_bench_fixture.h"
#include "coproc/tests/fixtures/coproc_cluster_fixture.h"
#include "coproc/tests/utils/batch_utils.h"
#include "coproc/tests/utils/coprocessor.h"
#include "kafka/client/client.h"

#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test_log.hpp>

using copro_typeid = coproc::registry::type_identifier;

/// Test moves across shards on same node works
FIXTURE_TEST(test_move_source_topic, coproc_test_fixture) {
    model::topic mvp("mvp");
    setup({{mvp, 4}}).get();

    auto id = coproc::script_id(443920);
    enable_coprocessors(
      {{.id = id(),
        .data{
          .tid = copro_typeid::identity_coprocessor,
          .topics = {{mvp, coproc::topic_ingestion_policy::latest}}}}})
      .get();

    /// Push sample data onto all partitions
    std::vector<ss::future<>> fs;
    for (auto i = 0; i < 4; ++i) {
        model::ntp input_ntp(
          model::kafka_namespace, mvp, model::partition_id(i));
        auto f = produce(input_ntp, make_random_batch(40));
        fs.emplace_back(std::move(f));
    }
    ss::when_all_succeed(fs.begin(), fs.end()).get();

    /// Wait until the materialized topic has come into existance
    model::ntp origin_ntp(model::kafka_namespace, mvp, model::partition_id(0));
    model::ntp target_ntp = origin_ntp;
    target_ntp.tp.topic = to_materialized_topic(
      mvp, identity_coprocessor::identity_topic);
    auto r = consume(target_ntp, 40).get();
    BOOST_REQUIRE(num_records(r) == 40);
    auto shard = root_fixture()->app.shard_table.local().shard_for(target_ntp);
    BOOST_REQUIRE(shard);

    /// Choose target and calculate where to move it to
    const ss::shard_id next_shard = (*shard + 1) % ss::smp::count;
    info("Current target shard {} and next shard {}", *shard, next_shard);
    model::broker_shard bs{
      .node_id = model::node_id(config::node().node_id), .shard = next_shard};

    /// Move the input onto the new desired target
    auto& topics_fe = root_fixture()->app.controller->get_topics_frontend();
    auto ec = topics_fe.local()
                .move_partition_replicas(origin_ntp, {bs}, model::no_timeout)
                .get();
    info("Error code: {}", ec);
    BOOST_REQUIRE(!ec);

    /// Wait until the shard table is updated with the new shard
    tests::cooperative_spin_wait_with_timeout(
      10s,
      [this, target_ntp, next_shard] {
          auto s = root_fixture()->app.shard_table.local().shard_for(
            target_ntp);
          return s && *s == next_shard;
      })
      .get();

    /// Issue a read from the target shard, and expect the topic content
    info("Draining from shard....{}", bs.shard);
    r = consume(target_ntp, 40).get();
    BOOST_CHECK_EQUAL(num_records(r), 40);

    /// Finally, ensure there is no materialized partition on original shard
    auto logf = root_fixture()->app.storage.invoke_on(
      *shard, [origin_ntp](storage::api& api) {
          return api.log_mgr().get(origin_ntp).has_value();
      });
    auto cpmf = root_fixture()->app.cp_partition_manager.invoke_on(
      *shard, [origin_ntp](coproc::partition_manager& pm) {
          return (bool)pm.get(origin_ntp);
      });
    BOOST_CHECK(!logf.get0());
    BOOST_CHECK(!cpmf.get0());
}

bool partition_exists_on_node(
  cluster_test_fixture* ctf, model::broker_shard bs, const model::ntp& ntp) {
    auto n1 = ctf->get_node_application(model::node_id(0))
                ->controller->get_partition_leaders()
                .local()
                .get_leader(ntp);
    if (!n1 || (*n1 != bs.node_id)) {
        return false;
    }
    auto s1 = ctf->get_node_application(bs.node_id);
    auto shard = s1->shard_table.local().shard_for(ntp);
    return bs.shard == *shard;
}

FIXTURE_TEST(test_move_topic_across_nodes, coproc_cluster_fixture) {
    const auto n = 3;
    for (auto i = 0; i < n; ++i) {
        create_node_application(model::node_id(i));
    }
    wait_for_all_members(5s).get();
    for (auto i = 0; i < n; ++i) {
        wait_for_controller_leadership(model::node_id(i)).get();
    }
    start().get();

    /// Find leader node
    auto leader_node_id = get_node_application(model::node_id(0))
                            ->controller->get_partition_leaders()
                            .local()
                            .get_leader(model::controller_ntp);
    BOOST_REQUIRE(leader_node_id.has_value());
    auto leader = get_node_application(*leader_node_id);

    /// Create topic
    auto n_partitions = 3;
    model::topic_namespace tp(model::kafka_namespace, model::topic("source"));
    cluster::topic_configuration tcfg(tp.ns, tp.tp, n_partitions, 1);
    auto oec = leader->controller->get_topics_frontend()
                 .local()
                 .autocreate_topics({tcfg}, 5s)
                 .get();
    BOOST_REQUIRE_EQUAL(oec.size(), 1);
    BOOST_REQUIRE_EQUAL(oec[0].ec, cluster::errc::success);
    BOOST_REQUIRE_EQUAL(oec[0].tp_ns, tp);

    /// Deploy coprocessor
    auto id = coproc::script_id(440);
    enable_coprocessors(
      {{.id = id(),
        .data{
          .tid = copro_typeid::identity_coprocessor,
          .topics = {{tp.tp, coproc::topic_ingestion_policy::earliest}}}}})
      .get();

    /// Push some sample data
    std::vector<ss::future<>> fs;
    for (auto i = 0; i < n_partitions; ++i) {
        model::ntp input_ntp(
          model::kafka_namespace, tp.tp, model::partition_id(i));
        auto f = produce(input_ntp, make_random_batch(40));
        fs.emplace_back(std::move(f));
    }
    ss::when_all_succeed(fs.begin(), fs.end()).get();

    /// Push consume all materialized log data from target partition
    model::ntp source_ntp(tp.ns, tp.tp, model::partition_id(0));
    model::ntp materialized_ntp(
      tp.ns,
      to_materialized_topic(tp.tp, identity_coprocessor::identity_topic),
      model::partition_id(0));
    auto r = consume(materialized_ntp, 40).get();
    BOOST_REQUIRE(num_records(r) == 40);

    /// Finally move the source topic after all processing by coproc has been
    /// completed. To do this, first find out what model::broker_shard the
    /// source_ntp exists on, then attempt to move it to the node with the next
    /// node/shard id (mod n)
    auto topic_leader_id
      = leader->controller->get_partition_leaders().local().get_leader(
        source_ntp);
    BOOST_REQUIRE(topic_leader_id);
    auto topic_leader = get_node_application(*topic_leader_id);
    auto origin_shard = topic_leader->shard_table.local().shard_for(source_ntp);
    BOOST_REQUIRE(origin_shard.has_value());
    model::broker_shard old_bs{
      .node_id = *topic_leader_id, .shard = *origin_shard};
    model::broker_shard new_bs{
      .node_id = model::node_id((*topic_leader_id + 1) % n),
      .shard = ss::shard_id((*origin_shard + 1) % n)};

    info("Old broker_shard {} moving to new broker_shard {}", old_bs, new_bs);
    auto& topics_fe = leader->controller->get_topics_frontend();
    auto ec = topics_fe.local()
                .move_partition_replicas(
                  source_ntp, {new_bs}, model::timeout_clock::now() + 5s)
                .get();
    info("Error code: {}", ec);
    BOOST_REQUIRE(!ec);

    /// Wait until the move occurs
    info("Waiting for move of {} to {}", source_ntp, new_bs);
    tests::cooperative_spin_wait_with_timeout(
      10s,
      [this, source_ntp, new_bs]() {
          return ss::make_ready_future<bool>(
            partition_exists_on_node(this, new_bs, source_ntp));
      })
      .get();

    /// Wait until the materialized moves also occurred
    info("Waiting for move of {} to {}", materialized_ntp, new_bs);
    tests::cooperative_spin_wait_with_timeout(
      10s,
      [this, materialized_ntp, new_bs]() {
          return ss::make_ready_future<bool>(
            partition_exists_on_node(this, new_bs, materialized_ntp));
      })
      .get();

    /// Try to consume from the materialized partition
    get_client().update_metadata().get();
    r = consume(materialized_ntp, 40).get();
    BOOST_REQUIRE(num_records(r) == 40);

    /// Verify no reminents exist on old broker/shard
    auto logf = get_node_application(old_bs.node_id)
                  ->storage.invoke_on(
                    old_bs.shard, [source_ntp](storage::api& api) {
                        return api.log_mgr().get(source_ntp).has_value();
                    });
    BOOST_CHECK(!logf.get());
}
