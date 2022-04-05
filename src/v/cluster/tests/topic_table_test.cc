// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tests/topic_table_fixture.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/types.h"

#include <seastar/testing/thread_test_case.hh>

using namespace std::chrono_literals;

FIXTURE_TEST(test_happy_path_create, topic_table_fixture) {
    create_topics();
    auto md = table.local().all_topics_metadata();

    BOOST_REQUIRE_EQUAL(md.size(), 3);
    std::sort(
      md.begin(),
      md.end(),
      [](const model::topic_metadata& a, const model::topic_metadata& b) {
          return a.tp_ns.tp < b.tp_ns.tp;
      });
    BOOST_REQUIRE_EQUAL(md[0].tp_ns, make_tp_ns("test_tp_1"));
    BOOST_REQUIRE_EQUAL(md[1].tp_ns, make_tp_ns("test_tp_2"));
    BOOST_REQUIRE_EQUAL(md[2].tp_ns, make_tp_ns("test_tp_3"));

    BOOST_REQUIRE_EQUAL(md[0].partitions.size(), 1);
    BOOST_REQUIRE_EQUAL(md[1].partitions.size(), 12);
    BOOST_REQUIRE_EQUAL(md[2].partitions.size(), 8);

    // check delta
    auto d = table.local().wait_for_changes(as).get0();

    validate_delta(d, 21, 0);
}

FIXTURE_TEST(test_happy_path_delete, topic_table_fixture) {
    create_topics();
    // discard create delta
    table.local().wait_for_changes(as).get0();
    auto res_1 = table.local()
                   .apply(
                     cluster::delete_topic_cmd(
                       make_tp_ns("test_tp_2"), make_tp_ns("test_tp_2")),
                     model::offset(0))
                   .get0();
    auto res_2 = table.local()
                   .apply(
                     cluster::delete_topic_cmd(
                       make_tp_ns("test_tp_3"), make_tp_ns("test_tp_3")),
                     model::offset(0))
                   .get0();

    auto md = table.local().all_topics_metadata();
    BOOST_REQUIRE_EQUAL(md.size(), 1);
    BOOST_REQUIRE_EQUAL(md[0].tp_ns, make_tp_ns("test_tp_1"));

    BOOST_REQUIRE_EQUAL(md[0].partitions.size(), 1);
    // check delta
    auto d = table.local().wait_for_changes(as).get0();

    validate_delta(d, 0, 20);
}

FIXTURE_TEST(test_conflicts, topic_table_fixture) {
    create_topics();
    // discard create delta
    table.local().wait_for_changes(as).get0();

    auto res_1 = table.local()
                   .apply(
                     cluster::delete_topic_cmd(
                       make_tp_ns("not_exists"), make_tp_ns("not_exists")),
                     model::offset(0))
                   .get0();
    BOOST_REQUIRE_EQUAL(res_1, cluster::errc::topic_not_exists);

    auto res_2 = table.local()
                   .apply(
                     make_create_topic_cmd("test_tp_1", 2, 3), model::offset(0))
                   .get0();
    BOOST_REQUIRE_EQUAL(res_2, cluster::errc::topic_already_exists);
    BOOST_REQUIRE_EQUAL(table.local().has_pending_changes(), false);
}

FIXTURE_TEST(get_getting_config, topic_table_fixture) {
    create_topics();
    auto cfg = table.local().get_topic_cfg(make_tp_ns("test_tp_1"));
    BOOST_REQUIRE(cfg.has_value());
    auto v = cfg.value();
    BOOST_REQUIRE_EQUAL(
      v.properties.compaction_strategy, model::compaction_strategy::offset);

    BOOST_REQUIRE_EQUAL(
      v.properties.cleanup_policy_bitflags,
      model::cleanup_policy_bitflags::compaction);
    BOOST_REQUIRE_EQUAL(v.properties.compression, model::compression::lz4);
    BOOST_REQUIRE_EQUAL(
      v.properties.retention_bytes, tristate(std::make_optional(2_GiB)));
    BOOST_REQUIRE_EQUAL(
      v.properties.retention_duration,
      tristate(std::make_optional(std::chrono::milliseconds(3600000))));
}

FIXTURE_TEST(test_wait_aborted, topic_table_fixture) {
    ss::abort_source local_as;
    ss::timer<> timer;
    timer.set_callback([&local_as] { local_as.request_abort(); });
    timer.arm(500ms);
    // discard create delta
    BOOST_REQUIRE_THROW(
      table.local().wait_for_changes(local_as).get0(),
      ss::abort_requested_exception);
}

FIXTURE_TEST(test_adding_partition, topic_table_fixture) {
    // discard create delta
    create_topics();
    table.local().wait_for_changes(as).get0();
    cluster::create_partititions_configuration cfg(make_tp_ns("test_tp_2"), 3);
    std::vector<cluster::partition_assignment> p_as{
      cluster::partition_assignment{
        .group = raft::group_id(10),
        .id = model::partition_id(0),
        .replicas
        = {model::broker_shard{model::node_id(0), 0}, model::broker_shard{model::node_id(1), 1}, model::broker_shard{model::node_id(2), 2}},
      },
      cluster::partition_assignment{
        .group = raft::group_id(11),
        .id = model::partition_id(1),
        .replicas
        = {model::broker_shard{model::node_id(0), 0}, model::broker_shard{model::node_id(1), 1}, model::broker_shard{model::node_id(2), 2}},
      },
      cluster::partition_assignment{
        .group = raft::group_id(12),
        .id = model::partition_id(2),
        .replicas
        = {model::broker_shard{model::node_id(0), 0}, model::broker_shard{model::node_id(1), 1}, model::broker_shard{model::node_id(2), 2}},
      }};
    cluster::create_partititions_configuration_assignment pca(
      std::move(cfg), std::move(p_as));

    auto res_1 = table.local()
                   .apply(
                     cluster::create_partition_cmd(
                       make_tp_ns("test_tp_2"), std::move(pca)),
                     model::offset(0))
                   .get0();

    auto md = table.local().get_topic_metadata(make_tp_ns("test_tp_2"));

    BOOST_REQUIRE_EQUAL(md->partitions.size(), 15);
    // check delta
    auto d = table.local().wait_for_changes(as).get0();
    // require 3 partition additions
    validate_delta(d, 3, 0);
}
