// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/scheduling/types.h"
#include "cluster/tests/topic_table_fixture.h"
#include "cluster/topic_updates_dispatcher.h"
#include "model/metadata.h"

#include <seastar/testing/thread_test_case.hh>

#include <bits/stdint-uintn.h>

#include <cstdint>
using namespace std::chrono_literals;

struct topic_table_updates_dispatcher_fixture : topic_table_fixture {
    topic_table_updates_dispatcher_fixture()
      : dispatcher(allocator, table, leaders, pb_state) {}

    void create_topics() {
        auto cmd_1 = make_create_topic_cmd("test_tp_1", 1, 3);
        cmd_1.value.cfg.properties.compaction_strategy
          = model::compaction_strategy::offset;
        cmd_1.value.cfg.properties.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::compaction;
        cmd_1.value.cfg.properties.compression = model::compression::lz4;
        cmd_1.value.cfg.properties.retention_bytes = tristate(
          std::make_optional(2_GiB));
        cmd_1.value.cfg.properties.retention_duration = tristate(
          std::make_optional(std::chrono::milliseconds(3600000)));
        auto cmd_2 = make_create_topic_cmd("test_tp_2", 12, 3);
        auto cmd_3 = make_create_topic_cmd("test_tp_3", 8, 1);

        auto res_1 = dispatcher
                       .apply_update(serialize_cmd(std::move(cmd_1)).get0())
                       .get0();
        auto res_2 = dispatcher
                       .apply_update(serialize_cmd(std::move(cmd_2)).get0())
                       .get0();
        auto res_3 = dispatcher
                       .apply_update(serialize_cmd(std::move(cmd_3)).get0())
                       .get0();

        BOOST_REQUIRE_EQUAL(res_1, cluster::errc::success);
        BOOST_REQUIRE_EQUAL(res_2, cluster::errc::success);
        BOOST_REQUIRE_EQUAL(res_3, cluster::errc::success);
    }

    cluster::topic_updates_dispatcher dispatcher;
};

constexpr uint64_t node_initial_capacity(uint32_t cores) {
    return (topic_table_fixture::partitions_per_shard * cores)
           - topic_table_fixture::partitions_reserve_shard0;
}

uint64_t
current_cluster_capacity(const cluster::allocation_state::underlying_t& nodes) {
    return std::accumulate(
      nodes.begin(),
      nodes.end(),
      0,
      [](
        uint64_t acc,
        const cluster::allocation_state::underlying_t::value_type& p) {
          return acc + p.second->partition_capacity();
      });
}

constexpr uint64_t max_cluster_capacity() {
    return node_initial_capacity(12) + node_initial_capacity(8)
           + node_initial_capacity(4);
}

FIXTURE_TEST(
  test_dispatching_happy_path_create, topic_table_updates_dispatcher_fixture) {
    create_topics();
    auto md = table.local().all_topics_metadata();

    BOOST_REQUIRE_EQUAL(md.size(), 3);

    BOOST_REQUIRE_EQUAL(md.contains(make_tp_ns("test_tp_1")), true);
    BOOST_REQUIRE_EQUAL(md.contains(make_tp_ns("test_tp_2")), true);
    BOOST_REQUIRE_EQUAL(md.contains(make_tp_ns("test_tp_3")), true);

    BOOST_REQUIRE_EQUAL(
      md.find(make_tp_ns("test_tp_1"))->second.get_assignments().size(), 1);
    BOOST_REQUIRE_EQUAL(
      md.find(make_tp_ns("test_tp_2"))->second.get_assignments().size(), 12);
    BOOST_REQUIRE_EQUAL(
      md.find(make_tp_ns("test_tp_3"))->second.get_assignments().size(), 8);
    // Initial capacity
    // (cpus * max_allocations_per_core) - core0_extra_weight;
    // node 1, 8 cores
    // node 2, 12 cores
    // node 3, 4 cores

    // topics:
    //
    // test_tp_1, partitions: 1, replication factor: 3
    // test_tp_2, partitions: 12, replication factor: 3
    // test_tp_3, partitions: 8, replication factor: 1

    BOOST_REQUIRE_EQUAL(
      current_cluster_capacity(allocator.local().state().allocation_nodes()),
      max_cluster_capacity() - (1 * 3 + 12 * 3 + 8 * 1));
}

FIXTURE_TEST(
  test_dispatching_happy_path_delete, topic_table_updates_dispatcher_fixture) {
    create_topics();
    dispatcher
      .apply_update(
        serialize_cmd(cluster::delete_topic_cmd(
                        make_tp_ns("test_tp_2"), make_tp_ns("test_tp_2")))
          .get0())
      .get0();
    dispatcher
      .apply_update(
        serialize_cmd(cluster::delete_topic_cmd(
                        make_tp_ns("test_tp_3"), make_tp_ns("test_tp_3")))
          .get0())
      .get0();

    auto md = table.local().all_topics_metadata();
    BOOST_REQUIRE_EQUAL(md.size(), 1);

    BOOST_REQUIRE_EQUAL(md.contains(make_tp_ns("test_tp_1")), true);
    BOOST_REQUIRE_EQUAL(
      md.find(make_tp_ns("test_tp_1"))->second.get_assignments().size(), 1);

    BOOST_REQUIRE_EQUAL(
      current_cluster_capacity(allocator.local().state().allocation_nodes()),
      max_cluster_capacity() - 3);
}

FIXTURE_TEST(
  test_dispatching_conflicts, topic_table_updates_dispatcher_fixture) {
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

    BOOST_REQUIRE_EQUAL(
      current_cluster_capacity(allocator.local().state().allocation_nodes()),
      max_cluster_capacity() - (1 * 3 + 12 * 3 + 8 * 1));
}
