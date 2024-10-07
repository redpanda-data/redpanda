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

ss::logger logger{"dispatcher_test"};

struct topic_table_updates_dispatcher_fixture : topic_table_fixture {
    topic_table_updates_dispatcher_fixture()
      : dispatcher(allocator, table, leaders, pb_state) {}

    template<typename Cmd>
    void dispatch_command(Cmd cmd) {
        auto res
          = dispatcher.apply_update(serde_serialize_cmd(std::move(cmd))).get();
        BOOST_REQUIRE_EQUAL(res, cluster::errc::success);
    }

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

        dispatch_command(std::move(cmd_1));
        dispatch_command(std::move(cmd_2));
        dispatch_command(std::move(cmd_3));
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
    auto& md = table.local().all_topics_metadata();

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
    BOOST_REQUIRE(
      !dispatcher
         .apply_update(serde_serialize_cmd(cluster::delete_topic_cmd(
           make_tp_ns("test_tp_2"), make_tp_ns("test_tp_2"))))
         .get());
    BOOST_REQUIRE(
      !dispatcher
         .apply_update(serde_serialize_cmd(cluster::delete_topic_cmd(
           make_tp_ns("test_tp_3"), make_tp_ns("test_tp_3"))))
         .get());

    auto& md = table.local().all_topics_metadata();
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

    std::vector<cluster::topic_table_ntp_delta> deltas;
    table.local().register_ntp_delta_notification(
      [&](const auto& d) { deltas.insert(deltas.end(), d.begin(), d.end()); });

    auto res_1 = table.local()
                   .apply(
                     cluster::delete_topic_cmd(
                       make_tp_ns("not_exists"), make_tp_ns("not_exists")),
                     model::offset(0))
                   .get();
    BOOST_REQUIRE_EQUAL(res_1, cluster::errc::topic_not_exists);

    auto res_2 = table.local()
                   .apply(
                     make_create_topic_cmd("test_tp_1", 2, 3), model::offset(0))
                   .get();
    BOOST_REQUIRE_EQUAL(res_2, cluster::errc::topic_already_exists);
    BOOST_REQUIRE_EQUAL(deltas.size(), 0);

    BOOST_REQUIRE_EQUAL(
      current_cluster_capacity(allocator.local().state().allocation_nodes()),
      max_cluster_capacity() - (1 * 3 + 12 * 3 + 8 * 1));
}

FIXTURE_TEST(
  allocator_partition_counts, topic_table_updates_dispatcher_fixture) {
    const auto& allocation_nodes = allocator.local().state().allocation_nodes();

    auto check_allocated_counts = [&](std::vector<size_t> expected) {
        std::vector<size_t> counts;
        for (const auto& [id, node] : allocation_nodes) {
            BOOST_REQUIRE(id() == counts.size() + 1); // 1-based node ids
            counts.push_back(node->allocated_partitions());
        }
        logger.debug("allocated counts: {}, expected: {}", counts, expected);
        BOOST_CHECK_EQUAL(counts, expected);
    };

    auto check_final_counts = [&](std::vector<size_t> expected) {
        std::vector<size_t> counts;
        for (const auto& [id, node] : allocation_nodes) {
            BOOST_REQUIRE(id() == counts.size() + 1); // 1-based node ids
            counts.push_back(node->final_partitions());
        }
        logger.debug("final counts: {}, expected: {}", counts, expected);
        BOOST_CHECK_EQUAL(counts, expected);
    };

    auto create_topic_cmd = make_create_topic_cmd("test_tp_1", 4, 3);
    logger.info("create topic {}", create_topic_cmd.key);
    dispatch_command(create_topic_cmd);

    // create a node to move replicas to
    allocator.local().register_node(
      create_allocation_node(model::node_id(4), 4));

    check_allocated_counts({4, 4, 4, 0});
    check_final_counts({4, 4, 4, 0});

    // get data needed to move a partition
    auto get_partition = [&](model::partition_id::type id) {
        model::ntp ntp{
          create_topic_cmd.key.ns,
          create_topic_cmd.key.tp,
          model::partition_id{id}};
        auto assignment_it = std::next(
          create_topic_cmd.value.assignments.begin(), id);
        BOOST_REQUIRE(assignment_it->id() == id);

        auto old_replicas = assignment_it->replicas;

        auto new_replicas = old_replicas;
        auto it = std::find_if(
          new_replicas.begin(), new_replicas.end(), [](const auto& bs) {
              return bs.node_id() == 1;
          });
        BOOST_REQUIRE(it != new_replicas.end());
        it->node_id = model::node_id{4};
        it->shard = random_generators::get_int(3);

        return std::tuple{
          ntp,
          old_replicas,
          new_replicas,
        };
    };

    // move + finish
    {
        auto [ntp, old_replicas, new_replicas] = get_partition(0);

        logger.info("move ntp {}", ntp);
        dispatch_command(
          cluster::move_partition_replicas_cmd{ntp, new_replicas});
        check_allocated_counts({4, 4, 4, 1});
        check_final_counts({3, 4, 4, 1});

        logger.info("finish move");
        dispatch_command(
          cluster::finish_moving_partition_replicas_cmd{ntp, new_replicas});
        check_allocated_counts({3, 4, 4, 1});
        check_final_counts({3, 4, 4, 1});
    }

    // move + cancel + force_cancel + finish
    {
        auto [ntp, old_replicas, new_replicas] = get_partition(1);

        logger.info("move ntp {}", ntp);
        dispatch_command(
          cluster::move_partition_replicas_cmd{ntp, new_replicas});
        check_allocated_counts({3, 4, 4, 2});
        check_final_counts({2, 4, 4, 2});

        logger.info("cancel move");
        dispatch_command(cluster::cancel_moving_partition_replicas_cmd{
          ntp,
          cluster::cancel_moving_partition_replicas_cmd_data{
            cluster::force_abort_update{false}}});
        check_allocated_counts({3, 4, 4, 2});
        check_final_counts({3, 4, 4, 1});

        logger.info("force-cancel move");
        dispatch_command(cluster::cancel_moving_partition_replicas_cmd{
          ntp,
          cluster::cancel_moving_partition_replicas_cmd_data{
            cluster::force_abort_update{true}}});
        check_allocated_counts({3, 4, 4, 2});
        check_final_counts({3, 4, 4, 1});

        logger.info("finish move");
        dispatch_command(
          cluster::finish_moving_partition_replicas_cmd{ntp, old_replicas});
        check_allocated_counts({3, 4, 4, 1});
        check_final_counts({3, 4, 4, 1});
    }

    // move + cancel + revert_cancel
    {
        auto [ntp, old_replicas, new_replicas] = get_partition(2);

        logger.info("move ntp {}", ntp);
        dispatch_command(
          cluster::move_partition_replicas_cmd{ntp, new_replicas});
        check_allocated_counts({3, 4, 4, 2});
        check_final_counts({2, 4, 4, 2});

        logger.info("cancel move");
        dispatch_command(cluster::cancel_moving_partition_replicas_cmd{
          ntp,
          cluster::cancel_moving_partition_replicas_cmd_data{
            cluster::force_abort_update{false}}});
        check_allocated_counts({3, 4, 4, 2});
        check_final_counts({3, 4, 4, 1});

        logger.info("revert_cancel move");
        dispatch_command(cluster::revert_cancel_partition_move_cmd(
          int8_t{0},
          cluster::revert_cancel_partition_move_cmd_data{.ntp = ntp}));
        check_allocated_counts({2, 4, 4, 2});
        check_final_counts({2, 4, 4, 2});
    }

    // force_move
    {
        auto [ntp, old_replicas, new_replicas] = get_partition(3);

        // for new_replicas choose a proper subset of old replicas, as required
        // by force_partition_reconfiguration.
        auto repl_it = std::find_if(
          old_replicas.begin(), old_replicas.end(), [](const auto& bs) {
              return bs.node_id() == 1;
          });
        BOOST_REQUIRE(repl_it != old_replicas.end());
        new_replicas = std::vector({*repl_it});

        logger.info(
          "force_partition_reconfiguration ntp {} to {}", ntp, new_replicas);
        dispatch_command(cluster::force_partition_reconfiguration_cmd{
          ntp,
          cluster::force_partition_reconfiguration_cmd_data(new_replicas)});
        check_allocated_counts({2, 4, 4, 2});
        check_final_counts({2, 3, 3, 2});

        logger.info("finish move");
        dispatch_command(
          cluster::finish_moving_partition_replicas_cmd{ntp, new_replicas});
        check_allocated_counts({2, 3, 3, 2});
        check_final_counts({2, 3, 3, 2});
    }

    // move topic + topic delete
    {
        // move everything back
        logger.info("move topic");
        std::vector<cluster::move_topic_replicas_data> cmd_data;
        for (const auto& p_as : create_topic_cmd.value.assignments) {
            cmd_data.emplace_back(p_as.id, p_as.replicas);
        }
        dispatch_command(
          cluster::move_topic_replicas_cmd(create_topic_cmd.key, cmd_data));
        check_allocated_counts({4, 4, 4, 2});
        check_final_counts({4, 4, 4, 0});

        logger.info("delete topic");
        dispatch_command(cluster::delete_topic_cmd(
          create_topic_cmd.key, create_topic_cmd.key));
        check_allocated_counts({0, 0, 0, 0});
        check_final_counts({0, 0, 0, 0});
    }
}
