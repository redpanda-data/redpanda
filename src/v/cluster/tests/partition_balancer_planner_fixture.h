/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/members_table.h"
#include "cluster/partition_balancer_planner.h"
#include "cluster/tests/utils.h"
#include "cluster/topic_updates_dispatcher.h"
#include "model/metadata.h"
#include "test_utils/fixture.h"
#include "units.h"

#include <chrono>
#include <optional>

constexpr uint64_t node_size = 200_MiB;
constexpr uint64_t full_node_free_size = 5_MiB;
constexpr uint64_t nearly_full_node_free_size = 41_MiB;
constexpr uint64_t default_partition_size = 10_MiB;
constexpr uint64_t not_full_node_free_size = 150_MiB;
constexpr uint64_t reallocation_batch_size = default_partition_size * 2 - 1_MiB;
constexpr std::chrono::seconds node_unavailable_timeout = std::chrono::minutes(
  5);

static constexpr uint32_t partitions_per_shard = 7000;
static constexpr uint32_t partitions_reserve_shard0 = 2;

static std::unique_ptr<cluster::allocation_node> create_allocation_node(
  model::node_id nid,
  uint32_t cores,
  const std::optional<model::rack_id>& rack_id = std::nullopt) {
    return std::make_unique<cluster::allocation_node>(
      nid,
      cores,
      absl::node_hash_map<ss::sstring, ss::sstring>{},
      rack_id,
      config::mock_binding<uint32_t>(uint32_t{partitions_per_shard}),
      config::mock_binding<uint32_t>(uint32_t{partitions_reserve_shard0}));
}

struct controller_workers {
public:
    controller_workers()
      : dispatcher(allocator, table, leaders) {
        table.start().get();
        members.start_single().get();
        allocator
          .start_single(
            std::ref(members),
            config::mock_binding<std::optional<size_t>>(std::nullopt),
            config::mock_binding<std::optional<int32_t>>(std::nullopt),
            config::mock_binding<uint32_t>(uint32_t{partitions_per_shard}),
            config::mock_binding<uint32_t>(uint32_t{partitions_reserve_shard0}),
            config::mock_binding<bool>(true))
          .get();
    }

    ~controller_workers() {
        table.stop().get();
        allocator.stop().get();
        members.stop().get();
    }

    ss::sharded<cluster::members_table> members;
    ss::sharded<cluster::partition_allocator> allocator;
    ss::sharded<cluster::topic_table> table;
    ss::sharded<cluster::partition_leaders_table> leaders;
    cluster::topic_updates_dispatcher dispatcher;
};

struct partition_balancer_planner_fixture {
    partition_balancer_planner_fixture()
      : planner(
        cluster::planner_config{
          .soft_max_disk_usage_ratio = 0.8,
          .hard_max_disk_usage_ratio = 0.95,
          .movement_disk_size_batch = reallocation_batch_size,
          .node_availability_timeout_sec = std::chrono::minutes(1)},
        workers.table.local(),
        workers.members.local(),
        workers.allocator.local())
      , workers() {}

    cluster::topic_configuration_assignment make_tp_configuration(
      const ss::sstring& topic, int partitions, int16_t replication_factor) {
        cluster::topic_configuration cfg(
          test_ns, model::topic(topic), partitions, replication_factor);

        cluster::allocation_request req;
        req.partitions.reserve(partitions);
        for (auto p = 0; p < partitions; ++p) {
            req.partitions.emplace_back(
              model::partition_id(p), replication_factor);
        }

        auto pas = workers.allocator.local()
                     .allocate(std::move(req))
                     .value()
                     ->get_assignments();

        return {cfg, std::move(pas)};
    }

    cluster::create_topic_cmd make_create_topic_cmd(
      const ss::sstring& name, int partitions, int16_t replication_factor) {
        return {
          make_tp_ns(name),
          make_tp_configuration(name, partitions, replication_factor)};
    }

    model::topic_namespace make_tp_ns(const ss::sstring& tp) {
        return {test_ns, model::topic(tp)};
    }

    void create_topic(
      const ss::sstring& name, int partitions, int16_t replication_factor) {
        auto cmd = make_create_topic_cmd(name, partitions, replication_factor);
        auto res = workers.dispatcher
                     .apply_update(serialize_cmd(std::move(cmd)).get())
                     .get();
        BOOST_REQUIRE_EQUAL(res, cluster::errc::success);
    }

    void allocator_register_nodes(
      size_t nodes_amount,
      const std::optional<model::rack_id>& rack_id = std::nullopt) {
        auto& members_table = workers.members.local();

        std::vector<model::broker> new_brokers;
        for (auto broker_ptr : members_table.all_brokers()) {
            new_brokers.push_back(*broker_ptr);
        }

        for (size_t i = 0; i < nodes_amount; ++i) {
            workers.allocator.local().register_node(create_allocation_node(
              model::node_id(last_node_idx), 4, rack_id));
            new_brokers.push_back(model::broker(
              model::node_id(last_node_idx),
              net::unresolved_address{},
              net::unresolved_address{},
              rack_id,
              model::broker_properties{
                .cores = 4,
                .available_memory_gb = 2,
                .available_disk_gb = 100}));
            last_node_idx++;
        }

        members_table.update_brokers(model::offset{}, new_brokers);
    }

    cluster::move_partition_replicas_cmd make_move_partition_replicas_cmd(
      model::ntp ntp, std::vector<model::broker_shard> replica_set) {
        return cluster::move_partition_replicas_cmd(
          std::move(ntp), std::move(replica_set));
    }

    void move_partition_replicas(
      const model::ntp& ntp,
      const std::vector<model::broker_shard>& new_replicas) {
        auto cmd = make_move_partition_replicas_cmd(ntp, new_replicas);
        auto res = workers.dispatcher
                     .apply_update(serialize_cmd(std::move(cmd)).get())
                     .get();
        BOOST_REQUIRE_EQUAL(res, cluster::errc::success);
    }

    void move_partition_replicas(cluster::ntp_reassignments& reassignment) {
        move_partition_replicas(
          reassignment.ntp,
          reassignment.allocation_units.get_assignments().front().replicas);
    }

    std::vector<raft::follower_metrics>
    create_follower_metrics(const std::set<size_t>& unavailable_nodes = {}) {
        std::vector<raft::follower_metrics> metrics;
        metrics.reserve(last_node_idx);
        for (size_t i = 0; i < last_node_idx; ++i) {
            if (unavailable_nodes.contains(i)) {
                metrics.push_back(raft::follower_metrics{
                  .id = model::node_id(i),
                  .last_heartbeat = raft::clock_type::now()
                                    - node_unavailable_timeout,
                  .is_live = false,
                });
            } else {
                metrics.push_back(raft::follower_metrics{
                  .id = model::node_id(i),
                  .last_heartbeat = raft::clock_type::now(),
                  .is_live = true,
                });
            }
        }
        return metrics;
    }

    cluster::cluster_health_report create_health_report(
      const std::set<size_t>& full_nodes = {},
      const std::set<size_t>& nearly_full_nodes = {},
      uint64_t partition_size = default_partition_size) {
        cluster::cluster_health_report health_report;
        std::vector<cluster::topic_status> topics;
        for (const auto& topic : workers.table.local().topics_map()) {
            cluster::topic_status ts;
            ts.tp_ns = topic.second.get_configuration().tp_ns;
            for (size_t i = 0;
                 i < topic.second.get_configuration().partition_count;
                 ++i) {
                cluster::partition_status ps;
                ps.id = model::partition_id(i);
                ps.size_bytes = partition_size;
                ts.partitions.push_back(ps);
            }
            topics.push_back(ts);
        }
        for (int i = 0; i < last_node_idx; ++i) {
            cluster::node_health_report node_report;
            storage::disk node_disk{
              .free = not_full_node_free_size, .total = node_size};
            if (full_nodes.contains(i)) {
                node_disk.free = full_node_free_size;
            } else if (nearly_full_nodes.contains(i)) {
                node_disk.free = nearly_full_node_free_size;
            }
            node_report.id = model::node_id(i);
            node_report.local_state.disks.push_back(node_disk);
            health_report.node_reports.push_back(node_report);
        }
        health_report.node_reports[0].topics = topics;
        return health_report;
    }

    void set_maintenance_mode(model::node_id id) {
        workers.members.local().apply(
          model::offset{}, cluster::maintenance_mode_cmd(id, true));
        auto broker = workers.members.local().get_broker(id);
        BOOST_REQUIRE(broker);
        BOOST_REQUIRE(
          broker.value()->get_maintenance_state()
          == model::maintenance_state::active);
    }

    void set_decommissioning(model::node_id id) {
        workers.members.local().apply(
          model::offset{}, cluster::decommission_node_cmd(id, 0));
        auto broker = workers.members.local().get_broker(id);
        BOOST_REQUIRE(broker);
        BOOST_REQUIRE(
          broker.value()->get_membership_state()
          == model::membership_state::draining);
    }

    controller_workers workers;
    cluster::partition_balancer_planner planner;
    int last_node_idx{};
};
