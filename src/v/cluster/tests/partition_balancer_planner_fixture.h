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

#include "base/units.h"
#include "cluster/commands.h"
#include "cluster/data_migrated_resources.h"
#include "cluster/health_monitor_types.h"
#include "cluster/members_table.h"
#include "cluster/node_status_table.h"
#include "cluster/partition_balancer_planner.h"
#include "cluster/partition_balancer_state.h"
#include "cluster/scheduling/allocation_node.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/tests/utils.h"
#include "cluster/topic_updates_dispatcher.h"
#include "cluster/types.h"
#include "container/fragmented_vector.h"
#include "features/feature_table.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "random/generators.h"
#include "test_utils/fixture.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>

#include <chrono>
#include <optional>

constexpr uint64_t node_size = 200_MiB;
constexpr uint64_t full_node_free_size = 5_MiB;
constexpr uint64_t nearly_full_node_free_size = 41_MiB;
constexpr uint64_t default_partition_size = 10_MiB;
constexpr uint64_t not_full_node_free_size = 150_MiB;
constexpr std::chrono::seconds node_unavailable_timeout = std::chrono::minutes(
  5);

static constexpr uint32_t partitions_per_shard = 7000;
static constexpr uint32_t partitions_reserve_shard0 = 2;

static std::unique_ptr<cluster::allocation_node>
create_allocation_node(model::node_id nid, uint32_t cores) {
    return std::make_unique<cluster::allocation_node>(
      nid,
      cores,
      config::mock_binding<uint32_t>(uint32_t{partitions_per_shard}),
      config::mock_binding<uint32_t>(uint32_t{partitions_reserve_shard0}),
      config::mock_binding<std::vector<ss::sstring>>({}));
}

struct controller_workers {
public:
    controller_workers()
      : dispatcher(allocator, table, leaders, state) {
        migrated_resources.start().get();
        table
          .start(ss::sharded_parameter(
            [this] { return std::ref(migrated_resources.local()); }))
          .get();
        members.start_single().get();
        features.start().get();
        features
          .invoke_on_all(
            [](features::feature_table& f) { f.testing_activate_all(); })
          .get();
        allocator
          .start_single(
            std::ref(members),
            std::ref(features),
            config::mock_binding<std::optional<size_t>>(std::nullopt),
            config::mock_binding<std::optional<int32_t>>(std::nullopt),
            config::mock_binding<uint32_t>(uint32_t{partitions_per_shard}),
            config::mock_binding<uint32_t>(uint32_t{partitions_reserve_shard0}),
            config::mock_binding<std::vector<ss::sstring>>(
              std::vector<ss::sstring>{
                {model::kafka_audit_logging_topic,
                 "__consumer_offsets",
                 "_schemas"}}),
            config::mock_binding<bool>(true))
          .get();
        // use node status that is not used in test as self is always available
        node_status_table.start_single(model::node_id{123}).get();
        state
          .start_single(
            std::ref(table),
            std::ref(members),
            std::ref(allocator),
            std::ref(node_status_table))
          .get();
    }

    template<typename Cmd>
    void dispatch_topic_command(Cmd cmd) {
        auto res
          = dispatcher.apply_update(serde_serialize_cmd(std::move(cmd))).get();
        BOOST_REQUIRE_EQUAL(res, cluster::errc::success);
    }

    cluster::topic_configuration_assignment make_tp_configuration(
      const ss::sstring& topic, int partitions, int16_t replication_factor) {
        cluster::topic_configuration cfg(
          test_ns, model::topic(topic), partitions, replication_factor);

        cluster::allocation_request req(cfg.tp_ns);
        req.partitions.reserve(partitions);
        for (auto p = 0; p < partitions; ++p) {
            req.partitions.emplace_back(
              model::partition_id(p), replication_factor);
        }
        // enable topic-aware placement
        req.existing_replica_counts = cluster::node2count_t{};

        auto pas = allocator.local()
                     .allocate(std::move(req))
                     .get()
                     .value()
                     ->copy_assignments();

        return {cfg, std::move(pas)};
    }

    void set_maintenance_mode(model::node_id id) {
        BOOST_REQUIRE(!members.local().apply(
          model::offset{}, cluster::maintenance_mode_cmd(id, true)));
        auto broker = members.local().get_node_metadata_ref(id);
        BOOST_REQUIRE(broker);
        BOOST_REQUIRE(
          broker.value().get().state.get_maintenance_state()
          == model::maintenance_state::active);
    }

    void set_decommissioning(model::node_id id) {
        BOOST_REQUIRE(!members.local().apply(
          model::offset{}, cluster::decommission_node_cmd(id, 0)));
        allocator.local().decommission_node(id);
        auto broker = members.local().get_node_metadata_ref(id);
        BOOST_REQUIRE(broker);
        BOOST_REQUIRE(
          broker.value().get().state.get_membership_state()
          == model::membership_state::draining);
    }

    ~controller_workers() {
        state.stop().get();
        node_status_table.stop().get();
        table.stop().get();
        allocator.stop().get();
        features.stop().get();
        members.stop().get();
        migrated_resources.stop().get();
    }

    ss::sharded<cluster::members_table> members;
    ss::sharded<features::feature_table> features;
    ss::sharded<cluster::partition_allocator> allocator;
    ss::sharded<cluster::topic_table> table;
    ss::sharded<cluster::partition_leaders_table> leaders;
    ss::sharded<cluster::partition_balancer_state> state;
    ss::sharded<cluster::node_status_table> node_status_table;
    cluster::topic_updates_dispatcher dispatcher;
    ss::sharded<cluster::data_migrations::migrated_resources>
      migrated_resources;
};

struct partition_balancer_planner_fixture {
    cluster::partition_balancer_planner make_planner(
      model::partition_autobalancing_mode mode
      = model::partition_autobalancing_mode::continuous,
      size_t max_concurrent_actions = 2,
      bool request_ondemand_rebalance = false) {
        return cluster::partition_balancer_planner(
          cluster::planner_config{
            .mode = mode,
            .soft_max_disk_usage_ratio = 0.8,
            .hard_max_disk_usage_ratio = 0.95,
            .max_concurrent_actions = max_concurrent_actions,
            .node_availability_timeout_sec = std::chrono::minutes(1),
            .ondemand_rebalance_requested = request_ondemand_rebalance,
            .segment_fallocation_step = 16,
            .node_responsiveness_timeout = std::chrono::seconds(10),
            .topic_aware = true,
          },
          workers.state.local(),
          workers.allocator.local());
    }

    model::topic_namespace make_tp_ns(const ss::sstring& tp) {
        return {test_ns, model::topic(tp)};
    }

    void create_topic(
      const ss::sstring& name, int partitions, int16_t replication_factor) {
        cluster::create_topic_cmd cmd{
          make_tp_ns(name),
          workers.make_tp_configuration(name, partitions, replication_factor)};
        workers.dispatch_topic_command(std::move(cmd));
    }

    void create_topic(
      const ss::sstring& name,
      std::vector<std::vector<model::node_id>> partition_nodes) {
        BOOST_REQUIRE(!partition_nodes.empty());
        int16_t replication_factor = partition_nodes.front().size();
        cluster::topic_configuration cfg(
          test_ns,
          model::topic{name},
          partition_nodes.size(),
          replication_factor);

        ss::chunked_fifo<cluster::partition_assignment> assignments;
        for (model::partition_id::type i = 0; i < partition_nodes.size(); ++i) {
            const auto& nodes = partition_nodes[i];
            BOOST_REQUIRE_EQUAL(nodes.size(), replication_factor);
            std::vector<model::broker_shard> replicas;
            for (model::node_id n : nodes) {
                replicas.push_back(model::broker_shard{
                  n, random_generators::get_int<uint32_t>(0, 3)});
            }
            assignments.push_back(cluster::partition_assignment{
              raft::group_id{1}, model::partition_id{i}, replicas});
        }
        cluster::create_topic_cmd cmd{
          make_tp_ns(name),
          cluster::topic_configuration_assignment(cfg, std::move(assignments))};

        workers.dispatch_topic_command(std::move(cmd));
    }

    void allocator_register_nodes(
      size_t nodes_amount, const std::vector<ss::sstring>& rack_ids = {}) {
        if (!rack_ids.empty()) {
            vassert(
              rack_ids.size() == nodes_amount,
              "mismatch between rack ids: {} and the number of new nodes: {}",
              rack_ids,
              nodes_amount);
        }

        auto& members_table = workers.members.local();

        std::vector<model::broker> new_brokers;
        new_brokers.reserve(nodes_amount);
        for (size_t i = 0; i < nodes_amount; ++i) {
            std::optional<model::rack_id> rack_id;
            if (!rack_ids.empty()) {
                rack_id = model::rack_id{rack_ids[i]};
            }

            workers.allocator.local().register_node(
              create_allocation_node(model::node_id(last_node_idx), 4));
            new_brokers.emplace_back(
              model::node_id(last_node_idx),
              net::unresolved_address{},
              net::unresolved_address{},
              rack_id,
              model::broker_properties{
                .cores = 4,
                .available_memory_gb = 2,
                .available_disk_gb = 100});
            last_node_idx++;
        }
        for (auto& b : new_brokers) {
            BOOST_REQUIRE(!members_table.apply(
              model::offset{}, cluster::add_node_cmd(b.id(), b)));
        }
    }

    cluster::move_partition_replicas_cmd make_move_partition_replicas_cmd(
      model::ntp ntp, std::vector<model::broker_shard> replica_set) {
        return cluster::move_partition_replicas_cmd(
          std::move(ntp), std::move(replica_set));
    }

    void move_partition_replicas(
      const model::ntp& ntp,
      const std::vector<model::broker_shard>& new_replicas) {
        workers.dispatch_topic_command(
          make_move_partition_replicas_cmd(ntp, new_replicas));
    }

    void move_partition_replicas(
      model::ntp ntp, const std::vector<model::node_id>& new_nodes) {
        std::vector<model::broker_shard> new_replicas;
        for (auto n : new_nodes) {
            new_replicas.push_back(model::broker_shard{
              n, random_generators::get_int<uint32_t>(0, 3)});
        }
        move_partition_replicas(std::move(ntp), std::move(new_replicas));
    }

    void move_partition_replicas(cluster::ntp_reassignment& reassignment) {
        move_partition_replicas(
          reassignment.ntp, reassignment.allocated.replicas());
    }

    void cancel_partition_move(model::ntp ntp) {
        cluster::cancel_moving_partition_replicas_cmd cmd{
          std::move(ntp),
          cluster::cancel_moving_partition_replicas_cmd_data{
            cluster::force_abort_update{false}}};
        workers.dispatch_topic_command(std::move(cmd));
    }

    void finish_partition_move(model::ntp ntp) {
        auto cur_assignment = workers.table.local().get_partition_assignment(
          ntp);
        BOOST_REQUIRE(cur_assignment);

        cluster::finish_moving_partition_replicas_cmd cmd{
          std::move(ntp), cur_assignment->replicas};

        workers.dispatch_topic_command(std::move(cmd));
    }

    void delete_topic(const model::topic& topic) {
        cluster::delete_topic_cmd cmd{make_tp_ns(topic()), make_tp_ns(topic())};
        workers.dispatch_topic_command(std::move(cmd));
    }

    ss::future<>
    populate_node_status_table(std::set<size_t> unavailable_nodes = {}) {
        std::vector<cluster::node_status> status_updates;
        status_updates.reserve(last_node_idx + 1);
        for (size_t i = 0; i < last_node_idx; ++i) {
            auto last_seen = raft::clock_type::now();
            if (unavailable_nodes.contains(i)) {
                last_seen = last_seen - node_unavailable_timeout;
            }
            status_updates.push_back(cluster::node_status{
              .node_id = model::node_id(i),
              .last_seen = last_seen,
            });
        }

        co_await workers.node_status_table.invoke_on_all(
          [status_updates](cluster::node_status_table& nts) {
              nts.update_peers(status_updates);
          });
    }

    cluster::cluster_health_report create_health_report(
      const std::set<size_t>& full_nodes = {},
      const std::set<size_t>& nearly_full_nodes = {},
      uint64_t partition_size = default_partition_size) {
        cluster::cluster_health_report health_report;
        chunked_vector<cluster::topic_status> topics;
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
            storage::disk node_disk{
              .free = not_full_node_free_size, .total = node_size};
            if (full_nodes.contains(i)) {
                node_disk.free = full_node_free_size;
            } else if (nearly_full_nodes.contains(i)) {
                node_disk.free = nearly_full_node_free_size;
            }
            cluster::node::local_state local_state;
            local_state.log_data_size = {
              .data_target_size = node_disk.total,
              .data_current_size = node_disk.total - node_disk.free,
              .data_reclaimable_size = 0};
            local_state.set_disk(node_disk);
            chunked_vector<cluster::topic_status> node_topics;
            if (i == 0) {
                node_topics = topics.copy();
            }
            health_report.node_reports.emplace_back(
              ss::make_lw_shared<cluster::node_health_report>(
                model::node_id(i),
                local_state,
                std::move(node_topics),
                std::nullopt));
        }

        return health_report;
    }

    void set_maintenance_mode(model::node_id id) {
        workers.set_maintenance_mode(id);
    }

    void set_decommissioning(model::node_id id) {
        workers.set_decommissioning(id);
    }

    controller_workers workers;
    int last_node_idx{};
    ss::abort_source as;
};
