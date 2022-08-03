/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cluster/health_monitor_types.h"
#include "cluster/partition_balancer_types.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/topic_table.h"

#include <absl/container/flat_hash_map.h>

namespace cluster {

struct ntp_reassignments {
    model::ntp ntp;
    allocation_units allocation_units;
};

struct planner_config {
    // If node disk usage goes over this ratio planner will actively move
    // partitions away from the node.
    double soft_max_disk_usage_ratio;
    // Planner won't plan a move that will result in destination node(s) going
    // over this ratio.
    double hard_max_disk_usage_ratio;
    // Size of partitions that can be planned to move in one request
    size_t movement_disk_size_batch;
    std::chrono::seconds node_availability_timeout_sec;
};

class partition_balancer_planner {
public:
    partition_balancer_planner(
      planner_config config,
      topic_table& topic_table,
      partition_allocator& partition_allocator);

    enum class status {
        empty,
        movement_planned,
        cancellations_planned,
        waiting_for_reports,
    };

    struct plan_data {
        partition_balancer_violations violations;
        std::vector<ntp_reassignments> reassignments;
        std::vector<model::ntp> cancellations;
        size_t failed_reassignments_count = 0;
        status status = status::empty;
    };

    plan_data plan_reassignments(
      const cluster_health_report&, const std::vector<raft::follower_metrics>&);

private:
    struct reallocation_request_state {
        uint64_t planned_movement_disk_size = 0;
        absl::flat_hash_map<model::ntp, size_t> ntp_sizes;
        absl::flat_hash_map<model::node_id, node_disk_space> node_disk_reports;
        absl::flat_hash_set<model::node_id> unavailable_nodes;
        // Partitions that are planned to move in current planner request
        absl::flat_hash_set<model::ntp> moving_partitions;
    };

    partition_constraints get_partition_constraints(
      const partition_assignment& assignments,
      const topic_metadata& topic_metadata,
      size_t partition_size,
      double max_disk_usage_ratio,
      reallocation_request_state&) const;

    result<allocation_units> get_reallocation(
      const model::ntp&,
      const partition_assignment&,
      size_t partition_size,
      partition_constraints,
      const std::vector<model::broker_shard>& stable_replicas,
      reallocation_request_state&);

    void get_unavailable_nodes_reassignments(
      plan_data&, reallocation_request_state&);

    void get_full_node_reassignments(plan_data&, reallocation_request_state&);

    void get_unavailable_node_movement_cancellations(
      std::vector<model::ntp>& cancellations,
      const reallocation_request_state&);

    void calculate_nodes_with_disk_constraints_violation(
      reallocation_request_state&, plan_data&);

    void calculate_unavailable_nodes(
      const std::vector<raft::follower_metrics>&,
      reallocation_request_state&,
      plan_data&);

    bool is_partition_movement_possible(
      const std::vector<model::broker_shard>& current_replicas,
      const reallocation_request_state&);

    void init_node_disk_reports_from_health_report(
      const cluster_health_report& health_report, reallocation_request_state&);

    void init_ntp_sizes_from_health_report(
      const cluster_health_report& health_report, reallocation_request_state&);

    std::optional<size_t> get_partition_size(
      const model::ntp& ntp, const reallocation_request_state&);

    bool all_reports_received(const reallocation_request_state&);

    planner_config _config;
    topic_table& _topic_table;
    partition_allocator& _partition_allocator;
};

} // namespace cluster
