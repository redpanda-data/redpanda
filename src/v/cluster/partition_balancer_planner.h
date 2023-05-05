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

struct ntp_reassignment {
    model::ntp ntp;
    allocated_partition allocated;
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
    // Fallocation step used to calculate upperbound for partition size
    size_t segment_fallocation_step;
};

class partition_balancer_planner {
public:
    partition_balancer_planner(
      planner_config config,
      partition_balancer_state& state,
      partition_allocator& partition_allocator);

    enum class status {
        empty,
        actions_planned,
        waiting_for_maintenance_end,
        waiting_for_reports,
    };

    struct plan_data {
        partition_balancer_violations violations;
        std::vector<ntp_reassignment> reassignments;
        std::vector<model::ntp> cancellations;
        size_t failed_reassignments_count = 0;
        status status = status::empty;
    };

    plan_data plan_reassignments(
      const cluster_health_report&, const std::vector<raft::follower_metrics>&);

private:
    class request_context;

    partition_constraints get_partition_constraints(
      const partition_assignment& assignments,
      size_t partition_size,
      double max_disk_usage_ratio,
      request_context&) const;

    result<allocated_partition> get_reallocation(
      const model::ntp&,
      const partition_assignment&,
      size_t partition_size,
      partition_constraints,
      const std::vector<model::broker_shard>& stable_replicas,
      request_context&);

    void get_unavailable_nodes_reassignments(request_context&);

    void get_rack_constraint_repair_reassignments(request_context&);

    void get_full_node_reassignments(request_context&);

    void init_per_node_state(
      const cluster_health_report&,
      const std::vector<raft::follower_metrics>&,
      request_context&,
      plan_data&) const;

    void get_unavailable_node_movement_cancellations(request_context&);

    bool is_partition_movement_possible(
      const std::vector<model::broker_shard>& current_replicas,
      const request_context&);

    void init_ntp_sizes_from_health_report(
      const cluster_health_report& health_report, request_context&);

    std::optional<size_t>
    get_partition_size(const model::ntp& ntp, const request_context&);

    bool all_reports_received(const request_context&);

    planner_config _config;
    partition_balancer_state& _state;
    partition_allocator& _partition_allocator;
};

} // namespace cluster
