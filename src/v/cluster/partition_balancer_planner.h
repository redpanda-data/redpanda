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
#include "model/metadata.h"

#include <absl/container/flat_hash_map.h>

#include <chrono>

namespace cluster {

struct ntp_reassignment {
    model::ntp ntp;
    allocated_partition allocated;
};

struct planner_config {
    model::partition_autobalancing_mode mode;
    // If node disk usage goes over this ratio planner will actively move
    // partitions away from the node.
    double soft_max_disk_usage_ratio;
    // Planner won't plan a move that will result in destination node(s) going
    // over this ratio.
    double hard_max_disk_usage_ratio;
    // Size of partitions that can be planned to move in one request
    size_t movement_disk_size_batch;
    // Max number of actions that can be scheduled in one planning iteration
    size_t max_concurrent_actions;
    std::chrono::seconds node_availability_timeout_sec;
    // Fallocation step used to calculate upperbound for partition size
    size_t segment_fallocation_step;
    // Threshold for minimum size of partition that is going to be prioritized
    // for movement, partitions with size smaller than threshold will have the
    // lowest priority
    size_t min_partition_size_threshold;
    // Timeout after which node is claimed unresponsive i.e. it doesn't respond
    // the request but it is not yet considered as a violation of partition
    // balancing rules
    std::chrono::milliseconds node_responsiveness_timeout;
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
        size_t failed_actions_count = 0;
        status status = status::empty;
    };

    plan_data plan_actions(const cluster_health_report&);

private:
    class request_context;
    class partition;
    class reassignable_partition;
    class moving_partition;
    class immutable_partition;

    void init_per_node_state(
      const cluster_health_report&, request_context&, plan_data&) const;

    void init_ntp_sizes_from_health_report(
      const cluster_health_report& health_report, request_context&);

    static void get_node_drain_actions(
      request_context&,
      const absl::flat_hash_set<model::node_id>&,
      std::string_view reason);
    static void get_rack_constraint_repair_actions(request_context&);
    static void get_full_node_actions(request_context&);
    static size_t calculate_full_disk_partition_move_priority(
      model::node_id, const reassignable_partition&, const request_context&);

    planner_config _config;
    partition_balancer_state& _state;
    partition_allocator& _partition_allocator;
};

} // namespace cluster
