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

#include "cluster/fwd.h"
#include "cluster/health_monitor_types.h"
#include "cluster/partition_balancer_types.h"
#include "cluster/scheduling/types.h"
#include "cluster/types.h"
#include "model/metadata.h"

#include <absl/container/flat_hash_map.h>

#include <chrono>

namespace cluster {

enum ntp_reassignment_type : int8_t { regular, force };

struct ntp_reassignment {
    model::ntp ntp;
    allocated_partition allocated;
    reconfiguration_policy reconfiguration_policy;
    ntp_reassignment_type type;
};

struct planner_config {
    model::partition_autobalancing_mode mode;
    // If node disk usage goes over this ratio planner will actively move
    // partitions away from the node.
    double soft_max_disk_usage_ratio;
    // Planner won't plan a move that will result in destination node(s) going
    // over this ratio.
    double hard_max_disk_usage_ratio;
    // Max number of actions that can be scheduled in one planning iteration
    size_t max_concurrent_actions;
    std::chrono::seconds node_availability_timeout_sec;
    // If the user manually requested a rebalance (not connected to node
    // addition)
    bool ondemand_rebalance_requested = false;
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
    // If true, prioritize balancing topic-wise number of
    // partitions on each node, as opposed to balancing the total number of
    // partitions.
    bool topic_aware = false;
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
        waiting_for_reports,
        missing_sizes,
    };
    /**
     * class describing a reason underlying partition replica set change
     */
    enum class change_reason {
        rack_constraint_repair,
        partition_count_rebalancing,
        node_decommissioning,
        node_unavailable,
        disk_full,
    };

    struct plan_data {
        partition_balancer_violations violations;
        std::vector<ntp_reassignment> reassignments;
        std::vector<model::ntp> cancellations;
        absl::flat_hash_map<model::node_id, absl::btree_set<model::ntp>>
          decommission_realloc_failures;
        bool counts_rebalancing_finished = false;
        size_t failed_actions_count = 0;
        status status = status::empty;
    };

    ss::future<plan_data>
    plan_actions(const cluster_health_report&, ss::abort_source&);

private:
    class request_context;
    class partition;
    class reassignable_partition;
    class force_reassignable_partition;
    class moving_partition;
    class immutable_partition;

    void init_per_node_state(
      const cluster_health_report&, request_context&, plan_data&);

    ss::future<> init_ntp_sizes_from_health_report(
      const cluster_health_report& health_report, request_context&);
    ss::future<> init_topic_node_counts(request_context&);

    /// Returns a pair of (total, free) bytes on a given node.
    std::pair<uint64_t, uint64_t> get_node_bytes_info(const node::local_state&);

    static ss::future<> get_node_drain_actions(
      request_context&,
      const absl::flat_hash_set<model::node_id>&,
      change_reason reason);

    static ss::future<> get_rack_constraint_repair_actions(request_context&);
    static ss::future<> get_full_node_actions(request_context&);
    static ss::future<> get_counts_rebalancing_actions(request_context&);
    static ss::future<> get_force_repair_actions(request_context&);

    static size_t calculate_full_disk_partition_move_priority(
      model::node_id, const reassignable_partition&, const request_context&);

    planner_config _config;
    partition_balancer_state& _state;
    partition_allocator& _partition_allocator;

    friend std::ostream& operator<<(std::ostream&, change_reason);
};

} // namespace cluster
