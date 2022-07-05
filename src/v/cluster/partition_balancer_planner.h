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
    double max_disk_usage_ratio;
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

    std::vector<ntp_reassignments> get_ntp_reassignments(
      const cluster_health_report&, const std::vector<raft::follower_metrics>&);

private:
    struct reallocation_request_state {
        uint64_t planned_movement_disk_size = 0;
        absl::flat_hash_map<model::ntp, size_t> ntp_sizes;
        absl::flat_hash_map<model::node_id, node_disk_space> node_disk_reports;
        absl::flat_hash_set<model::node_id> unavailable_nodes;
        absl::flat_hash_set<model::node_id>
          nodes_with_disk_constraints_violation;
        absl::btree_set<node_disk_space> full_nodes_disk_sizes;
        // Partitions that are planned to move in current planner request
        absl::flat_hash_set<model::ntp> moving_partitions;
        // Size of partitions that are planned to move from node
        absl::flat_hash_map<model::node_id, uint64_t> node_released_disk_size;
        // Size of partitions that are planned to move on node
        absl::flat_hash_map<model::node_id, uint64_t>
          assigned_reallocation_sizes;
    };

    result<allocation_units> get_reallocation(
      const partition_assignment& assignments,
      const topic_metadata& topic_metadata,
      size_t partition_size,
      bool use_max_disk_constraint,
      reallocation_request_state&);

    void get_unavailable_nodes_reassignments(
      std::vector<ntp_reassignments>&, reallocation_request_state&);

    void get_full_node_reassignments(
      std::vector<ntp_reassignments>&, reallocation_request_state&);

    void calculate_nodes_with_disk_constraints_violation(
      reallocation_request_state&);

    void calculate_unavailable_nodes(
      const std::vector<raft::follower_metrics>&, reallocation_request_state&);

    size_t get_full_nodes_amount(
      const std::vector<model::broker_shard>& replicas,
      const reallocation_request_state& rrs);

    std::vector<model::broker_shard> get_stable_replicas(
      const std::vector<model::broker_shard>& replicas,
      const reallocation_request_state&,
      size_t full_nodes_leave_amount);

    bool is_partition_movement_possible(
      const std::vector<model::broker_shard>& current_replicas,
      const reallocation_request_state&);

    void init_ntp_sizes_and_node_disk_reports_from_health_report(
      const cluster_health_report& health_report, reallocation_request_state&);

    std::optional<size_t> get_partition_size(
      const model::ntp& ntp, const reallocation_request_state&);

    planner_config _config;
    topic_table& _topic_table;
    partition_allocator& _partition_allocator;
};

} // namespace cluster
