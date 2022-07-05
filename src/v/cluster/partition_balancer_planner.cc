/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/partition_balancer_planner.h"

#include "cluster/partition_balancer_types.h"
#include "cluster/scheduling/constraints.h"
#include "cluster/scheduling/types.h"

#include <optional>

namespace cluster {

partition_balancer_planner::partition_balancer_planner(
  planner_config config,
  topic_table& topic_table,
  partition_allocator& partition_allocator)
  : _config(config)
  , _topic_table(topic_table)
  , _partition_allocator(partition_allocator) {}

void partition_balancer_planner::
  init_ntp_sizes_and_node_disk_reports_from_health_report(
    const cluster_health_report& health_report,
    reallocation_request_state& rrs) {
    for (const auto& node_report : health_report.node_reports) {
        uint64_t total = 0;
        uint64_t free = 0;
        for (const auto& disk : node_report.local_state.disks) {
            total += disk.total;
            free += disk.free;
        }
        rrs.node_disk_reports.insert(
          {node_report.id, node_disk_space(node_report.id, free, total)});
        for (const auto& tp_ns : node_report.topics) {
            for (const auto& partition : tp_ns.partitions) {
                rrs.ntp_sizes[model::ntp(
                  tp_ns.tp_ns.ns, tp_ns.tp_ns.tp, partition.id)]
                  = partition.size_bytes;
            }
        }
    }
    return;
}

void partition_balancer_planner::
  calculate_nodes_with_disk_constraints_violation(
    reallocation_request_state& rrs) {
    for (const auto& n : rrs.node_disk_reports) {
        if (
          n.second.free_space_rate < double(1) - _config.max_disk_usage_ratio) {
            rrs.nodes_with_disk_constraints_violation.insert(n.first);
            rrs.full_nodes_disk_sizes.insert(n.second);
        }
    }
}

void partition_balancer_planner::calculate_unavailable_nodes(
  const std::vector<raft::follower_metrics>& follower_metrics,
  reallocation_request_state& rrs) {
    const auto now = raft::clock_type::now();
    for (const auto& follower : follower_metrics) {
        if (
          now - follower.last_heartbeat
          > _config.node_availability_timeout_sec) {
            rrs.unavailable_nodes.insert(follower.id);
        }
    }
}

std::vector<model::broker_shard>
partition_balancer_planner::get_stable_replicas(
  const std::vector<model::broker_shard>& replicas,
  const reallocation_request_state& rrs,
  size_t full_nodes_leave_amount) {
    std::vector<model::broker_shard> stable_replicas;
    absl::flat_hash_map<model::node_id, uint32_t>
      replicas_breaking_disk_constraints;

    // add stable nodes
    for (const auto& r : replicas) {
        if (!rrs.unavailable_nodes.contains(r.node_id)) {
            if (!rrs.nodes_with_disk_constraints_violation.contains(
                  r.node_id)) {
                stable_replicas.push_back(r);
            } else {
                replicas_breaking_disk_constraints[r.node_id] = r.shard;
            }
        }
    }

    // add 'full_nodes_leave_amount' of nodes with disk constraints violation
    // ordered by free_space rate
    for (auto node = rrs.full_nodes_disk_sizes.rbegin();
         node != rrs.full_nodes_disk_sizes.rend();
         node++) {
        if (full_nodes_leave_amount == 0) {
            return stable_replicas;
        }
        if (const auto& r = replicas_breaking_disk_constraints.find(
              node->node_id);
            r != replicas_breaking_disk_constraints.end()) {
            stable_replicas.push_back(model::broker_shard{r->first, r->second});
            full_nodes_leave_amount--;
        }
    }

    return stable_replicas;
}

size_t partition_balancer_planner::get_full_nodes_amount(
  const std::vector<model::broker_shard>& replicas,
  const reallocation_request_state& rrs) {
    size_t full_nodes_amount = 0;
    for (const auto& r : replicas) {
        if (rrs.nodes_with_disk_constraints_violation.contains(r.node_id)) {
            full_nodes_amount += 1;
        }
    }
    return full_nodes_amount;
}

bool partition_balancer_planner::is_partition_movement_possible(
  const std::vector<model::broker_shard>& current_replicas,
  const reallocation_request_state& rrs) {
    // Check that nodes quorum is available
    size_t available_nodes_amount = std::count_if(
      current_replicas.begin(),
      current_replicas.end(),
      [&rrs](const model::broker_shard& bs) {
          return rrs.unavailable_nodes.find(bs.node_id)
                 == rrs.unavailable_nodes.end();
      });
    if (available_nodes_amount * 2 < current_replicas.size()) {
        return false;
    }
    return true;
}

std::optional<size_t> partition_balancer_planner::get_partition_size(
  const model::ntp& ntp, const reallocation_request_state& rrs) {
    const auto ntp_data = rrs.ntp_sizes.find(ntp);
    if (ntp_data == rrs.ntp_sizes.end()) {
        vlog(
          clusterlog.error,
          "Partition {} status was not found in cluster health "
          "report",
          ntp);
    } else {
        return ntp_data->second;
    }
    return std::nullopt;
}

/*
 * Function tries to find max reallocation
 * It tries to move ntp out of 'bad' nodes
 * Initially it considers as 'bad' all unavailable nodes
 * and all nodes that are breaking max_disk_usage_ratio
 * If reallocation request fails, it removes least filled node that
 * is available but is breaking max_disk_usage_ratio
 * It repeats until it doesn't work out to calculate reallocation
 * or all nodes that are breaking max_disk_usage_ratio are removed out of 'bad'
 */
result<allocation_units> partition_balancer_planner::get_reallocation(
  const partition_assignment& assignments,
  const topic_metadata& topic_metadata,
  size_t partition_size,
  bool use_max_disk_constraint,
  reallocation_request_state& rrs) {
    allocation_constraints allocation_constraints;

    // Add constraint on least disk usage
    allocation_constraints.soft_constraints.push_back(
      ss::make_lw_shared<soft_constraint_evaluator>(least_disk_filled(
        _config.max_disk_usage_ratio,
        rrs.assigned_reallocation_sizes,
        rrs.node_disk_reports)));

    if (use_max_disk_constraint) {
        // Add constraint on partition max_disk_usage_ratio overfill
        allocation_constraints.hard_constraints.push_back(
          ss::make_lw_shared<hard_constraint_evaluator>(
            disk_not_overflowed_by_partition(
              _config.max_disk_usage_ratio,
              partition_size,
              rrs.assigned_reallocation_sizes,
              rrs.node_disk_reports)));
    } else {
        // Add constraint on partition disk overfill
        allocation_constraints.hard_constraints.push_back(
          ss::make_lw_shared<hard_constraint_evaluator>(
            disk_not_overflowed_by_partition(
              1,
              partition_size,
              rrs.assigned_reallocation_sizes,
              rrs.node_disk_reports)));
    }

    // Add constraint on unavailable nodes
    std::vector<model::broker_shard> unavailable_nodes;
    unavailable_nodes.reserve(unavailable_nodes.size());
    for (auto node_id : rrs.unavailable_nodes) {
        unavailable_nodes.push_back(model::broker_shard{node_id, 0});
    }
    allocation_constraints.hard_constraints.push_back(
      ss::make_lw_shared<hard_constraint_evaluator>(
        distinct_from(unavailable_nodes)));

    auto constraints = partition_constraints(
      assignments.id,
      topic_metadata.get_configuration().replication_factor,
      allocation_constraints);

    size_t full_node_amount = get_full_nodes_amount(assignments.replicas, rrs);
    for (size_t i = 0; i <= full_node_amount; ++i) {
        auto stable_replicas = get_stable_replicas(
          assignments.replicas, rrs, i);

        if (stable_replicas.size() == assignments.replicas.size()) {
            break;
        }

        auto stable_assigments = partition_assignment(
          assignments.group, assignments.id, stable_replicas);

        auto reallocation = _partition_allocator.reallocate_partition(
          constraints, stable_assigments);

        auto ntp = model::ntp(
          topic_metadata.get_configuration().tp_ns.ns,
          topic_metadata.get_configuration().tp_ns.tp,
          assignments.id);
        if (reallocation.has_value()) {
            rrs.moving_partitions.insert(ntp);
            rrs.planned_movement_disk_size += partition_size;
            for (const auto r :
                 reallocation.value().get_assignments().front().replicas) {
                if (
                  std::find(stable_replicas.begin(), stable_replicas.end(), r)
                  == stable_replicas.end()) {
                    rrs.assigned_reallocation_sizes[r.node_id]
                      += partition_size;
                }
                if (
                  std::find(
                    assignments.replicas.begin(), assignments.replicas.end(), r)
                  == assignments.replicas.end()) {
                    rrs.node_released_disk_size[r.node_id] += partition_size;
                }
            }
            return reallocation;
        } else {
            vlog(
              clusterlog.info,
              "Can't find movement for ntp {} with stable_replicas: {}, "
              "Error: {}",
              ntp,
              stable_replicas,
              reallocation.error().message());
        }
    }
    return errc::no_eligible_allocation_nodes;
}

/*
 * Function is trying to move ntp out of unavailable nodes
 * It can move to nodes that are violating max_disk_usage_ratio constraint
 */
void partition_balancer_planner::get_unavailable_nodes_reassignments(
  std::vector<ntp_reassignments>& reassignments,
  reallocation_request_state& rrs) {
    for (const auto& t : _topic_table.topics_map()) {
        for (const auto& a : t.second.get_assignments()) {
            auto ntp = model::ntp(t.first.ns, t.first.tp, a.id);
            if (rrs.moving_partitions.contains(ntp)) {
                continue;
            }
            // Get all available nodes
            auto available_replicas = get_stable_replicas(
              a.replicas, rrs, SIZE_MAX);
            if (available_replicas.size() != a.replicas.size()) {
                auto partition_size = get_partition_size(ntp, rrs);
                if (
                  !partition_size.has_value()
                  || !is_partition_movement_possible(a.replicas, rrs)) {
                    continue;
                }
                auto new_allocation_units = get_reallocation(
                  a, t.second, partition_size.value(), false, rrs);
                if (!new_allocation_units) {
                    vlog(
                      clusterlog.info,
                      "Can't reallocate {} from down node",
                      ntp);
                } else {
                    reassignments.emplace_back(ntp_reassignments{
                      .ntp = ntp,
                      .allocation_units = std::move(
                        new_allocation_units.value())});
                }
            }

            // End adding movements if batch is collected
            if (
              rrs.planned_movement_disk_size
              >= _config.movement_disk_size_batch) {
                return;
            }
        }
    }
}

/*
 * Function is trying to move ntps out of node that are violating
 * max_disk_usage_ratio It takes nodes in free space rate order For each node it
 * is trying to collect set of partitions to move Partitions are selected in
 * ascending order of their size.
 */
void partition_balancer_planner::get_full_node_reassignments(
  std::vector<ntp_reassignments>& reassignments,
  reallocation_request_state& rrs) {
    absl::flat_hash_map<model::node_id, std::vector<model::ntp>> ntp_on_nodes;
    for (const auto& t : _topic_table.topics_map()) {
        for (const auto& a : t.second.get_assignments()) {
            for (const auto& r : a.replicas) {
                ntp_on_nodes[r.node_id].emplace_back(
                  t.first.ns, t.first.tp, a.id);
            }
        }
    }

    for (const auto& node_disk_space : rrs.full_nodes_disk_sizes) {
        if (
          rrs.planned_movement_disk_size >= _config.movement_disk_size_batch) {
            return;
        }
        absl::btree_multimap<size_t, model::ntp> ntp_on_node_sizes;
        for (const auto& ntp : ntp_on_nodes[node_disk_space.node_id]) {
            auto partition_size_opt = get_partition_size(ntp, rrs);
            if (partition_size_opt.has_value()) {
                ntp_on_node_sizes.emplace(partition_size_opt.value(), ntp);
            }
        }
        auto ntp_size_it = ntp_on_node_sizes.begin();
        while (double(
                 rrs.node_released_disk_size[node_disk_space.node_id]
                 + node_disk_space.free_space)
                   / double(node_disk_space.total_space)
                 < double(1) - _config.max_disk_usage_ratio
               && ntp_size_it != ntp_on_node_sizes.end()) {
            const auto& partition_to_move = ntp_size_it->second;
            if (rrs.moving_partitions.contains(partition_to_move)) {
                ntp_size_it++;
                continue;
            }

            const auto& topic_metadata = _topic_table.topics_map().at(
              model::topic_namespace_view(partition_to_move));
            const auto& current_assignments
              = topic_metadata.get_assignments().find(
                partition_to_move.tp.partition);
            if (!is_partition_movement_possible(
                  current_assignments->replicas, rrs)) {
                ntp_size_it++;
                continue;
            }
            auto new_allocation_units = get_reallocation(
              *current_assignments,
              topic_metadata,
              ntp_size_it->first,
              true,
              rrs);
            if (!new_allocation_units) {
                vlog(clusterlog.info, "Can't reallocate {}", partition_to_move);
            } else {
                reassignments.emplace_back(ntp_reassignments{
                  .ntp = partition_to_move,
                  .allocation_units = std::move(new_allocation_units.value())});
            }
            ntp_size_it++;
            if (
              rrs.planned_movement_disk_size
              >= _config.movement_disk_size_batch) {
                return;
            }
        }
    }
}

std::vector<ntp_reassignments>
partition_balancer_planner::get_ntp_reassignments(
  const cluster_health_report& health_report,
  const std::vector<raft::follower_metrics>& follower_metrics) {
    if (_topic_table.has_updates_in_progress()) {
        return {};
    }

    reallocation_request_state rrs;
    std::vector<ntp_reassignments> reassignments;

    init_ntp_sizes_and_node_disk_reports_from_health_report(health_report, rrs);

    calculate_unavailable_nodes(follower_metrics, rrs);
    calculate_nodes_with_disk_constraints_violation(rrs);

    get_unavailable_nodes_reassignments(reassignments, rrs);
    get_full_node_reassignments(reassignments, rrs);

    return reassignments;
}

} // namespace cluster