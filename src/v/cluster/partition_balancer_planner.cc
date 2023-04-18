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

#include "cluster/cluster_utils.h"
#include "cluster/members_table.h"
#include "cluster/partition_balancer_state.h"
#include "cluster/partition_balancer_types.h"
#include "cluster/scheduling/constraints.h"
#include "cluster/scheduling/types.h"
#include "ssx/sformat.h"

#include <seastar/core/sstring.hh>

#include <optional>

namespace cluster {

namespace {

hard_constraint
distinct_from(const absl::flat_hash_set<model::node_id>& nodes) {
    class impl : public hard_constraint::impl {
    public:
        explicit impl(const absl::flat_hash_set<model::node_id>& nodes)
          : _nodes(nodes) {}

        hard_constraint_evaluator
        make_evaluator(const replicas_t&) const final {
            return [this](const allocation_node& node) {
                return !_nodes.contains(node.id());
            };
        }

        ss::sstring name() const final {
            return ssx::sformat(
              "distinct from nodes: {}",
              std::vector(_nodes.begin(), _nodes.end()));
        }

    private:
        const absl::flat_hash_set<model::node_id>& _nodes;
    };

    return hard_constraint(std::make_unique<impl>(nodes));
}

} // namespace

partition_balancer_planner::partition_balancer_planner(
  planner_config config,
  partition_balancer_state& state,
  partition_allocator& partition_allocator)
  : _config(config)
  , _state(state)
  , _partition_allocator(partition_allocator) {
    _config.soft_max_disk_usage_ratio = std::min(
      _config.soft_max_disk_usage_ratio, _config.hard_max_disk_usage_ratio);
}

void partition_balancer_planner::init_per_node_state(
  const cluster_health_report& health_report,
  const std::vector<raft::follower_metrics>& follower_metrics,
  reallocation_request_state& rrs,
  plan_data& result) const {
    for (const auto& [id, broker] : _state.members().nodes()) {
        if (
          broker.state.get_membership_state()
          == model::membership_state::removed) {
            continue;
        }

        rrs.all_nodes.push_back(id);

        if (
          broker.state.get_maintenance_state()
          == model::maintenance_state::active) {
            vlog(clusterlog.debug, "node {}: in maintenance", id);
            rrs.num_nodes_in_maintenance += 1;
        }

        if (
          broker.state.get_membership_state()
          == model::membership_state::draining) {
            vlog(clusterlog.debug, "node {}: decommissioning", id);
            rrs.decommissioning_nodes.insert(id);
        }
    }

    const auto now = raft::clock_type::now();
    for (const auto& follower : follower_metrics) {
        auto unavailable_dur = now - follower.last_heartbeat;

        vlog(
          clusterlog.debug,
          "node {}: {} ms since last heartbeat",
          follower.id,
          std::chrono::duration_cast<std::chrono::milliseconds>(unavailable_dur)
            .count());

        if (follower.is_live) {
            continue;
        }

        rrs.all_unavailable_nodes.insert(follower.id);

        if (unavailable_dur > _config.node_availability_timeout_sec) {
            rrs.timed_out_unavailable_nodes.insert(follower.id);
            model::timestamp unavailable_since = model::to_timestamp(
              model::timestamp_clock::now()
              - std::chrono::duration_cast<model::timestamp_clock::duration>(
                unavailable_dur));
            result.violations.unavailable_nodes.emplace_back(
              follower.id, unavailable_since);
        }
    }

    for (const auto& node_report : health_report.node_reports) {
        const uint64_t total = node_report.local_state.data_disk.total;
        const uint64_t free = node_report.local_state.data_disk.free;

        rrs.node_disk_reports.emplace(
          node_report.id, node_disk_space(node_report.id, total, total - free));
    }

    for (const auto& [id, disk] : rrs.node_disk_reports) {
        double used_space_ratio = disk.original_used_ratio();
        vlog(
          clusterlog.debug,
          "node {}: bytes used: {}, bytes total: {}, used ratio: {:.4}",
          id,
          disk.used,
          disk.total,
          used_space_ratio);
        if (used_space_ratio > _config.soft_max_disk_usage_ratio) {
            result.violations.full_nodes.emplace_back(
              id, uint32_t(used_space_ratio * 100.0));
        }
    }
}

void partition_balancer_planner::init_ntp_sizes_from_health_report(
  const cluster_health_report& health_report, reallocation_request_state& rrs) {
    for (const auto& node_report : health_report.node_reports) {
        for (const auto& tp_ns : node_report.topics) {
            for (const auto& partition : tp_ns.partitions) {
                rrs.ntp_sizes[model::ntp(
                  tp_ns.tp_ns.ns, tp_ns.tp_ns.tp, partition.id)]
                  = partition.size_bytes;
            }
        }
    }
}

bool partition_balancer_planner::all_reports_received(
  const reallocation_request_state& rrs) {
    for (auto id : rrs.all_nodes) {
        if (
          !rrs.all_unavailable_nodes.contains(id)
          && !rrs.node_disk_reports.contains(id)) {
            vlog(clusterlog.info, "No disk report for node {}", id);
            return false;
        }
    }
    return true;
}

bool partition_balancer_planner::is_partition_movement_possible(
  const std::vector<model::broker_shard>& current_replicas,
  const reallocation_request_state& rrs) {
    // Check that nodes quorum is available
    size_t available_nodes_amount = std::count_if(
      current_replicas.begin(),
      current_replicas.end(),
      [&rrs](const model::broker_shard& bs) {
          return !rrs.all_unavailable_nodes.contains(bs.node_id);
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
          clusterlog.info,
          "Partition {} status was not found in cluster health "
          "report",
          ntp);
    } else {
        return ntp_data->second;
    }
    return std::nullopt;
}

partition_constraints partition_balancer_planner::get_partition_constraints(
  const partition_assignment& assignments,
  size_t partition_size,
  double max_disk_usage_ratio,
  reallocation_request_state& rrs) const {
    allocation_constraints allocation_constraints;

    // Add constraint on least disk usage
    allocation_constraints.add(
      least_disk_filled(max_disk_usage_ratio, rrs.node_disk_reports));

    // Add constraint on partition max_disk_usage_ratio overfill
    size_t upper_bound_for_partition_size = partition_size
                                            + _config.segment_fallocation_step;
    allocation_constraints.add(disk_not_overflowed_by_partition(
      max_disk_usage_ratio,
      upper_bound_for_partition_size,
      rrs.node_disk_reports));

    // Add constraint on unavailable nodes
    allocation_constraints.add(distinct_from(rrs.timed_out_unavailable_nodes));

    // Add constraint on decommissioning nodes
    if (!rrs.decommissioning_nodes.empty()) {
        allocation_constraints.add(distinct_from(rrs.decommissioning_nodes));
    }

    return partition_constraints(
      assignments.id,
      assignments.replicas.size(),
      std::move(allocation_constraints));
}

result<allocation_units> partition_balancer_planner::get_reallocation(
  const model::ntp& ntp,
  const partition_assignment& assignments,
  size_t partition_size,
  partition_constraints constraints,
  const std::vector<model::broker_shard>& stable_replicas,
  reallocation_request_state& rrs) {
    vlog(
      clusterlog.debug,
      "trying to find reallocation for ntp {} with stable_replicas: {}",
      ntp,
      stable_replicas);

    auto stable_assigments = partition_assignment(
      assignments.group, assignments.id, stable_replicas);

    auto reallocation = _partition_allocator.reallocate_partition(
      std::move(constraints), stable_assigments, get_allocation_domain(ntp));

    if (!reallocation) {
        vlog(
          clusterlog.info,
          "attempt to find reallocation for ntp {} with "
          "stable_replicas: {} failed, error: {}",
          ntp,
          stable_replicas,
          reallocation.error().message());

        return reallocation;
    }

    rrs.moving_partitions.insert(ntp);
    rrs.planned_moves_size += partition_size;
    for (const auto r :
         reallocation.value().get_assignments().front().replicas) {
        if (
          std::find(stable_replicas.begin(), stable_replicas.end(), r)
          == stable_replicas.end()) {
            auto disk_it = rrs.node_disk_reports.find(r.node_id);
            if (disk_it != rrs.node_disk_reports.end()) {
                disk_it->second.assigned += partition_size;
            }
        }
    }
    for (const auto r : assignments.replicas) {
        if (
          std::find(stable_replicas.begin(), stable_replicas.end(), r)
          == stable_replicas.end()) {
            auto disk_it = rrs.node_disk_reports.find(r.node_id);
            if (disk_it != rrs.node_disk_reports.end()) {
                disk_it->second.released += partition_size;
            }
        }
    }

    return reallocation;
}

void partition_balancer_planner::plan_data::add_reassignment(
  model::ntp ntp,
  const std::vector<model::broker_shard>& orig_replicas,
  allocation_units allocation_units,
  std::string_view reason) {
    vlog(
      clusterlog.info,
      "ntp: {}, planning move {} -> {} (reason: {})",
      ntp,
      orig_replicas,
      allocation_units.get_assignments().front().replicas,
      reason);

    reassignments.emplace_back(ntp_reassignments{
      .ntp = ntp, .allocation_units = std::move(allocation_units)});
}

/*
 * Function is trying to move ntp out of unavailable nodes
 * It can move to nodes that are violating soft_max_disk_usage_ratio constraint
 */
void partition_balancer_planner::get_unavailable_nodes_reassignments(
  plan_data& result, reallocation_request_state& rrs) {
    if (rrs.timed_out_unavailable_nodes.empty()) {
        return;
    }

    for (const auto& t : _state.topics().topics_map()) {
        for (const auto& a : t.second.get_assignments()) {
            // End adding movements if batch is collected
            if (rrs.planned_moves_size >= _config.movement_disk_size_batch) {
                return;
            }

            auto ntp = model::ntp(t.first.ns, t.first.tp, a.id);
            if (rrs.moving_partitions.contains(ntp)) {
                continue;
            }

            std::vector<model::broker_shard> stable_replicas;
            for (const auto& bs : a.replicas) {
                if (!rrs.timed_out_unavailable_nodes.contains(bs.node_id)) {
                    stable_replicas.push_back(bs);
                }
            }

            if (stable_replicas.size() == a.replicas.size()) {
                continue;
            }

            auto partition_size = get_partition_size(ntp, rrs);
            if (
              !partition_size.has_value()
              || !is_partition_movement_possible(a.replicas, rrs)) {
                result.failed_reassignments_count += 1;
                continue;
            }

            auto constraints = get_partition_constraints(
              a,
              partition_size.value(),
              _config.hard_max_disk_usage_ratio,
              rrs);

            auto new_allocation_units = get_reallocation(
              ntp,
              a,
              partition_size.value(),
              std::move(constraints),
              stable_replicas,
              rrs);
            if (new_allocation_units) {
                result.add_reassignment(
                  ntp,
                  a.replicas,
                  std::move(new_allocation_units.value()),
                  "unavailable nodes");
            } else {
                result.failed_reassignments_count += 1;
            }
        }
    }
}

/// Try to fix ntps that have several replicas in one rack (these ntps can
/// appear because rack awareness constraint is not a hard constraint, e.g. when
/// a rack dies and we move all replicas that resided on dead nodes to live
/// ones).
///
/// We go over all such ntps (a list maintained by partition_balancer_state) and
/// if the number of currently live racks is more than the number of racks that
/// the ntp is replicated to, we try to schedule a move. For each rack we
/// arbitrarily choose the first appearing replica to remain there (note: this
/// is probably not optimal choice).
void partition_balancer_planner::get_rack_constraint_repair_reassignments(
  plan_data& result, reallocation_request_state& rrs) {
    if (_state.ntps_with_broken_rack_constraint().empty()) {
        return;
    }

    absl::flat_hash_set<model::rack_id> available_racks;
    for (auto node_id : rrs.all_nodes) {
        if (!rrs.timed_out_unavailable_nodes.contains(node_id)) {
            auto rack = _state.members().get_node_rack_id(node_id);
            if (rack) {
                available_racks.insert(*rack);
            }
        }
    }

    for (const auto& ntp : _state.ntps_with_broken_rack_constraint()) {
        if (rrs.planned_moves_size >= _config.movement_disk_size_batch) {
            return;
        }

        if (rrs.moving_partitions.contains(ntp)) {
            continue;
        }

        auto assignment = _state.topics().get_partition_assignment(ntp);
        if (!assignment) {
            vlog(clusterlog.warn, "assignment for ntp {} not found", ntp);
            continue;
        }

        const auto& orig_replicas = assignment->replicas;

        std::vector<model::broker_shard> stable_replicas;
        absl::flat_hash_set<model::rack_id> cur_racks;
        for (const auto& bs : orig_replicas) {
            auto rack = _state.members().get_node_rack_id(bs.node_id);
            if (rack) {
                auto [it, inserted] = cur_racks.insert(*rack);
                if (inserted) {
                    stable_replicas.push_back(bs);
                }
            } else {
                stable_replicas.push_back(bs);
            }
        }

        if (stable_replicas.size() == orig_replicas.size()) {
            continue;
        }

        if (available_racks.size() <= cur_racks.size()) {
            // Can't repair the constraint if we don't have an available rack to
            // place a replica there.
            continue;
        }

        auto partition_size = get_partition_size(ntp, rrs);
        if (
          !partition_size.has_value()
          || !is_partition_movement_possible(orig_replicas, rrs)) {
            result.failed_reassignments_count += 1;
            continue;
        }

        auto constraints = get_partition_constraints(
          *assignment,
          partition_size.value(),
          _config.hard_max_disk_usage_ratio,
          rrs);

        auto new_allocation_units = get_reallocation(
          ntp,
          *assignment,
          partition_size.value(),
          std::move(constraints),
          stable_replicas,
          rrs);
        if (new_allocation_units) {
            result.add_reassignment(
              ntp,
              orig_replicas,
              std::move(new_allocation_units.value()),
              "rack constraint repair");
        } else {
            result.failed_reassignments_count += 1;
        }
    }
}

/*
 * Function is trying to move ntps out of node that are violating
 * soft_max_disk_usage_ratio. It takes nodes in reverse used space ratio order.
 * For each node it is trying to collect set of partitions to move. Partitions
 * are selected in ascending order of their size.
 *
 * If more than one replica in a group is on a node violating disk usage
 * constraints, we try to reallocate all such replicas. But if a reallocation
 * request fails, we retry while leaving some of these replicas intact.
 */
void partition_balancer_planner::get_full_node_reassignments(
  plan_data& result, reallocation_request_state& rrs) {
    std::vector<const node_disk_space*> sorted_full_nodes;
    for (const auto& kv : rrs.node_disk_reports) {
        const auto* node_disk = &kv.second;
        if (node_disk->final_used_ratio() > _config.soft_max_disk_usage_ratio) {
            sorted_full_nodes.push_back(node_disk);
        }
    }
    std::sort(
      sorted_full_nodes.begin(),
      sorted_full_nodes.end(),
      [](const auto* lhs, const auto* rhs) {
          return lhs->final_used_ratio() > rhs->final_used_ratio();
      });

    if (sorted_full_nodes.empty()) {
        return;
    }

    absl::flat_hash_map<model::node_id, std::vector<model::ntp>> ntp_on_nodes;
    for (const auto& t : _state.topics().topics_map()) {
        for (const auto& a : t.second.get_assignments()) {
            for (const auto& r : a.replicas) {
                ntp_on_nodes[r.node_id].emplace_back(
                  t.first.ns, t.first.tp, a.id);
            }
        }
    }

    for (const auto* node_disk : sorted_full_nodes) {
        if (rrs.planned_moves_size >= _config.movement_disk_size_batch) {
            return;
        }

        absl::btree_multimap<size_t, model::ntp> ntp_on_node_sizes;
        for (const auto& ntp : ntp_on_nodes[node_disk->node_id]) {
            auto partition_size_opt = get_partition_size(ntp, rrs);
            if (partition_size_opt.has_value()) {
                ntp_on_node_sizes.emplace(partition_size_opt.value(), ntp);
            } else {
                result.failed_reassignments_count += 1;
            }
        }

        auto ntp_size_it = ntp_on_node_sizes.begin();
        while (node_disk->final_used_ratio() > _config.soft_max_disk_usage_ratio
               && ntp_size_it != ntp_on_node_sizes.end()) {
            if (rrs.planned_moves_size >= _config.movement_disk_size_batch) {
                return;
            }

            const auto& partition_to_move = ntp_size_it->second;
            if (rrs.moving_partitions.contains(partition_to_move)) {
                ntp_size_it++;
                continue;
            }

            const auto& topic_metadata = _state.topics().topics_map().at(
              model::topic_namespace_view(partition_to_move));
            const auto& current_assignments
              = topic_metadata.get_assignments().find(
                partition_to_move.tp.partition);

            if (!is_partition_movement_possible(
                  current_assignments->replicas, rrs)) {
                result.failed_reassignments_count += 1;
                ntp_size_it++;
                continue;
            }

            auto constraints = get_partition_constraints(
              *current_assignments,
              ntp_size_it->first,
              _config.soft_max_disk_usage_ratio,
              rrs);

            struct full_node_replica {
                model::broker_shard bs;
                node_disk_space disk;
            };
            std::vector<full_node_replica> full_node_replicas;
            std::vector<model::broker_shard> stable_replicas;

            for (const auto& r : current_assignments->replicas) {
                if (rrs.timed_out_unavailable_nodes.contains(r.node_id)) {
                    continue;
                }

                auto disk_it = rrs.node_disk_reports.find(r.node_id);
                if (disk_it == rrs.node_disk_reports.end()) {
                    // A replica on a node we recently lost contact with (but
                    // availability timeout hasn't elapsed yet). Better leave it
                    // where it is.
                    stable_replicas.push_back(r);
                    continue;
                }

                const auto& disk = disk_it->second;
                if (
                  disk.final_used_ratio() < _config.soft_max_disk_usage_ratio) {
                    stable_replicas.push_back(r);
                } else {
                    full_node_replicas.push_back(full_node_replica{
                      .bs = r,
                      .disk = disk,
                    });
                }
            }

            // We start with a small set of stable replicas that are on "good"
            // nodes and try to find a reallocation. If that fails, we add one
            // replica from the set of full_node_replicas (starting from the
            // least full) to stable replicas and retry until we get a valid
            // reallocation.
            std::sort(
              full_node_replicas.begin(),
              full_node_replicas.end(),
              [](const auto& lhs, const auto& rhs) {
                  return lhs.disk.final_used_ratio()
                         < rhs.disk.final_used_ratio();
              });

            bool success = false;
            for (const auto& replica : full_node_replicas) {
                auto new_allocation_units = get_reallocation(
                  partition_to_move,
                  *current_assignments,
                  ntp_size_it->first,
                  constraints,
                  stable_replicas,
                  rrs);

                if (new_allocation_units) {
                    result.add_reassignment(
                      partition_to_move,
                      current_assignments->replicas,
                      std::move(new_allocation_units.value()),
                      "full nodes");
                    success = true;
                    break;
                } else {
                    stable_replicas.push_back(replica.bs);
                }
            }
            if (!success) {
                result.failed_reassignments_count += 1;
            }

            ntp_size_it++;
        }
    }
}

/*
 * Cancel movement if new assignments contains unavailble node
 * and previous replica set doesn't contain this node
 */
void partition_balancer_planner::get_unavailable_node_movement_cancellations(
  plan_data& result, const reallocation_request_state& rrs) {
    for (const auto& update : _state.topics().updates_in_progress()) {
        if (update.second.get_state() != reconfiguration_state::in_progress) {
            continue;
        }

        absl::flat_hash_set<model::node_id> previous_replicas_set;
        bool was_on_decommissioning_node = false;
        for (const auto& r : update.second.get_previous_replicas()) {
            previous_replicas_set.insert(r.node_id);
            if (rrs.decommissioning_nodes.contains(r.node_id)) {
                was_on_decommissioning_node = true;
            }
        }

        auto current_assignments = _state.topics().get_partition_assignment(
          update.first);
        if (!current_assignments.has_value()) {
            continue;
        }
        for (const auto& r : current_assignments->replicas) {
            if (
              rrs.timed_out_unavailable_nodes.contains(r.node_id)
              && !previous_replicas_set.contains(r.node_id)) {
                if (!was_on_decommissioning_node) {
                    vlog(
                      clusterlog.info,
                      "ntp: {}, cancelling move {} -> {}",
                      update.first,
                      update.second.get_previous_replicas(),
                      current_assignments->replicas);

                    result.cancellations.push_back(update.first);
                } else {
                    result.failed_reassignments_count += 1;
                }
                break;
            }
        }
    }
}

partition_balancer_planner::plan_data
partition_balancer_planner::plan_reassignments(
  const cluster_health_report& health_report,
  const std::vector<raft::follower_metrics>& follower_metrics) {
    reallocation_request_state rrs;
    plan_data result;

    init_per_node_state(health_report, follower_metrics, rrs, result);

    if (rrs.num_nodes_in_maintenance > 0) {
        if (!result.violations.is_empty()) {
            result.status = status::waiting_for_maintenance_end;
        }
        return result;
    }

    if (_state.topics().has_updates_in_progress()) {
        get_unavailable_node_movement_cancellations(result, rrs);
        if (!result.cancellations.empty()) {
            result.status = status::cancellations_planned;
        }
        return result;
    }

    if (!all_reports_received(rrs)) {
        result.status = status::waiting_for_reports;
        return result;
    }

    if (
      result.violations.is_empty()
      && _state.ntps_with_broken_rack_constraint().empty()) {
        result.status = status::empty;
        return result;
    }

    init_ntp_sizes_from_health_report(health_report, rrs);

    get_unavailable_nodes_reassignments(result, rrs);
    get_rack_constraint_repair_reassignments(result, rrs);
    get_full_node_reassignments(result, rrs);

    if (!result.reassignments.empty()) {
        result.status = status::movement_planned;
    }

    return result;
}

} // namespace cluster
