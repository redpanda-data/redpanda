/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/partition_balancer_types.h"

#include "utils/human.h"
#include "utils/to_string.h"

namespace cluster {

node_disk_space::node_disk_space(
  model::node_id node_id, uint64_t total, uint64_t used)
  : node_id(node_id)
  , total(total)
  , used(used) {}

double node_disk_space::final_used_ratio() const {
    // it sometimes may happen  that the partition replica size on one node
    // is out of date with the total used size reported by a node space
    // manager. This may lead to an overflow of final used ratio.
    if (released >= used + assigned) {
        return 0.0;
    }

    return double(used + assigned - released) / total;
}

std::ostream& operator<<(std::ostream& o, const node_disk_space& d) {
    fmt::print(
      o,
      "{{total: {}, used: {}, assigned: {}, released: {}; "
      "used ratios: orig: {:.4}, peak: {:.4}, final: {:.4}}}",
      human::bytes(d.total),
      human::bytes(d.used),
      human::bytes(d.assigned),
      human::bytes(d.released),
      d.original_used_ratio(),
      d.peak_used_ratio(),
      d.final_used_ratio());
    return o;
}

partition_balancer_violations::unavailable_node::unavailable_node(
  model::node_id id, model::timestamp unavailable_since)
  : id(id)
  , unavailable_since(unavailable_since) {}

std::ostream& operator<<(
  std::ostream& o, const partition_balancer_violations::unavailable_node& u) {
    fmt::print(o, "{{ id: {} since: {} }}", u.id, u.unavailable_since);
    return o;
}

partition_balancer_violations::full_node::full_node(
  model::node_id id, uint32_t disk_used_percent)
  : id(id)
  , disk_used_percent(disk_used_percent) {}

std::ostream&
operator<<(std::ostream& o, const partition_balancer_violations::full_node& f) {
    fmt::print(
      o, "{{ id: {} disk_used_percent: {} }}", f.id, f.disk_used_percent);
    return o;
}

partition_balancer_violations::partition_balancer_violations(
  std::vector<unavailable_node> un, std::vector<full_node> fn)
  : unavailable_nodes(std::move(un))
  , full_nodes(std::move(fn)) {}

void partition_balancer_overview_reply::set_reallocation_failures(
  const chunked_hash_map<model::ntp, reallocation_failure_details>&
    reallocations) {
    reallocation_failures.reserve(reallocations.size());
    for (const auto& [ntp, details] : reallocations) {
        reallocation_failures.emplace(ntp, details);
        /**
         * Fill in the decommission_realloc_failures map with the reallocation
         * failures for backward compatibility.
         */
        if (details.reason == change_reason::node_decommissioning) {
            auto& failed_ntps
              = decommission_realloc_failures[details.replica_to_move];
            failed_ntps.insert(ntp);
        }
    }
}

partition_balancer_overview_reply
partition_balancer_overview_reply::copy() const {
    partition_balancer_overview_reply copy;
    copy.error = error;
    copy.last_tick_time = last_tick_time;
    copy.status = status;
    copy.violations = violations;
    copy.partitions_pending_force_recovery_count
      = partitions_pending_force_recovery_count;
    copy.partitions_pending_force_recovery_sample
      = partitions_pending_force_recovery_sample;
    copy.decommission_realloc_failures = decommission_realloc_failures;
    copy.reallocation_failures.reserve(reallocation_failures.size());
    for (const auto& [ntp, details] : reallocation_failures) {
        copy.reallocation_failures.emplace(ntp, details);
    }
    return copy;
}

std::ostream&
operator<<(std::ostream& o, const partition_balancer_violations& v) {
    fmt::print(
      o,
      "{{ unavailable_nodes: {} full_nodes: {} }}",
      v.unavailable_nodes,
      v.full_nodes);
    return o;
}

std::ostream& operator<<(std::ostream& os, partition_balancer_status status) {
    switch (status) {
    case partition_balancer_status::off:
        os << "off";
        break;
    case partition_balancer_status::starting:
        os << "starting";
        break;
    case partition_balancer_status::ready:
        os << "ready";
        break;
    case partition_balancer_status::in_progress:
        os << "in_progress";
        break;
    case partition_balancer_status::stalled:
        os << "stalled";
        break;
    }
    return os;
}

std::ostream&
operator<<(std::ostream& o, const partition_balancer_overview_request&) {
    fmt::print(o, "{{}}");
    return o;
}

std::ostream&
operator<<(std::ostream& o, const partition_balancer_overview_reply& rep) {
    fmt::print(
      o,
      "{{ error: {} last_tick_time: {} status: {} violations: {}, "
      "partitions_pending_force_recovery: {}, "
      "decommission_reallocation_failures_count: {}, reallocation_failures: "
      "{}}}",
      rep.error,
      rep.last_tick_time,
      rep.status,
      rep.violations,
      rep.partitions_pending_force_recovery_count,
      rep.decommission_realloc_failures.size(),
      fmt::join(rep.reallocation_failures | std::views::values, ", "));
    return o;
}

std::ostream& operator<<(std::ostream& o, change_reason reason) {
    switch (reason) {
    case change_reason::rack_constraint_repair:
        return o << "rack_constraint_repair";
    case change_reason::partition_count_rebalancing:
        return o << "partition_count_rebalancing";
    case change_reason::node_decommissioning:
        return o << "node_decommissioning";
    case change_reason::node_unavailable:
        return o << "node_unavailable";
    case change_reason::disk_full:
        return o << "disk_full";
    }
}

std::ostream& operator<<(std::ostream& o, reallocation_error err) {
    switch (err) {
    case reallocation_error::missing_partition_size_info:
        return o << "Missing partition size information, all replicas may be "
                    "offline";
    case reallocation_error::no_eligible_node_found:
        return o << "No eligible node found to move replica";
    case reallocation_error::over_partition_fd_limit:
        return o << "Over the total partition file descriptor limit";
    case reallocation_error::over_partition_memory_limit:
        return o << "Over the total partition memory limit";
    case reallocation_error::over_partition_core_limit:
        return o << "Over the partition per core limit";
    case reallocation_error::no_quorum:
        return o << "No quorum, majority of replicas are offline";
    case reallocation_error::reconfiguration_in_progress:
        return o << "Non cancellable reconfiguration in progress";
    case reallocation_error::partition_disabled:
        return o << "Partition is disabled";
    case reallocation_error::unknown_error:
        return o << "Unknown error or error not reported";
    }
}

std::ostream&
operator<<(std::ostream& o, const reallocation_failure_details& details) {
    fmt::print(
      o,
      "{{replica_to_move: {}, reason: {}, error: {}}}",
      details.replica_to_move,
      details.reason,
      details.error);
    return o;
}

} // namespace cluster
