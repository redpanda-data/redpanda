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

#include "base/format_to.h"
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
fmt::iterator node_disk_space::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{total: {}, used: {}, assigned: {}, released: {}; "
      "used ratios: orig: {:.4}, peak: {:.4}, final: {:.4}}}",
      human::bytes(total),
      human::bytes(used),
      human::bytes(assigned),
      human::bytes(released),
      original_used_ratio(),
      peak_used_ratio(),
      final_used_ratio());
}

partition_balancer_violations::unavailable_node::unavailable_node(
  model::node_id id, model::timestamp unavailable_since)
  : id(id)
  , unavailable_since(unavailable_since) {}
fmt::iterator partition_balancer_violations::unavailable_node::format_to(
  fmt::iterator it) const {
    return fmt::format_to(it, "{{ id: {} since: {} }}", id, unavailable_since);
}

partition_balancer_violations::full_node::full_node(
  model::node_id id, uint32_t disk_used_percent)
  : id(id)
  , disk_used_percent(disk_used_percent) {}
fmt::iterator
partition_balancer_violations::full_node::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it, "{{ id: {} disk_used_percent: {} }}", id, disk_used_percent);
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
fmt::iterator partition_balancer_violations::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ unavailable_nodes: {} full_nodes: {} }}",
      unavailable_nodes,
      full_nodes);
}
fmt::iterator format_to(partition_balancer_status e, fmt::iterator out) {
    switch (e) {
    case partition_balancer_status::off:
        return fmt::format_to(out, "off");
    case partition_balancer_status::starting:
        return fmt::format_to(out, "starting");
    case partition_balancer_status::ready:
        return fmt::format_to(out, "ready");
    case partition_balancer_status::in_progress:
        return fmt::format_to(out, "in_progress");
    case partition_balancer_status::stalled:
        return fmt::format_to(out, "stalled");
    }
    return fmt::format_to(out, "");
}
fmt::iterator
partition_balancer_overview_request::format_to(fmt::iterator it) const {
    return fmt::format_to(it, "{{}}");
}
fmt::iterator
partition_balancer_overview_reply::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{ error: {} last_tick_time: {} status: {} violations: {}, "
      "partitions_pending_force_recovery: {}, "
      "decommission_reallocation_failures_count: {}, reallocation_failures: "
      "{}}}",
      error,
      last_tick_time,
      status,
      violations,
      partitions_pending_force_recovery_count,
      decommission_realloc_failures.size(),
      fmt::join(reallocation_failures | std::views::values, ", "));
}
fmt::iterator format_to(change_reason e, fmt::iterator out) {
    switch (e) {
    case change_reason::rack_constraint_repair:
        return fmt::format_to(out, "rack_constraint_repair");
    case change_reason::partition_count_rebalancing:
        return fmt::format_to(out, "partition_count_rebalancing");
    case change_reason::node_decommissioning:
        return fmt::format_to(out, "node_decommissioning");
    case change_reason::node_unavailable:
        return fmt::format_to(out, "node_unavailable");
    case change_reason::disk_full:
        return fmt::format_to(out, "disk_full");
    case change_reason::replica_pinning_repair:
        return fmt::format_to(out, "replica_pinning_repair");
    }
}
fmt::iterator format_to(reallocation_error e, fmt::iterator out) {
    switch (e) {
    case reallocation_error::missing_partition_size_info:
        return fmt::format_to(
          out,
          "Missing partition size information, all replicas may be offline");
    case reallocation_error::no_eligible_node_found:
        return fmt::format_to(out, "No eligible node found to move replica");
    case reallocation_error::over_partition_fd_limit:
        return fmt::format_to(
          out, "Over the total partition file descriptor limit");
    case reallocation_error::over_partition_memory_limit:
        return fmt::format_to(out, "Over the total partition memory limit");
    case reallocation_error::over_partition_core_limit:
        return fmt::format_to(out, "Over the partition per core limit");
    case reallocation_error::no_quorum:
        return fmt::format_to(
          out, "No quorum, majority of replicas are offline");
    case reallocation_error::reconfiguration_in_progress:
        return fmt::format_to(
          out, "Non cancellable reconfiguration in progress");
    case reallocation_error::partition_disabled:
        return fmt::format_to(out, "Partition is disabled");
    case reallocation_error::unknown_error:
        return fmt::format_to(out, "Unknown error or error not reported");
    }
}
fmt::iterator reallocation_failure_details::format_to(fmt::iterator it) const {
    return fmt::format_to(
      it,
      "{{replica_to_move: {}, reason: {}, error: {}}}",
      replica_to_move,
      reason,
      error);
}

} // namespace cluster
