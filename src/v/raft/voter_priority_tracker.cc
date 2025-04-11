/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "raft/voter_priority_tracker.h"
namespace raft {
namespace {
// zero priority doesn't allow node to become a leader
inline constexpr voter_priority zero_voter_priority = voter_priority{0};
// 1 is smallest possible priority allowing node to become a leader
inline constexpr voter_priority min_voter_priority = voter_priority{1};
} // namespace

voter_priority_tracker::voter_priority_tracker(
  raft::vnode self, bool is_ready_for_leader_election)
  : _self(self)
  , _replica_priority_override(
      is_ready_for_leader_election ? std::nullopt
                                   : std::make_optional(min_voter_priority)) {}

void voter_priority_tracker::block_new_leadership() {
    _replica_priority_override = zero_voter_priority;
}

void voter_priority_tracker::reset_voter_priority_override() {
    _replica_priority_override.reset();
}

void voter_priority_tracker::on_leader_election(size_t replica_count) {
    auto node_count = std::max<size_t>(replica_count, 1);

    _target_priority = voter_priority(std::max<voter_priority::type>(
      (_target_priority / node_count) * (node_count - 1), min_voter_priority));
}

void voter_priority_tracker::on_successful_leader_election() {
    _target_priority = voter_priority::max();
}

void voter_priority_tracker::mark_ready_for_leader_election() {
    if (_replica_priority_override == raft::min_voter_priority) {
        unblock_new_leadership();
    }
}
/**
 * We use simple policy where we calculate priority based on the position of the
 * node in configuration broker vector. We shuffle brokers in raft configuration
 * so it should give us fairly even distribution of leaders across the nodes.
 */
voter_priority voter_priority_tracker::get_replica_priority(
  const vnode& replica_id, const std::vector<vnode>& all_replicas) const {
    if (_replica_priority_override.has_value() && replica_id == _self) {
        return _replica_priority_override.value();
    }

    auto it = std::ranges::find(all_replicas, replica_id);

    if (it == all_replicas.end()) {
        /**
         * If node is not present in current configuration i.e. was added to the
         * cluster, return max, this way for joining node we will use
         * priorityless, classic raft leader election
         */
        return voter_priority::max();
    }

    auto idx = std::distance(all_replicas.begin(), it);

    /**
     * Voter priority is inversly proportion to node position in brokers
     * vector.
     */
    return voter_priority(
      (all_replicas.size() - idx)
      * (voter_priority::max() / all_replicas.size()));
}

bool voter_priority_tracker::is_blocked() const {
    return _replica_priority_override == zero_voter_priority;
}

} // namespace raft
