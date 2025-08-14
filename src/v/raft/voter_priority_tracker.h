/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once
#include "raft/fundamental.h"
namespace raft {

// priority used to implement semi-deterministic leader election
using voter_priority = named_type<uint32_t, struct voter_priority_tag>;

class voter_priority_tracker {
public:
    voter_priority_tracker(raft::vnode self, bool is_ready_for_leader_election);

    // Sets the priority to the minimum value, blocking the replica from
    // becoming a leader. The replica can only become a leader after the target
    // priority of other raft group replicas is lower than or equal to the
    // min_voter_priority. This will require multiple failed leader elections
    // but eventually a replica with min voter priority can be elected as a
    // leader.
    void set_min_voter_priority();

    /*
     * Resets the priority override to the default value
     */
    void reset_voter_priority_override();

    /**
     * Resets node priority override if it was set to min priority to mark the
     * node not ready to become a leader.
     */
    void mark_ready_for_leader_election();

    /**
     * Called to update the next target priority for the next leader election.
     */
    void on_leader_election(size_t replica_count);

    /**
     * Called when a new leader is elected. It resets the target priority to
     * max voter priority.
     */
    void on_successful_leader_election();

    /**
     * Returns the priority of the replica.
     */
    voter_priority get_replica_priority(
      const vnode& replica_id, const std::vector<vnode>& all_replicas) const;

    /**
     * Returns current target priority. i.e. the priority threshold for the
     * successful vote to be casted for a candidate. If candidate priority is
     * lower then the threshold current voter will not vote for the candidate.
     */
    voter_priority target_priority() const { return _target_priority; }

    /**
     * Return true if current replica is blocked from becoming a leader. f.e.
     * during the maintenance mode.
     */
    bool is_blocked() const;

private:
    vnode _self;
    std::optional<voter_priority> _replica_priority_override;
    voter_priority _target_priority = voter_priority::max();
};

} // namespace raft
