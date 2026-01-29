/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/types.h"
#include "model/fundamental.h"
#include "raft/fundamental.h"
#include "ssx/mutex.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/weak_ptr.hh>

#include <expected>
#include <system_error>

namespace cluster {

class controller;

/*
 * sidecar component to controller, responsible for orchestrating
 * controller_forced_reconfiguration.
 *
 * referred to as CFRM subsequently
 */
class controller_forced_reconfiguration_manager {
private:
    using self_t = controller_forced_reconfiguration_manager;

public:
    // stable pointer, no default construction
    explicit controller_forced_reconfiguration_manager(controller*) noexcept;
    controller_forced_reconfiguration_manager() = delete;
    controller_forced_reconfiguration_manager(const self_t&) = delete;
    self_t& operator=(const self_t&) = delete;
    controller_forced_reconfiguration_manager(self_t&&) = delete;
    self_t& operator=(self_t&&) = delete;
    ~controller_forced_reconfiguration_manager() = default;

    // mark CFRM as ready to accept forced reconfiguration invocations
    ss::future<std::error_code> start(ss::abort_source& shard0_as);

    // cancel all ongoing operations and drain
    ss::future<> stop() noexcept;

    /**
     * @brief CFR is an emergency escape-hatch to be used when a cluster has
     * irrevocably lost quorum on the controller group.
     *
     * This operation is dangerous by design, and can likely lead to data loss.
     *
     * Usage requires the following:
     *     1. the broker is in recovery mode
     *     2. there is no controller leader
     *         - if there is a controller leader, the operator should use normal
     *           controller operations to salvage partitions and remove nodes
     *
     * Behavior:
     * This operation proceeds in 4 steps
     *     0. validation of the above requirements
     *     1. force reconfiguration of the raft0 group to exclude dead_nodes
     *     2. poll for a newly elected controller leader
     * This operation is volatile until completion of step 2
     *     - successful leader election will replicate a configuration batch
     *     - on successful replication, the reduced raft0 group is persistent
     *     - the controller will be durable to leader loss or cluster reset
     *       but CFR will be interrupted, the operator will need to manually
     *       decommission brokers and force reconfigure partitions
     * A new invocation of CFR will cancel the old invocation(s), and block
     * until they have completed.
     */
    ss::future<cluster::error_info>
    initialize_controller_forced_reconfiguration(
      std::vector<model::node_id> dead_nodes, uint16_t quorum_size);

    /**
     * After raft0 force reconfigure, we want a new leader to be elected with
     * the consent of all survivors. We achieve this by adjusting the raft group
     * to the max group size for which the survivors are still majority
     *
     * example: nodes 0,1,2,3,4,5,6 w/t dead_nodes 0,1,2,3 & survivor size 3
     * we calculate a group of [4,5,6],[0,1] <- 2 dead nodes s.t. majority is 3
     *                            ^
     *                            the three living
     */
    static std::expected<std::vector<raft::vnode>, cluster::error_info>
    calculation_reconfiguration_group(
      const std::vector<model::node_id>& dead_nodes,
      std::vector<raft::vnode> current_voters,
      uint16_t surviving_node_size);

private:
    struct holder_bundle {
        ss::gate::holder gate_holder;
        ssx::mutex::units lock_holder;
    };

    // either an invocation of CFR gets all locks, gates, tokens, etc it needs
    // or it fails. Gather all lock-like resources here
    std::expected<holder_bundle, cluster::error_info> gather_holders();

    controller* _controller_ptr{nullptr};
    ss::gate _gate{};
    ss::abort_source _abort_source{};
    ss::optimized_optional<ss::abort_source::subscription> _as_sub{};
    ssx::mutex _execution_lock{"controller_forced_reconfiguration_manager"};
};

} // namespace cluster
