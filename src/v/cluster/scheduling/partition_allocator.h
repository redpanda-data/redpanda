/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "cluster/logger.h"
#include "cluster/scheduling/allocation_node.h"
#include "cluster/scheduling/allocation_state.h"
#include "cluster/scheduling/allocation_strategy.h"
#include "cluster/scheduling/types.h"
#include "config/property.h"
#include "vlog.h"

namespace cluster {

class partition_allocator {
public:
    static constexpr ss::shard_id shard = 0;
    partition_allocator(
      ss::sharded<members_table>&,
      config::binding<std::optional<size_t>>,
      config::binding<std::optional<int32_t>>,
      config::binding<size_t>,
      config::binding<bool>);

    void register_node(allocation_state::node_ptr n) {
        _state->register_node(std::move(n));
    }

    void update_allocation_nodes(const std::vector<model::broker>& brokers) {
        _state->update_allocation_nodes(brokers);
    }
    void decommission_node(model::node_id id) { _state->decommission_node(id); }
    void recommission_node(model::node_id id) { _state->recommission_node(id); }

    bool is_empty(model::node_id id) const { return _state->is_empty(id); }
    bool contains_node(model::node_id n) const {
        return _state->contains_node(n);
    }

    result<allocation_units> allocate(allocation_request);

    /// Realocates partition replicas, moving them away from decommissioned
    /// nodes. Replicas on nodes that were left untouched are not changed.
    ///
    /// Returns an error it reallocation is impossible
    result<allocation_units>
    reassign_decommissioned_replicas(const partition_assignment&);

    result<allocation_units>
    reallocate_partition(partition_constraints, const partition_assignment&);

    /// best effort. Does not throw if we cannot find the old partition
    void deallocate(const std::vector<model::broker_shard>&);

    /// updates the state of allocation, it is used during recovery and
    /// when processing raft0 committed notifications
    void update_allocation_state(
      const std::vector<model::broker_shard>&, raft::group_id);

    /// updates the state of allocation, it is used during recovery and
    /// when processing raft0 committed notifications
    void update_allocation_state(
      const std::vector<model::broker_shard>&,
      const std::vector<model::broker_shard>&);

    allocation_state& state() { return *_state; }

private:
    template<typename T>
    class intermediate_allocation {
    public:
        intermediate_allocation(allocation_state& state, size_t res)
          : _state(state) {
            _partial.reserve(res);
        }
        void push_back(T t) { _partial.push_back(std::move(t)); }

        const std::vector<T>& get() const { return _partial; }
        std::vector<T> finish() && { return std::exchange(_partial, {}); }
        intermediate_allocation(intermediate_allocation&&) noexcept = default;

        intermediate_allocation(
          const intermediate_allocation&) noexcept = delete;
        intermediate_allocation&
        operator=(intermediate_allocation&&) noexcept = default;
        intermediate_allocation&
        operator=(const intermediate_allocation&) noexcept = delete;

        ~intermediate_allocation() { _state.rollback(_partial); }

    private:
        std::vector<T> _partial;
        allocation_state& _state;
    };

    std::error_code
    check_cluster_limits(allocation_request const& request) const;

    result<std::vector<model::broker_shard>>
      allocate_partition(partition_constraints);

    result<std::vector<model::broker_shard>> do_reallocate_partition(
      partition_constraints, const std::vector<model::broker_shard>&);

    std::unique_ptr<allocation_state> _state;
    allocation_strategy _allocation_strategy;
    ss::sharded<members_table>& _members;

    config::binding<std::optional<size_t>> _memory_per_partition;
    config::binding<std::optional<int32_t>> _fds_per_partition;
    config::binding<size_t> _fallocation_step;
    config::binding<bool> _enable_rack_awareness;
};
} // namespace cluster
