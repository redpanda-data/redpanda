/*
 * Copyright 2020 Redpanda Data, Inc.
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

// This class keeps track of the partition assignments of each group, also
// tracking the lifecycle events (decommissioning, etc) of each node to
// determine where to place new partitions.
//
// Under the hood, this doesn't do any bookkeeping of what partitions exist on
// each shard, only how many partition replicas are assigned per shard.
class partition_allocator {
public:
    static constexpr ss::shard_id shard = 0;
    partition_allocator(
      ss::sharded<members_table>&,
      config::binding<std::optional<size_t>>,
      config::binding<std::optional<int32_t>>,
      config::binding<uint32_t>,
      config::binding<uint32_t>,
      config::binding<bool>);

    void register_node(allocation_state::node_ptr n) {
        _state->register_node(std::move(n));
    }

    void update_allocation_nodes(const std::vector<model::broker>& brokers) {
        _state->update_allocation_nodes(brokers);
    }

    void upsert_allocation_node(const model::broker& broker) {
        _state->upsert_allocation_node(broker);
    }

    void remove_allocation_node(model::node_id id) {
        _state->remove_allocation_node(id);
    }
    void decommission_node(model::node_id id) { _state->decommission_node(id); }
    void recommission_node(model::node_id id) { _state->recommission_node(id); }

    bool is_empty(model::node_id id) const { return _state->is_empty(id); }
    bool contains_node(model::node_id n) const {
        return _state->contains_node(n);
    }

    /**
     * Return an allocation_units object wrapping the result of the allocating
     * the given allocation request, or an error if it was not possible.
     */
    ss::future<result<allocation_units::pointer>> allocate(allocation_request);

    result<allocation_units> reallocate_partition(
      partition_constraints,
      const partition_assignment&,
      partition_allocation_domain);

    /// Best effort. Does not throw if we cannot find the replicas.
    /// Allocation domain must match the one used to allocate the partition.
    void deallocate(
      const std::vector<model::broker_shard>&, partition_allocation_domain);

    /// updates the state of allocation, it is used during recovery and
    /// when processing raft0 committed notifications.
    /// Allocation domain must match the one used to allocate the partition.
    void update_allocation_state(
      const std::vector<model::broker_shard>&,
      raft::group_id,
      partition_allocation_domain);

    void add_allocations(
      const std::vector<model::broker_shard>&, partition_allocation_domain);
    void remove_allocations(
      const std::vector<model::broker_shard>&, partition_allocation_domain);

    bool is_rack_awareness_enabled() const { return _enable_rack_awareness(); }

    allocation_state& state() { return *_state; }
    const allocation_state& state() const { return *_state; }

    ss::future<> apply_snapshot(const controller_snapshot&);

private:
    template<typename T>
    class intermediate_allocation {
    public:
        intermediate_allocation(
          allocation_state& state,
          size_t res,
          const partition_allocation_domain domain)
          : _state(state.weak_from_this())
          , _domain(domain) {
            _partial.reserve(res);
        }
        void push_back(T t) { _partial.push_back(std::move(t)); }

        template<typename... Args>
        void emplace_back(Args&&... args) {
            _partial.emplace_back(std::forward<Args>(args)...);
        }

        const std::vector<T>& get() const { return _partial; }
        std::vector<T> finish() && { return std::exchange(_partial, {}); }
        intermediate_allocation(intermediate_allocation&&) noexcept = default;

        intermediate_allocation(
          const intermediate_allocation&) noexcept = delete;
        intermediate_allocation&
        operator=(intermediate_allocation&&) noexcept = default;
        intermediate_allocation&
        operator=(const intermediate_allocation&) noexcept = delete;

        ~intermediate_allocation() {
            if (_state) {
                _state->rollback(_partial, _domain);
            }
        }

    private:
        std::vector<T> _partial;
        ss::weak_ptr<allocation_state> _state;
        partition_allocation_domain _domain;
    };

    std::error_code
    check_cluster_limits(allocation_request const& request) const;

    result<std::vector<model::broker_shard>> allocate_partition(
      partition_constraints,
      partition_allocation_domain,
      const std::vector<model::broker_shard>& not_changed_replicas = {});

    result<std::vector<model::broker_shard>> do_reallocate_partition(
      partition_constraints,
      partition_allocation_domain,
      const std::vector<model::broker_shard>&);

    allocation_constraints
    default_constraints(const partition_allocation_domain);

    std::unique_ptr<allocation_state> _state;
    allocation_strategy _allocation_strategy;
    ss::sharded<members_table>& _members;

    config::binding<std::optional<size_t>> _memory_per_partition;
    config::binding<std::optional<int32_t>> _fds_per_partition;
    config::binding<uint32_t> _partitions_per_shard;
    config::binding<uint32_t> _partitions_reserve_shard0;
    config::binding<bool> _enable_rack_awareness;
};
} // namespace cluster
