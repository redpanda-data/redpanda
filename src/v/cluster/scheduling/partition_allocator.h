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

#include "cluster/fwd.h"
#include "cluster/scheduling/allocation_node.h"
#include "cluster/scheduling/allocation_state.h"
#include "cluster/scheduling/allocation_strategy.h"
#include "cluster/scheduling/types.h"
#include "config/property.h"
#include "features/fwd.h"

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
      ss::sharded<features::feature_table>&,
      config::binding<std::optional<size_t>> memory_per_partition,
      config::binding<std::optional<int32_t>> fds_per_partition,
      config::binding<uint32_t> partitions_per_shard,
      config::binding<uint32_t> partitions_reserve_shard0,
      config::binding<std::vector<ss::sstring>>
        kafka_topics_skipping_allocation,
      config::binding<bool> enable_rack_awareness);

    // Replica placement APIs

    /**
     * Return an allocation_units object wrapping the result of the allocating
     * the given allocation request, or an error if it was not possible.
     */
    ss::future<result<allocation_units::pointer>> allocate(allocation_request);

    /// Reallocate some replicas of an already existing partition without
    /// changing its replication factor.
    ///
    /// If existing_replica_counts is non-null, new replicas will be allocated
    /// using topic-aware counts objective and (if all allocations are
    /// successful) existing_replica_counts will be updated with newly allocated
    /// replicas.
    result<allocated_partition> reallocate_partition(
      model::ntp ntp,
      std::vector<model::broker_shard> current_replicas,
      const std::vector<model::node_id>& replicas_to_reallocate,
      allocation_constraints,
      node2count_t* existing_replica_counts);

    /// Create allocated_partition object from current replicas for use with the
    /// allocate_replica method.
    allocated_partition make_allocated_partition(
      model::ntp ntp, std::vector<model::broker_shard> replicas) const;

    /// try to substitute an existing replica with a newly allocated one and add
    /// it to the allocated_partition object. If the request fails,
    /// allocated_partition remains unchanged.
    ///
    /// Note: if after reallocation the replica ends up on a node from the
    /// original replica set (doesn't matter if the same as `previous` or a
    /// different one), its shard id is preserved.
    result<reallocation_step> reallocate_replica(
      allocated_partition&, model::node_id previous, allocation_constraints);

    // State accessors

    bool is_rack_awareness_enabled() const { return _enable_rack_awareness(); }

    bool is_empty(model::node_id id) const { return _state->is_empty(id); }
    bool contains_node(model::node_id n) const {
        return _state->contains_node(n);
    }

    const allocation_state& state() const { return *_state; }

    // State update functions called when processing controller commands

    // Node state updates

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

    // Partition state updates

    /// Best effort. Do not throw if we cannot find the replicas.
    void add_allocations(const std::vector<model::broker_shard>&);
    void remove_allocations(const std::vector<model::broker_shard>&);
    void add_final_counts(const std::vector<model::broker_shard>&);
    void remove_final_counts(const std::vector<model::broker_shard>&);

    void add_allocations_for_new_partition(
      const std::vector<model::broker_shard>& replicas,
      raft::group_id group_id) {
        add_allocations(replicas);
        add_final_counts(replicas);
        _state->update_highest_group_id(group_id);
    }

    ss::future<> apply_snapshot(const controller_snapshot&);

private:
    std::error_code
    check_cluster_limits(const allocation_request& request) const;

    result<reallocation_step> do_allocate_replica(
      allocated_partition&,
      std::optional<model::node_id> previous,
      const allocation_constraints&);

    allocation_constraints default_constraints();

    std::unique_ptr<allocation_state> _state;
    allocation_strategy _allocation_strategy;
    ss::sharded<members_table>& _members;
    features::feature_table& _feature_table;

    config::binding<std::optional<size_t>> _memory_per_partition;
    config::binding<std::optional<int32_t>> _fds_per_partition;
    config::binding<uint32_t> _partitions_per_shard;
    config::binding<uint32_t> _partitions_reserve_shard0;
    config::binding<std::vector<ss::sstring>> _internal_kafka_topics;
    config::binding<bool> _enable_rack_awareness;
};
} // namespace cluster
