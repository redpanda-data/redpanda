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

#include "cluster/scheduling/allocation_node.h"
#include "model/metadata.h"

namespace cluster {
/**
 * Partition allocator state
 */
class allocation_state {
public:
    using node_t = allocation_node;
    using node_ptr = std::unique_ptr<node_t>;
    // we use ordered container to achieve deterministic ordering of nodes
    using underlying_t = absl::btree_map<model::node_id, node_ptr>;

    allocation_state(
      config::binding<uint32_t> partitions_per_shard,
      config::binding<uint32_t> partitions_reserve_shard0)
      : _partitions_per_shard(partitions_per_shard)
      , _partitions_reserve_shard0(partitions_reserve_shard0) {}

    // Allocation nodes
    void register_node(node_ptr);
    void update_allocation_nodes(const std::vector<model::broker>&);
    void decommission_node(model::node_id);
    void recommission_node(model::node_id);
    bool is_empty(model::node_id) const;
    bool contains_node(model::node_id n) const { return _nodes.contains(n); }
    const underlying_t& allocation_nodes() const { return _nodes; }
    int16_t available_nodes() const;

    // Operations on state
    void deallocate(const model::broker_shard&);
    void apply_update(std::vector<model::broker_shard>, raft::group_id);
    result<uint32_t> allocate(model::node_id id);

    void rollback(const std::vector<partition_assignment>& pa);
    void rollback(const std::vector<model::broker_shard>& v);

    bool validate_shard(model::node_id node, uint32_t shard) const;

    // Raft group id
    raft::group_id next_group_id();
    raft::group_id last_group_id() const { return _highest_group; }

    // Get rack information
    //
    // Return rack id or nullopt if rack id is not configured
    // for the broker.
    std::optional<model::rack_id> get_rack_id(model::node_id) const;

private:
    config::binding<uint32_t> _partitions_per_shard;
    config::binding<uint32_t> _partitions_reserve_shard0;

    raft::group_id _highest_group{0};
    underlying_t _nodes;
};
} // namespace cluster
