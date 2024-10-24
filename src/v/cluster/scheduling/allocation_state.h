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

#include "base/oncore.h"
#include "cluster/scheduling/allocation_node.h"
#include "features/fwd.h"
#include "model/metadata.h"

#include <seastar/core/weak_ptr.hh>

namespace cluster {
/**
 * Partition allocator state
 */
class allocation_state : public ss::weakly_referencable<allocation_state> {
public:
    using node_t = allocation_node;
    using node_ptr = std::unique_ptr<node_t>;
    // we use ordered container to achieve deterministic ordering of nodes
    using underlying_t = absl::btree_map<model::node_id, node_ptr>;

    allocation_state(
      features::feature_table& feature_table,
      config::binding<uint32_t> partitions_per_shard,
      config::binding<uint32_t> partitions_reserve_shard0,
      config::binding<std::vector<ss::sstring>> internal_kafka_topics)
      : _feature_table(feature_table)
      , _partitions_per_shard(std::move(partitions_per_shard))
      , _partitions_reserve_shard0(std::move(partitions_reserve_shard0))
      , _internal_kafka_topics(std::move(internal_kafka_topics)) {}

    // Allocation nodes
    void register_node(node_ptr);
    void register_node(const model::broker&, allocation_node::state);
    void update_allocation_nodes(const std::vector<model::broker>&);
    void upsert_allocation_node(const model::broker&);
    void remove_allocation_node(model::node_id);

    void decommission_node(model::node_id);
    void recommission_node(model::node_id);
    bool is_empty(model::node_id) const;
    bool contains_node(model::node_id n) const { return _nodes.contains(n); }
    const underlying_t& allocation_nodes() const { return _nodes; }
    int16_t available_nodes() const;

    bool node_local_core_assignment_enabled() const;

    // Choose a shard for a replica and add the corresponding allocation.
    // node_id is required to belong to an existing node.
    uint32_t allocate(model::node_id id);

    // Operations on state
    void add_allocation(const model::broker_shard&);
    void remove_allocation(const model::broker_shard&);
    void add_final_count(const model::broker_shard&);
    void remove_final_count(const model::broker_shard&);

    bool validate_shard(model::node_id node, uint32_t shard) const;

    // Raft group id
    raft::group_id next_group_id();
    raft::group_id last_group_id() const { return _highest_group; }
    void update_highest_group_id(raft::group_id id) {
        _highest_group = std::max(_highest_group, id);
    }

private:
    /**
     * This function verifies that the current shard matches the shard the
     * state was originally created on, aborting if not.
     */
    void verify_shard() const;

    features::feature_table& _feature_table;
    config::binding<uint32_t> _partitions_per_shard;
    config::binding<uint32_t> _partitions_reserve_shard0;
    config::binding<std::vector<ss::sstring>> _internal_kafka_topics;

    raft::group_id _highest_group{0};
    underlying_t _nodes;
    expression_in_debug_mode(oncore _verify_shard;)
};
} // namespace cluster
