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

#include "cluster/scheduling/allocation_node.h"
#include "cluster/scheduling/types.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "utils/intrusive_list_helpers.h"
#include "vassert.h"

#include <boost/container/flat_map.hpp>

#include <vector>

namespace cluster {
class partition_allocator;

struct partition_allocator_tester;
class partition_allocator {
public:
    static constexpr ss::shard_id shard = 0;
    using value_type = allocation_node;
    using ptr = std::unique_ptr<value_type>;
    using underlying_t = boost::container::flat_map<model::node_id, ptr>;
    using iterator = underlying_t::iterator;
    using cil_t = counted_intrusive_list<value_type, &allocation_node::_hook>;

    /// should only be initialized _after_ we become the leader so we know we
    /// are up to date, and have the highest known group_id ever assigned
    /// reset to nullptr when no longer leader
    explicit partition_allocator(raft::group_id highest_known_group)
      : _highest_group(highest_known_group) {
        _rr = _available_machines.end();
    }
    void register_node(ptr n) {
        _available_machines.push_back(*n);
        _machines.emplace(n->id(), std::move(n));
    }

    void unregister_node(model::node_id);
    void decommission_node(model::node_id);
    void recommission_node(model::node_id);
    bool is_empty(model::node_id);

    bool contains_node(model::node_id n) { return _machines.contains(n); }

    /// best effort placement.
    /// kafka/common/protocol/Errors.java does not have a way to
    /// represent failed allocation yet. Up to caller to interpret
    /// how to use a nullopt value
    result<allocation_units> allocate(const topic_configuration&);

    /// Realocates partition replicas, moving them away from decommissioned
    /// nodes. Replicas on nodes that were left untouched are not changed.
    ///
    /// Returns an empty optional if it reallocation is impossible
    result<allocation_units>
    reassign_decommissioned_replicas(const partition_assignment&);

    /// best effort. Does not throw if we cannot find the old partition
    void deallocate(const model::broker_shard&);

    /// updates the state of allocation, it is used during recovery and
    /// when processing raft0 committed notifications
    void update_allocation_state(
      std::vector<model::topic_metadata>, raft::group_id);
    void
      update_allocation_state(std::vector<model::broker_shard>, raft::group_id);

    const underlying_t& allocation_nodes() { return _machines; }

    ~partition_allocator() {
        _available_machines.clear();
        _rr = _available_machines.end();
    }

private:
    friend partition_allocator_tester;
    /// rolls back partition assignment, only decrementing
    /// raft-group by distinct raft-group counts
    /// assumes sorted in raft-group order
    void rollback(const std::vector<partition_assignment>& pa);
    void rollback(const std::vector<model::broker_shard>& v);

    std::optional<std::vector<model::broker_shard>>
    allocate_replicas(int16_t, const std::vector<model::broker_shard>&);
    iterator find_node(model::node_id id);

    [[gnu::always_inline]] inline cil_t::iterator& round_robin_ptr() {
        if (_rr == _available_machines.end() || !_rr.pointed_node()) {
            _rr = _available_machines.begin();
        }
        return _rr;
    }
    raft::group_id _highest_group;

    cil_t::iterator _rr; // round robin
    cil_t _available_machines;
    underlying_t _machines;

    // for testing
    void test_only_saturate_all_machines();
    uint32_t test_only_max_cluster_allocation_partition_capacity() const;
};

} // namespace cluster
