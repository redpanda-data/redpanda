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

#include "cluster/scheduling/allocation_node.h"

namespace cluster {
allocation_node::allocation_node(
  model::node_id id,
  uint32_t cpus,
  absl::node_hash_map<ss::sstring, ss::sstring> labels)
  : _id(id)
  , _weights(cpus)
  , _max_capacity((cpus * max_allocations_per_core) - core0_extra_weight)
  , _machine_labels(std::move(labels)) {
    // add extra weights to core 0
    _weights[0] = core0_extra_weight;
}

ss::shard_id allocation_node::allocate() {
    auto it = std::min_element(_weights.begin(), _weights.end());
    (*it)++; // increment the weights
    _allocated_partitions++;
    return std::distance(_weights.begin(), it);
}

void allocation_node::deallocate(ss::shard_id core) {
    vassert(
      core < _weights.size(),
      "Tried to deallocate a non-existing core:{} - {}",
      core,
      *this);
    vassert(
      _allocated_partitions > allocation_capacity{0} && _weights[core] > 0,
      "unable to deallocate partition from core {} at node {}",
      core,
      *this);

    _allocated_partitions--;
    _weights[core]--;
}

void allocation_node::allocate(ss::shard_id core) {
    vassert(
      core < _weights.size(),
      "Tried to allocate a non-existing core:{} - {}",
      core,
      *this);
    _weights[core]++;
    _allocated_partitions++;
}

const absl::node_hash_map<ss::sstring, ss::sstring>&
allocation_node::machine_labels() const {
    return _machine_labels;
}

std::ostream& operator<<(std::ostream& o, const allocation_node& n) {
    o << "{node:" << n._id << ", max_partitions_per_core: "
      << allocation_node::max_allocations_per_core
      << ", partition_capacity:" << n.partition_capacity() << ", weights: [";
    for (auto w : n._weights) {
        o << "(" << w << ")";
    }
    return o << "]}";
}

} // namespace cluster
