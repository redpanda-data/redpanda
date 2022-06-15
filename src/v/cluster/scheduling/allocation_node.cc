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

#include "cluster/scheduling/allocation_node.h"

namespace cluster {
allocation_node::allocation_node(
  model::node_id id,
  uint32_t cpus,
  absl::node_hash_map<ss::sstring, ss::sstring> labels,
  std::optional<model::rack_id> rack,
  config::binding<uint32_t> partitions_per_shard,
  config::binding<uint32_t> partitions_reserve_shard0)
  : _id(id)
  , _weights(cpus)
  , _max_capacity((cpus * partitions_per_shard()) - partitions_reserve_shard0())
  , _machine_labels(std::move(labels))
  , _rack(std::move(rack))
  , _partitions_per_shard(std::move(partitions_per_shard))
  , _partitions_reserve_shard0(std::move(partitions_reserve_shard0))
  , _cpus(cpus) {
    // add extra weights to core 0
    _weights[0] = _partitions_reserve_shard0();
    _shard0_reserved = _partitions_reserve_shard0();
    _partitions_reserve_shard0.watch([this]() {
        int32_t delta = static_cast<int32_t>(_partitions_reserve_shard0())
                        - static_cast<int32_t>(_shard0_reserved);
        _weights[0] += delta;
        _shard0_reserved += delta;
    });

    _partitions_per_shard.watch([this]() {
        _max_capacity = allocation_capacity{
          (_cpus * _partitions_per_shard()) - _partitions_reserve_shard0()};
    });

    _partitions_reserve_shard0.watch([this]() {
        _max_capacity = allocation_capacity{
          (_cpus * _partitions_per_shard()) - _partitions_reserve_shard0()};
    });
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

void allocation_node::update_core_count(uint32_t core_count) {
    vassert(
      core_count >= cpus(),
      "decreasing node core count is not supported, current core count {} > "
      "requested core count {}",
      cpus(),
      core_count);
    auto current_cpus = cpus();
    for (auto i = current_cpus; i < core_count; ++i) {
        _weights.push_back(0);
    }
    _max_capacity = allocation_capacity(
      (core_count * _partitions_per_shard()) - _partitions_reserve_shard0());
}

std::ostream& operator<<(std::ostream& o, allocation_node::state s) {
    switch (s) {
    case allocation_node::state::active:
        return o << "active";
    case allocation_node::state::decommissioned:
        return o << "decommissioned";
    case allocation_node::state::deleted:
        return o << "deleted";
    }
    return o << "unknown";
}

std::ostream& operator<<(std::ostream& o, const allocation_node& n) {
    fmt::print(
      o,
      "{{node: {}, max_partitions_per_core: {}, state: {}, partition_capacity: "
      "{}, weights: [",
      n._id,
      n._partitions_per_shard(),
      n._state,
      n.partition_capacity());

    for (auto w : n._weights) {
        fmt::print(o, "({})", w);
    }
    fmt::print(o, "]}}");
    return o;
}

} // namespace cluster
