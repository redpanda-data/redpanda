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

#include "cluster/logger.h"

#include <fmt/ranges.h>

namespace cluster {
allocation_node::allocation_node(
  model::node_id id,
  uint32_t cpus,
  config::binding<uint32_t> partitions_per_shard,
  config::binding<uint32_t> partitions_reserve_shard0,
  config::binding<std::vector<ss::sstring>> internal_kafka_topics)
  : _id(id)
  , _weights(cpus)
  , _max_capacity((cpus * partitions_per_shard()) - partitions_reserve_shard0())
  , _partitions_per_shard(std::move(partitions_per_shard))
  , _partitions_reserve_shard0(std::move(partitions_reserve_shard0))
  , _internal_kafka_topics(std::move(internal_kafka_topics))
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

bool allocation_node::is_full(
  const model::ntp& ntp, bool will_add_allocation) const {
    // Internal topics are excluded from checks to prevent allocation failures
    // when creating them. This is okay because they are fairly small in number
    // compared to kafka user topic partitions.
    auto is_internal_ns = ntp.ns == model::redpanda_ns
                          || ntp.ns == model::kafka_internal_namespace;
    if (is_internal_ns) {
        return false;
    }
    const auto& internal_topics = _internal_kafka_topics();
    auto is_internal_topic = ntp.ns == model::kafka_namespace
                             && std::any_of(
                               internal_topics.cbegin(),
                               internal_topics.cend(),
                               [&ntp](const ss::sstring& topic) {
                                   return topic == ntp.tp.topic();
                               });

    auto count = _allocated_partitions;
    if (will_add_allocation) {
        count += 1;
    }
    return !is_internal_topic && count > _max_capacity;
}

ss::shard_id allocation_node::allocate_shard() {
    auto it = std::min_element(_weights.begin(), _weights.end());
    (*it)++; // increment the weights
    const ss::shard_id core = std::distance(_weights.begin(), it);
    vlog(
      clusterlog.trace,
      "allocation [node: {}, core: {}], total allocated: {}",
      _id,
      core,
      _allocated_partitions);
    return core;
}

void allocation_node::add_allocation() { _allocated_partitions++; }

void allocation_node::add_allocation(ss::shard_id core) {
    vassert(
      core < _weights.size(),
      "Tried to allocate a non-existing core:{} - {}",
      core,
      *this);
    _weights[core]++;
}

void allocation_node::remove_allocation() {
    vassert(
      _allocated_partitions > allocation_capacity{0},
      "unable to deallocate partition at node {}",
      *this);

    _allocated_partitions--;
}

void allocation_node::remove_allocation(ss::shard_id core) {
    vassert(
      core < _weights.size() && _weights[core] > 0,
      "unable to deallocate partition from core {} at node {}",
      core,
      *this);
    _weights[core]--;
}

void allocation_node::add_final_count() { ++_final_partitions; }

void allocation_node::remove_final_count() { --_final_partitions; }

void allocation_node::update_core_count(uint32_t core_count) {
    auto old_count = _weights.size();
    if (core_count < old_count) {
        _weights.resize(core_count);
    } else {
        for (auto i = old_count; i < core_count; ++i) {
            _weights.push_back(0);
        }
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
      "{{node: {}, max_partitions_per_core: {}, state: {}, allocated: {}, "
      "partition_capacity: {}, weights: [",
      n._id,
      n._partitions_per_shard(),
      n._state,
      n._allocated_partitions,
      n.partition_capacity());

    for (auto w : n._weights) {
        fmt::print(o, "({})", w);
    }
    fmt::print(o, "], allocated: {}}}", n._allocated_partitions);
    return o;
}

} // namespace cluster
