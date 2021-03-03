// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/partition_allocator.h"

#include "cluster/logger.h"
#include "vlog.h"

#include <boost/container_hash/hash.hpp>
#include <fmt/ostream.h>
#include <roaring/roaring.hh>

#include <algorithm>
#include <numeric>
#include <random>

namespace cluster {

void partition_allocator::rollback(const std::vector<partition_assignment>& v) {
    for (auto& as : v) {
        rollback(as.replicas);
        // rollback for each assignment as the groups are distinct
        _highest_group = raft::group_id(_highest_group() - 1);
    }
}

void partition_allocator::rollback(const std::vector<model::broker_shard>& v) {
    for (auto& bs : v) {
        deallocate(bs);
    }
}

static inline bool valid_machine_fault_domain_diversity(
  const std::vector<model::broker_shard>& replicas) {
    model::node_id sentinel = replicas.begin()->node_id;
    const uint32_t unique = std::accumulate(
      replicas.begin(),
      replicas.end(),
      uint32_t(1),
      [&sentinel](uint32_t acc, const model::broker_shard& bs) {
          if (sentinel != bs.node_id) {
              acc += 1;
          }
          return acc;
      });
    return unique == replicas.size();
}

static bool is_machine_in_replicas(
  const allocation_node& machine,
  const std::vector<model::broker_shard>& replicas) {
    return std::any_of(
      replicas.begin(),
      replicas.end(),
      [&machine](const model::broker_shard& bs) {
          return machine.id() == bs.node_id;
      });
}

std::optional<std::vector<model::broker_shard>>
partition_allocator::allocate_replicas(int16_t replication_factor) {
    std::vector<model::broker_shard> replicas;
    replicas.reserve(replication_factor);

    while (replicas.size() < (size_t)replication_factor) {
        const uint16_t replicas_left = replication_factor - replicas.size();
        if (_available_machines.size() < replicas_left) {
            rollback(replicas);
            return std::nullopt;
        }
        auto& rr = round_robin_ptr();
        auto& machine = *rr;
        rr++;
        if (is_machine_in_replicas(machine, replicas)) {
            continue;
        }
        const uint32_t cpu = machine.allocate();
        model::broker_shard bs{.node_id = machine.id(), .shard = cpu};
        replicas.push_back(bs);
        if (machine.is_full()) {
            _available_machines.erase(_available_machines.iterator_to(machine));
        }
    }
    if (!valid_machine_fault_domain_diversity(replicas)) {
        rollback(replicas);
        return std::nullopt;
    }
    return replicas;
}

// FIXME: take into account broker.rack diversity & other constraints
std::optional<partition_allocator::allocation_units>
partition_allocator::allocate(const topic_configuration& cfg) {
    if (_available_machines.empty()) {
        return std::nullopt;
    }
    const int32_t cap = std::accumulate(
      _available_machines.begin(),
      _available_machines.end(),
      int32_t(0),
      [](int32_t acc, const allocation_node& n) {
          return acc + n.partition_capacity();
      });
    if (cap < cfg.partition_count) {
        vlog(
          clusterlog.info,
          "Cannot allocate request: {}. Exceeds maximum capacity left:{}",
          cfg,
          cap);
        return std::nullopt;
    }
    std::vector<partition_assignment> ret;
    ret.reserve(cfg.partition_count);
    for (int32_t i = 0; i < cfg.partition_count; ++i) {
        // all replicas must belong to the same raft group
        raft::group_id partition_group = raft::group_id(_highest_group() + 1);
        auto replicas_assignment = allocate_replicas(cfg.replication_factor);
        if (replicas_assignment == std::nullopt) {
            rollback(ret);
            return std::nullopt;
        }

        partition_assignment p_as{
          .group = partition_group,
          .id = model::partition_id(i),
          .replicas = std::move(*replicas_assignment)};
        ret.push_back(std::move(p_as));
        _highest_group = partition_group;
    }
    return allocation_units(ret, this);
}

void partition_allocator::deallocate(const model::broker_shard& bs) {
    // find in brokers
    auto it = find_node(bs.node_id);
    if (it != _machines.end()) {
        auto& [id, machine] = *it;
        if (bs.shard < machine->cpus()) {
            machine->deallocate(bs.shard);
            if (!machine->_hook.is_linked()) {
                _available_machines.push_back(*machine);
            }
        }
    }
}

void partition_allocator::update_allocation_state(
  std::vector<model::topic_metadata> metadata, raft::group_id gid) {
    if (metadata.empty()) {
        return;
    }

    std::vector<model::broker_shard> shards;
    for (auto const& t_md : metadata) {
        for (auto& p_md : t_md.partitions) {
            std::move(
              p_md.replicas.begin(),
              p_md.replicas.end(),
              std::back_inserter(shards));
        }
    }
    update_allocation_state(shards, gid);
}

void partition_allocator::update_allocation_state(
  std::vector<model::broker_shard> shards, raft::group_id group_id) {
    if (shards.empty()) {
        return;
    }
    _highest_group = std::max(_highest_group, group_id);
    // We can use non stable sort algorithm as we do not need to preserver
    // the order of shards
    std::sort(
      shards.begin(),
      shards.end(),
      [](const model::broker_shard& l, const model::broker_shard& r) {
          return l.node_id > r.node_id;
      });
    auto node_id = std::cbegin(shards)->node_id;
    auto it = find_node(node_id);
    vassert(
      it != _machines.end(),
      "node: {} must exists in partition allocator, it currenlty does not",
      node_id);
    for (auto const& bs : shards) {
        // Thanks to shards being sorted we need to do only
        //  as many lookups as there are brokers
        if (it->first != bs.node_id) {
            it = find_node(bs.node_id);
        }
        if (it != _machines.end()) {
            it->second->allocate(bs.shard);
        }
    }
}

partition_allocator::iterator
partition_allocator::find_node(model::node_id id) {
    return _machines.find(id);
}

void partition_allocator::test_only_saturate_all_machines() {
    for (auto& [id, m] : _machines) {
        m->_partition_capacity = 0;
        for (auto& w : m->_weights) {
            w = allocation_node::max_allocations_per_core;
        }
    }
    _available_machines.clear();
}

uint32_t
partition_allocator::test_only_max_cluster_allocation_partition_capacity()
  const {
    return std::accumulate(
      _available_machines.begin(),
      _available_machines.end(),
      uint32_t(0),
      [](uint32_t acc, const value_type& n) {
          return acc + n.partition_capacity();
      });
}

std::ostream& operator<<(std::ostream& o, const allocation_node& n) {
    o << "{ node:" << n._id << ", max_partitions_per_core: "
      << allocation_node::max_allocations_per_core
      << ", partition_capacity:" << n._partition_capacity << ", weights: [";
    for (auto w : n._weights) {
        o << "(" << w << ")";
    }
    return o << "]}";
}

} // namespace cluster
