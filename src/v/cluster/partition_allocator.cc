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
#include "model/metadata.h"
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
partition_allocator::allocate_replicas(
  int16_t replication_factor, const std::vector<model::broker_shard>& current) {
    std::vector<model::broker_shard> new_replicas;
    new_replicas.reserve(replication_factor);
    for (size_t i = 0; i < _machines.size(); ++i) {
        const uint16_t replicas_left = replication_factor
                                       - (new_replicas.size() + current.size());
        if (replicas_left == 0) {
            break;
        }

        if (_available_machines.size() < replicas_left) {
            rollback(new_replicas);
            return std::nullopt;
        }
        auto& rr = round_robin_ptr();
        auto& machine = *rr;
        rr++;
        if (machine.is_decommissioned()) {
            continue;
        }
        if (
          is_machine_in_replicas(machine, new_replicas)
          || is_machine_in_replicas(machine, current)) {
            continue;
        }
        const uint32_t cpu = machine.allocate();
        model::broker_shard bs{.node_id = machine.id(), .shard = cpu};
        new_replicas.push_back(bs);
        if (machine.is_full()) {
            _available_machines.erase(_available_machines.iterator_to(machine));
        }
    }

    std::copy(
      current.cbegin(), current.cend(), std::back_inserter(new_replicas));

    if (
      new_replicas.size() < (size_t)replication_factor
      || !valid_machine_fault_domain_diversity(new_replicas)) {
        rollback(new_replicas);
        return std::nullopt;
    }
    return new_replicas;
}

std::optional<partition_allocator::allocation_units>
partition_allocator::reassign_decommissioned_replicas(
  const partition_assignment& pas) {
    int16_t rf = pas.replicas.size();
    auto current_replicas = pas.replicas;
    /**
     * In the first step we need to decide which replicas have to be moved, we
     * will move replicas from decommissioned nodes to other available nodes
     */
    auto it = std::stable_partition(
      current_replicas.begin(),
      current_replicas.end(),
      [this](model::broker_shard& bs) {
          if (auto it = _machines.find(bs.node_id); it != _machines.end()) {
              return !it->second->is_decommissioned();
          }
          // if machine is no longer in a list of allocation nodes, just
          // reallocate replica to another node
          return false;
      });

    // remove all replicas that doesn't have to be moved
    current_replicas.erase(it, current_replicas.end());

    // try to allocate replicas that have to be moved
    auto new_replicas = allocate_replicas(rf, current_replicas);

    if (new_replicas) {
        auto res = pas;
        res.replicas = std::move(*new_replicas);
        return allocation_units({res}, this);
    }
    // there are not enough nodes to fullfil replicas assignment return nullopt
    return std::nullopt;
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
        auto replicas_assignment = allocate_replicas(
          cfg.replication_factor, {});
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
            if (!machine->_hook.is_linked() && !machine->is_decommissioned()) {
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

    for (auto const& bs : shards) {
        if (it == _machines.end()) {
            // do nothing, node was deleted
            continue;
        }
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

void partition_allocator::unregister_node(model::node_id id) {
    auto it = std::find_if(
      _available_machines.begin(),
      _available_machines.end(),
      [id](allocation_node& n) { return n._id == id; });

    if (it != _available_machines.end()) {
        _available_machines.erase(it);
    }
    _machines.erase(id);
}

void partition_allocator::decommission_node(model::node_id id) {
    auto it = _machines.find(id);
    if (it == _machines.end()) {
        throw std::invalid_argument(
          fmt::format("machine with id {} not found", id));
    }
    // mark as decommissioned
    it->second->decommission();

    // remove from available machines list
    if (it->second->_hook.is_linked()) {
        _available_machines.erase(cil_t::s_iterator_to(*it->second));
    }
}

void partition_allocator::recommission_node(model::node_id id) {
    auto it = _machines.find(id);
    if (it == _machines.end()) {
        throw std::invalid_argument(
          fmt::format("machine with id {} not found", id));
    }
    // mark as decommissioned
    it->second->recommission();

    // add to available machines list
    _available_machines.push_back(*it->second);
}

bool partition_allocator::is_empty(model::node_id id) {
    auto it = _machines.find(id);
    if (it == _machines.end()) {
        throw std::invalid_argument(
          fmt::format("machine with id {} not found", id));
    }
    return it->second->empty();
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
