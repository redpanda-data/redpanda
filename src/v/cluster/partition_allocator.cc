#include "cluster/partition_allocator.h"

#include "model/metadata.h"

#include <boost/container_hash/hash.hpp>
#include <roaring/roaring.hh>

#include <algorithm>
#include <random>

namespace cluster {

void partition_allocator::rollback(const std::vector<partition_assignment>& v) {
    // decrease only distinct raft groups
    Roaring bm;
    for (auto& as : v) {
        bm.add(as.group());
        deallocate(as);
    }
    _highest_group = raft::group_id(_highest_group() - bm.cardinality());
}
static inline bool valid_machine_fault_domain_diversity(
  const std::vector<partition_assignment>& replicas) {
    model::node_id sentinel = replicas.begin()->broker.id();
    const uint32_t unique = std::accumulate(
      replicas.begin(),
      replicas.end(),
      uint32_t(1),
      [&sentinel](uint32_t acc, const partition_assignment& as) {
          if (sentinel != as.broker.id()) {
              acc += 1;
          }
          return acc;
      });
    return unique == replicas.size();
}
static bool is_machine_in_replicas(
  const allocation_node& machine,
  const std::vector<partition_assignment>& replicas) {
    return std::any_of(
      replicas.begin(),
      replicas.end(),
      [&machine](const partition_assignment& as) {
          return machine.id() == as.broker.id();
      });
}

std::optional<std::vector<partition_assignment>>
partition_allocator::allocate_replicas(
  model::ntp ntp, int16_t replication_factor) {
    std::vector<partition_assignment> replicas;
    replicas.reserve(replication_factor);

    // all replicas must belong to the same raft group
    raft::group_id replicas_group = raft::group_id(_highest_group() + 1);
    while (replicas.size() < replication_factor) {
        const int16_t replicas_left = replication_factor - replicas.size();
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
        partition_assignment as{.shard = cpu,
                                .group = replicas_group,
                                .ntp = ntp,
                                .broker = machine.node(),
                                .replica = model::replica_id(
                                  static_cast<model::replica_id::type>(replicas.size()))};
        replicas.push_back(std::move(as));
        if (machine.is_full()) {
            _available_machines.erase(_available_machines.iterator_to(machine));
        }
    }
    if (!valid_machine_fault_domain_diversity(replicas)) {
        rollback(replicas);
        return std::nullopt;
    }
    _highest_group = replicas_group;
    return replicas;
}

// FIXME: take into account broker.rack diversity & other constraints
std::optional<std::vector<partition_assignment>>
partition_allocator::allocate(const topic_configuration& cfg) {
    if (_available_machines.empty()) {
        return std::nullopt;
    }
    std::vector<partition_assignment> ret;
    ret.reserve(cfg.partition_count * cfg.replication_factor);
    for (int32_t i = 0; i < cfg.partition_count; ++i) {
        model::ntp ntp = model::ntp{
          cfg.ns, model::topic_partition{cfg.topic, model::partition_id(i)}};
        auto opt = allocate_replicas(std::move(ntp), cfg.replication_factor);
        if (opt == std::nullopt) {
            rollback(ret);
            return std::nullopt;
        }
        std::move(
          opt.value().begin(), opt.value().end(), std::back_inserter(ret));
    }
    return ret;
}

void partition_allocator::deallocate(const partition_assignment& as) {
    // find in brokers
    auto it = std::find_if(
      _machines.begin(), _machines.end(), [id = as.broker.id()](const ptr& n) {
          return id == n->node().id();
      });
    if (it != _machines.end()) {
        auto& machine = *it;
        if (as.shard < machine->cpus()) {
            machine->deallocate(as.shard);
            if (!machine->_hook.is_linked()) {
                _available_machines.push_back(*machine);
            }
        }
    }
}

void partition_allocator::test_only_saturate_all_machines() {
    for (auto& m : _machines) {
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
} // namespace cluster
