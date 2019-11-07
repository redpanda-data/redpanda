#include "cluster/partition_allocator.h"

#include <boost/container_hash/hash.hpp>
#include <roaring/roaring.hh>

#include <algorithm>
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
  model::ntp ntp, int16_t replication_factor) {
    std::vector<model::broker_shard> replicas;
    replicas.reserve(replication_factor);

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
        model::broker_shard bs{.node_id = machine.id(), .shard = cpu};
        replicas.push_back(std::move(bs));
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
std::optional<std::vector<partition_assignment>>
partition_allocator::allocate(const topic_configuration& cfg) {
    if (_available_machines.empty()) {
        return std::nullopt;
    }
    std::vector<partition_assignment> ret;
    ret.reserve(cfg.partition_count);
    for (int32_t i = 0; i < cfg.partition_count; ++i) {
        model::ntp ntp = model::ntp{
          cfg.ns, model::topic_partition{cfg.topic, model::partition_id(i)}};
        // all replicas must belong to the same raft group
        raft::group_id partition_group = raft::group_id(_highest_group() + 1);
        auto replicas_assignment = allocate_replicas(
          std::move(ntp), cfg.replication_factor);
        if (replicas_assignment == std::nullopt) {
            rollback(ret);
            return std::nullopt;
        }

        partition_assignment p_as{.group = partition_group,
                                  .ntp = std::move(ntp),
                                  .replicas = std::move(*replicas_assignment)};
        ret.push_back(std::move(p_as));
        _highest_group = partition_group;
    }
    return ret;
}

void partition_allocator::deallocate(const model::broker_shard& bs) {
    // find in brokers
    auto it = std::find_if(
      _machines.begin(), _machines.end(), [id = bs.node_id](const ptr& n) {
          return id == n->node().id();
      });
    if (it != _machines.end()) {
        auto& machine = *it;
        if (bs.shard < machine->cpus()) {
            machine->deallocate(bs.shard);
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
