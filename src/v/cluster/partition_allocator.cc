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
#include "cluster/types.h"
#include "model/fundamental.h"
#include "raft/types.h"

#include <absl/container/node_hash_set.h>

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

uint32_t allocation_node::allocate() {
    auto it = std::min_element(_weights.begin(), _weights.end());
    (*it)++; // increment the weights
    _allocated_partitions++;
    return std::distance(_weights.begin(), it);
}
void allocation_node::deallocate(uint32_t core) {
    vassert(
      core < _weights.size(),
      "Tried to deallocate a non-existing core:{} - {}",
      core,
      *this);
    vassert(
      _allocated_partitions > 0 && _weights[core] > 0,
      "unable to deallocate partition from core {} at node {}",
      core,
      *this);

    _allocated_partitions--;
    _weights[core]--;
}
void allocation_node::allocate(uint32_t core) {
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

allocation_configuration::allocation_configuration(int16_t rf)
  : allocation_configuration(rf, {}) {}

allocation_configuration::allocation_configuration(
  int16_t rf, std::vector<model::broker_shard> current)
  : replication_factor(rf)
  , current_allocations(std::move(current)) {}

void allocation_state::rollback(const std::vector<partition_assignment>& v) {
    for (auto& as : v) {
        rollback(as.replicas);
        // rollback for each assignment as the groups are distinct
        _highest_group = raft::group_id(_highest_group() - 1);
    }
}

void allocation_state::rollback(const std::vector<model::broker_shard>& v) {
    for (auto& bs : v) {
        deallocate(bs);
    }
}

int16_t allocation_state::available_nodes() const { return _nodes.size(); }

raft::group_id allocation_state::next_group_id() { return ++_highest_group; }

void allocation_state::apply_update(
  std::vector<model::broker_shard> replicas, raft::group_id group_id) {
    if (replicas.empty()) {
        return;
    }
    _highest_group = std::max(_highest_group, group_id);
    // We can use non stable sort algorithm as we do not need to preserver
    // the order of shards
    std::sort(
      replicas.begin(),
      replicas.end(),
      [](const model::broker_shard& l, const model::broker_shard& r) {
          return l.node_id > r.node_id;
      });
    auto node_id = std::cbegin(replicas)->node_id;
    auto it = _nodes.find(node_id);

    for (auto const& bs : replicas) {
        if (it == _nodes.end()) {
            // do nothing, node was deleted
            continue;
        }
        // Thanks to shards being sorted we need to do only
        //  as many lookups as there are brokers
        if (it->first != bs.node_id) {
            it = _nodes.find(bs.node_id);
        }
        if (it != _nodes.end()) {
            it->second->allocate(bs.shard);
        }
    }
}

void allocation_state::register_node(allocation_state::node_ptr n) {
    _nodes.emplace(n->_id, std::move(n));
}

void allocation_state::unregister_node(model::node_id id) { _nodes.erase(id); }

bool allocation_state::is_empty(model::node_id id) const {
    auto it = _nodes.find(id);
    if (it == _nodes.end()) {
        throw std::invalid_argument(
          fmt::format("machine with id {} not found", id));
    }
    return it->second->empty();
}

void allocation_state::deallocate(const model::broker_shard& replica) {
    if (auto it = _nodes.find(replica.node_id); it != _nodes.end()) {
        it->second->deallocate(replica.shard);
    }
}

result<uint32_t> allocation_state::allocate(model::node_id id) {
    if (auto it = _nodes.find(id); it != _nodes.end()) {
        if (it->second->is_full()) {
            return errc::invalid_node_opeartion;
        }
        return it->second->allocate();
    }

    return errc::node_does_not_exists;
}

partition_allocator::partition_allocator(
  allocation_strategy allocation_strategy)
  : _state(std::make_unique<allocation_state>())
  , _allocation_strategy(std::move(allocation_strategy)) {}

result<allocation_units>
partition_allocator::allocate(const topic_allocation_configuration& cfg) {
    vlog(clusterlog.trace, "allocation request for: {}", cfg);
    if (
      cfg.replication_factor <= 0
      || _state->available_nodes() < cfg.replication_factor) {
        return errc::topic_invalid_replication_factor;
    }
    if (unlikely(cfg.partition_count <= 0)) {
        return errc::topic_invalid_partitions;
    }
    std::vector<partition_assignment> assignments;
    assignments.reserve(cfg.partition_count);
    absl::node_hash_set<model::partition_id> custom_allocated;

    for (auto& custom_allocation : cfg.custom_allocations) {
        auto replicas = _allocation_strategy.allocate_partition(
          custom_allocation, *_state);

        if (!replicas) {
            _state->rollback(assignments);
            return replicas.error();
        }
        assignments.push_back(partition_assignment{
          .group = _state->next_group_id(),
          .id = custom_allocation.partition_id,
          .replicas = std::move(replicas.value()),
        });
        custom_allocated.emplace(custom_allocation.partition_id);
    }

    for (auto p = 0; p < cfg.partition_count; ++p) {
        model::partition_id pid(p);
        // already allocated, do nothing
        if (custom_allocated.contains(pid)) {
            continue;
        }
        auto replicas = _allocation_strategy.allocate_partition(
          allocation_configuration(cfg.replication_factor), *_state);

        if (!replicas) {
            _state->rollback(assignments);
            return replicas.error();
        }
        assignments.push_back(partition_assignment{
          .group = _state->next_group_id(),
          .id = pid,
          .replicas = std::move(replicas.value()),
        });
    }
    return allocation_units(std::move(assignments), _state.get());
}

void partition_allocator::deallocate(
  const std::vector<model::broker_shard>& replicas) {
    for (auto& r : replicas) {
        // find in brokers
        _state->deallocate(r);
    }
}

void partition_allocator::update_allocation_state(
  const std::vector<model::broker_shard>& shards, raft::group_id gid) {
    if (shards.empty()) {
        return;
    }

    _state->apply_update(shards, gid);
}

void partition_allocator::update_allocation_state(
  const std::vector<model::broker_shard>& current,
  const std::vector<model::broker_shard>& previous) {
    std::vector<model::broker_shard> to_add;
    std::vector<model::broker_shard> to_remove;

    std::copy_if(
      current.begin(),
      current.end(),
      std::back_inserter(to_add),
      [&previous](const model::broker_shard& current_bs) {
          auto it = std::find(previous.begin(), previous.end(), current_bs);
          return it == previous.end();
      });

    std::copy_if(
      previous.begin(),
      previous.end(),
      std::back_inserter(to_remove),
      [&current](const model::broker_shard& prev_bs) {
          auto it = std::find(current.begin(), current.end(), prev_bs);

          return it == current.end();
      });

    _state->apply_update(to_add, raft::group_id{});
    for (const auto& bs : to_remove) {
        _state->deallocate(bs);
    }
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

std::ostream&
operator<<(std::ostream& o, const custom_allocation_configuration& a) {
    fmt::print(
      o,
      "{{partition_id: {}, custom_allocations: {}}}",
      a.partition_id,
      a.nodes);
    return o;
}

std::ostream&
operator<<(std::ostream& o, const topic_allocation_configuration& cfg) {
    fmt::print(
      o,
      "{{partition_count: {}, replication_factor: {}, custom_allocations: "
      "{}}}",
      cfg.partition_count,
      cfg.replication_factor,
      cfg.custom_allocations);
    return o;
}

std::ostream& operator<<(std::ostream& o, const allocation_configuration& a) {
    fmt::print(
      o,
      "{{replication_factor: {}, current_allocations: "
      "{} }}",
      a.replication_factor,
      a.current_allocations);
    return o;
}

} // namespace cluster
