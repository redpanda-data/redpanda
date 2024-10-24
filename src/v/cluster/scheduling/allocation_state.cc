// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/scheduling/allocation_state.h"

#include "base/oncore.h"
#include "cluster/logger.h"
#include "features/feature_table.h"
#include "ssx/sformat.h"

namespace cluster {

int16_t allocation_state::available_nodes() const {
    verify_shard();
    return std::count_if(
      _nodes.begin(), _nodes.end(), [](const underlying_t::value_type& p) {
          return p.second->is_active();
      });
}

bool allocation_state::validate_shard(
  model::node_id node, uint32_t shard) const {
    if (auto node_i = _nodes.find(node); node_i != _nodes.end()) {
        return shard < node_i->second->cpus();
    } else {
        return false;
    }
}

raft::group_id allocation_state::next_group_id() { return ++_highest_group; }

void allocation_state::register_node(allocation_state::node_ptr n) {
    const auto id = n->_id;
    _nodes.emplace(id, std::move(n));
}

void allocation_state::register_node(
  const model::broker& broker, allocation_node::state state) {
    auto node = std::make_unique<allocation_node>(
      broker.id(),
      broker.properties().cores,
      _partitions_per_shard,
      _partitions_reserve_shard0,
      _internal_kafka_topics);

    if (state == allocation_node::state::decommissioned) {
        node->decommission();
    }

    auto inserted = _nodes.emplace(broker.id(), std::move(node)).second;
    vassert(inserted, "node {}: double registration", broker.id());
}

void allocation_state::update_allocation_nodes(
  const std::vector<model::broker>& brokers) {
    verify_shard();
    // deletions
    for (auto& [id, node] : _nodes) {
        auto it = std::find_if(
          brokers.begin(), brokers.end(), [id = id](const model::broker& b) {
              return b.id() == id;
          });
        if (it == brokers.end()) {
            node->mark_as_removed();
        }
    }

    // updates & additions
    for (auto& b : brokers) {
        auto it = _nodes.find(b.id());
        if (it == _nodes.end()) {
            _nodes.emplace(
              b.id(),
              std::make_unique<allocation_node>(
                b.id(),
                b.properties().cores,
                _partitions_per_shard,
                _partitions_reserve_shard0,
                _internal_kafka_topics));
        } else {
            it->second->update_core_count(b.properties().cores);
            // node was added back to the cluster
            if (it->second->is_removed()) {
                it->second->mark_as_active();
            }
        }
    }
}

void allocation_state::upsert_allocation_node(const model::broker& broker) {
    auto it = _nodes.find(broker.id());

    if (it == _nodes.end()) {
        _nodes.emplace(
          broker.id(),
          std::make_unique<allocation_node>(
            broker.id(),
            broker.properties().cores,
            _partitions_per_shard,
            _partitions_reserve_shard0,
            _internal_kafka_topics));
    } else {
        it->second->update_core_count(broker.properties().cores);
        // node was added back to the cluster
        if (it->second->is_removed()) {
            it->second->mark_as_active();
        }
    }
}

void allocation_state::remove_allocation_node(model::node_id id) {
    auto it = std::find_if(
      _nodes.begin(), _nodes.end(), [id](const auto& node) {
          return node.first == id;
      });

    it->second->mark_as_removed();
}

void allocation_state::decommission_node(model::node_id id) {
    verify_shard();
    auto it = _nodes.find(id);
    if (it == _nodes.end()) {
        throw std::invalid_argument(
          fmt::format("machine with id {} not found", id));
    }
    // mark as decommissioned
    it->second->decommission();
}

void allocation_state::recommission_node(model::node_id id) {
    verify_shard();
    auto it = _nodes.find(id);
    if (it == _nodes.end()) {
        throw std::invalid_argument(
          fmt::format("machine with id {} not found", id));
    }
    // mark as recommissioned
    it->second->recommission();
}

bool allocation_state::is_empty(model::node_id id) const {
    auto it = _nodes.find(id);
    if (it == _nodes.end()) {
        throw std::invalid_argument(
          fmt::format("machine with id {} not found", id));
    }
    return it->second->empty();
}

bool allocation_state::node_local_core_assignment_enabled() const {
    return _feature_table.is_active(
      features::feature::node_local_core_assignment);
}

void allocation_state::add_allocation(const model::broker_shard& replica) {
    verify_shard();
    if (auto it = _nodes.find(replica.node_id); it != _nodes.end()) {
        it->second->add_allocation();
        if (!node_local_core_assignment_enabled()) {
            it->second->add_allocation(replica.shard);
        }
        vlog(
          clusterlog.trace,
          "add allocation [node: {}, core: {}], total allocated: {}",
          replica.node_id,
          replica.shard,
          it->second->_allocated_partitions);
    }
}

void allocation_state::remove_allocation(const model::broker_shard& replica) {
    verify_shard();
    if (auto it = _nodes.find(replica.node_id); it != _nodes.end()) {
        it->second->remove_allocation();
        if (!node_local_core_assignment_enabled()) {
            it->second->remove_allocation(replica.shard);
        }
        vlog(
          clusterlog.trace,
          "remove allocation [node: {}, core: {}], total allocated: {}",
          replica.node_id,
          replica.shard,
          it->second->_allocated_partitions);
    }
}

uint32_t allocation_state::allocate(model::node_id id) {
    verify_shard();
    auto it = _nodes.find(id);
    vassert(
      it != _nodes.end(), "allocated node with id {} have to be present", id);

    it->second->add_allocation();
    it->second->add_final_count();
    if (node_local_core_assignment_enabled()) {
#ifndef NDEBUG
        // return invalid shard in debug mode so that we can spot places where
        // we are still using it.
        return 12312312;
#else
        return 0;
#endif
    } else {
        return it->second->allocate_shard();
    }
}

void allocation_state::add_final_count(const model::broker_shard& replica) {
    verify_shard();
    if (auto it = _nodes.find(replica.node_id); it != _nodes.end()) {
        it->second->add_final_count();
    }
}

void allocation_state::remove_final_count(const model::broker_shard& replica) {
    verify_shard();
    if (auto it = _nodes.find(replica.node_id); it != _nodes.end()) {
        it->second->remove_final_count();
    }
}

void allocation_state::verify_shard() const {
    /* This is a consistency check on the use of the allocation state:
     * it checks that the caller is on the same shard the state was originally
     * created on. It is easy to inadvertently violate this condition since
     * the cluster::allocation_units class embeds a pointer to this state and
     * when moved across shards (e.g., because allocation always happens on
     * shard 0 but some other shard initiates the request) it may result in
     * calls on the current shard being routed back to the original shard
     * through this pointer, a thread-safety violation.
     */
    oncore_debug_verify(_verify_shard);
}

} // namespace cluster
