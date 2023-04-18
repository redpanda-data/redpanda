// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/scheduling/allocation_state.h"

#include "bytes/oncore.h"
#include "cluster/logger.h"
#include "ssx/sformat.h"

namespace cluster {

void allocation_state::rollback(
  const std::vector<partition_assignment>& v,
  const partition_allocation_domain domain) {
    verify_shard();
    for (auto& as : v) {
        rollback(as.replicas, domain);
        // rollback for each assignment as the groups are distinct
        _highest_group = raft::group_id(_highest_group() - 1);
    }
}

void allocation_state::rollback(
  const std::vector<model::broker_shard>& v,
  const partition_allocation_domain domain) {
    verify_shard();
    for (auto& bs : v) {
        deallocate(bs, domain);
    }
}

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

void allocation_state::apply_update(
  std::vector<model::broker_shard> replicas,
  raft::group_id group_id,
  const partition_allocation_domain domain) {
    verify_shard();
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
            it->second->allocate_on(bs.shard, domain);
        }
    }
}

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
      _partitions_reserve_shard0);

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
                _partitions_reserve_shard0));
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
            _partitions_reserve_shard0));
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

void allocation_state::deallocate(
  const model::broker_shard& replica,
  const partition_allocation_domain domain) {
    verify_shard();
    if (auto it = _nodes.find(replica.node_id); it != _nodes.end()) {
        it->second->deallocate_on(replica.shard, domain);
    }
}

result<uint32_t> allocation_state::allocate(
  model::node_id id, const partition_allocation_domain domain) {
    verify_shard();
    if (auto it = _nodes.find(id); it != _nodes.end()) {
        if (it->second->is_full()) {
            return errc::invalid_node_operation;
        }
        return it->second->allocate(domain);
    }

    return errc::node_does_not_exists;
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
