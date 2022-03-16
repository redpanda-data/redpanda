// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/scheduling/allocation_state.h"

#include "cluster/logger.h"

namespace cluster {

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

int16_t allocation_state::available_nodes() const {
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
    const auto id = n->_id;
    _nodes.emplace(id, std::move(n));
}

void allocation_state::update_allocation_nodes(
  const std::vector<model::broker>& brokers) {
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
                absl::node_hash_map<ss::sstring, ss::sstring>{},
                b.rack()));
        } else {
            it->second->update_core_count(b.properties().cores);
            // node was added back to the cluster
            if (it->second->is_removed()) {
                it->second->mark_as_active();
            }
        }
    }
}

void allocation_state::decommission_node(model::node_id id) {
    auto it = _nodes.find(id);
    if (it == _nodes.end()) {
        throw std::invalid_argument(
          fmt::format("machine with id {} not found", id));
    }
    // mark as decommissioned
    it->second->decommission();
}

void allocation_state::recommission_node(model::node_id id) {
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

void allocation_state::deallocate(const model::broker_shard& replica) {
    if (auto it = _nodes.find(replica.node_id); it != _nodes.end()) {
        it->second->deallocate(replica.shard);
    }
}

result<uint32_t> allocation_state::allocate(model::node_id id) {
    if (auto it = _nodes.find(id); it != _nodes.end()) {
        if (it->second->is_full()) {
            return errc::invalid_node_operation;
        }
        return it->second->allocate();
    }

    return errc::node_does_not_exists;
}

} // namespace cluster
