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

#include "cluster/scheduling/types.h"

#include "cluster/logger.h"
#include "cluster/scheduling/allocation_state.h"
#include "utils/to_string.h"

#include <fmt/ostream.h>

namespace cluster {

hard_constraint_evaluator
hard_constraint::make_evaluator(const replicas_t& current_replicas) const {
    auto ev = _impl->make_evaluator(current_replicas);
    return [this, ev = std::move(ev)](const allocation_node& node) {
        auto res = ev(node);
        vlog(clusterlog.trace, "{}({}) = {}", name(), node.id(), res);
        return res;
    };
}

soft_constraint_evaluator
soft_constraint::make_evaluator(const replicas_t& current_replicas) const {
    auto ev = _impl->make_evaluator(current_replicas);
    return [this, ev = std::move(ev)](const allocation_node& node) {
        auto res = ev(node);
        vlog(
          clusterlog.trace,
          "{}(node: {}) = {} ({})",
          name(),
          node.id(),
          res,
          (double)res / soft_constraint::max_score);
        return res;
    };
};

std::ostream& operator<<(std::ostream& o, const allocation_constraints& a) {
    fmt::print(
      o,
      "{{soft_constraints: {}, hard_constraints: {}}}",
      a.soft_constraints,
      a.hard_constraints);
    return o;
}

void allocation_constraints::add(allocation_constraints other) {
    std::move(
      other.hard_constraints.begin(),
      other.hard_constraints.end(),
      std::back_inserter(hard_constraints));

    std::move(
      other.soft_constraints.begin(),
      other.soft_constraints.end(),
      std::back_inserter(soft_constraints));
}

allocation_units::allocation_units(
  ss::chunked_fifo<partition_assignment> assignments,
  allocation_state& state,
  const partition_allocation_domain domain)
  : _assignments(std::move(assignments))
  , _state(state.weak_from_this())
  , _domain(domain) {}

allocation_units::~allocation_units() {
    oncore_debug_verify(_oncore);
    for (auto& pas : _assignments) {
        for (auto& replica : pas.replicas) {
            _state->remove_allocation(replica, _domain);
        }
    }
}

allocated_partition::allocated_partition(
  std::vector<model::broker_shard> replicas,
  partition_allocation_domain domain,
  allocation_state& state)
  : _replicas(std::move(replicas))
  , _domain(domain)
  , _state(state.weak_from_this()) {}

std::optional<allocated_partition::previous_replica>
allocated_partition::prepare_move(model::node_id prev_node) {
    previous_replica prev;
    auto it = std::find_if(
      _replicas.begin(), _replicas.end(), [prev_node](const auto& bs) {
          return bs.node_id == prev_node;
      });
    if (it == _replicas.end()) {
        return std::nullopt;
    }
    prev.bs = *it;
    prev.idx = it - _replicas.begin();
    if (!_original_node2shard) {
        _original_node2shard.emplace();
        for (const auto& bs : _replicas) {
            _original_node2shard->emplace(bs.node_id, bs.shard);
        }
    } else if (!_original_node2shard->contains(prev_node)) {
        // if replica prev_node is not original, pick an arbitrary original one
        // that is not present in the current set as the "source" for prev.bs
        // (they are all interchangeable).
        //
        // Example (omitting shard ids for clarity):
        // _original = [0, 1, 2], _replicas = [2, 3, 4], and we want to move
        // replica 4 (so prev_node = 4). Because replica 4 is not in _original
        // (i.e. it was reallocated earlier using the same allocated_partition
        // object), we need to arbitrarily designate one of nodes 0 and 1 as the
        // "original" for replica 4 (meaning that the replica on node 4 was
        // originally on node 0 or 1).
        for (const auto& [node_id, shard] : *_original_node2shard) {
            model::broker_shard bs{.node_id = node_id, .shard = shard};
            if (
              std::find(_replicas.begin(), _replicas.end(), bs)
              == _replicas.end()) {
                prev.original = bs;
                break;
            }
        }
    }

    // remove previous replica and its allocation contribution so that it
    // doesn't interfere with allocation of the new replica.
    std::swap(_replicas[prev.idx], _replicas.back());
    _replicas.pop_back();
    _state->remove_allocation(prev.bs, _domain);
    if (prev.original) {
        _state->remove_allocation(*prev.original, _domain);
    }
    return prev;
}

model::broker_shard allocated_partition::add_replica(
  model::node_id node, const std::optional<previous_replica>& prev) {
    if (!_original_node2shard) {
        _original_node2shard.emplace();
        for (const auto& bs : _replicas) {
            _original_node2shard->emplace(bs.node_id, bs.shard);
        }
    }

    if (prev) {
        model::broker_shard original = prev->original.value_or(prev->bs);
        // previous replica will still be present while partition move is in
        // progress, so add its contribution (that we removed when preparing the
        // move) back.
        _state->add_allocation(original, _domain);
    }

    model::broker_shard replica{.node_id = node};
    if (auto it = _original_node2shard->find(node);
        it != _original_node2shard->end()) {
        // this is an original replica, preserve the shard
        replica.shard = it->second;
    } else {
        // the replica is new, choose the shard and add allocation
        replica.shard = _state->allocate(node, _domain);
    }

    _replicas.push_back(replica);
    return replica;
}

void allocated_partition::cancel_move(const previous_replica& prev) {
    // return substituted replica to its place
    _replicas.push_back(prev.bs);
    std::swap(_replicas[prev.idx], _replicas.back());
    _state->add_allocation(prev.bs, _domain);
    if (prev.original) {
        _state->add_allocation(*prev.original, _domain);
    }
}

std::vector<model::broker_shard> allocated_partition::release_new_partition() {
    vassert(
      _original_node2shard && _original_node2shard->empty(),
      "new partition shouldn't have previous replicas");
    _state = nullptr;
    return std::move(_replicas);
}

bool allocated_partition::has_changes() const {
    if (!_original_node2shard) {
        return false;
    }
    if (_replicas.size() != _original_node2shard->size()) {
        return true;
    }
    for (const auto& bs : _replicas) {
        if (!_original_node2shard->contains(bs.node_id)) {
            return true;
        }
    }
    return false;
}

bool allocated_partition::is_original(model::node_id node) const {
    if (_original_node2shard) {
        return _original_node2shard->contains(node);
    }
    return std::find_if(
             _replicas.begin(),
             _replicas.end(),
             [node](const auto& bs) { return bs.node_id == node; })
           != _replicas.end();
}

allocated_partition::~allocated_partition() {
    oncore_debug_verify(_oncore);

    if (!_original_node2shard || !_state) {
        // no new allocations took place or object was moved from
        return;
    }

    for (const auto& bs : _replicas) {
        if (!_original_node2shard->contains(bs.node_id)) {
            _state->remove_allocation(bs, _domain);
        }
    }
}

partition_constraints::partition_constraints(
  model::partition_id id, uint16_t replication_factor)
  : partition_constraints(id, replication_factor, allocation_constraints{}) {}

partition_constraints::partition_constraints(
  model::partition_id id,
  uint16_t replication_factor,
  allocation_constraints constraints)
  : partition_id(id)
  , replication_factor(replication_factor)
  , constraints(std::move(constraints)) {}

std::ostream& operator<<(std::ostream& o, const partition_constraints& pc) {
    fmt::print(
      o,
      "{{partition_id: {}, replication_factor: {}, constrains: {}}}",
      pc.partition_id,
      pc.replication_factor,
      pc.constraints);
    return o;
}
std::ostream& operator<<(std::ostream& o, const allocation_request& req) {
    fmt::print(
      o, "{{partion_constraints: {}, domain: {}}}", req.partitions, req.domain);
    return o;
}
} // namespace cluster
