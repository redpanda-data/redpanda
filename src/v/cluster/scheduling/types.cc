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
#include "utils/exceptions.h"
#include "utils/to_string.h"

#include <fmt/ostream.h>

namespace cluster {

std::ostream& operator<<(std::ostream& o, const allocation_constraints& a) {
    fmt::print(
      o,
      "{{hard_constraints: {}, soft_constraints: {}}}",
      a.hard_constraints,
      a.soft_constraints);
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

allocation_units::allocation_units(allocation_state& state)
  : _state(state.weak_from_this()) {}

allocation_units::~allocation_units() {
    oncore_debug_verify(_oncore);
    if (unlikely(!_state)) {
        return;
    }
    for (const auto& replica : _added_replicas) {
        _state->remove_allocation(replica);
        _state->remove_final_count(replica);
    }
}

allocated_partition::allocated_partition(
  model::ntp ntp,
  std::vector<model::broker_shard> replicas,
  allocation_state& state)
  : _ntp(std::move(ntp))
  , _replicas(std::move(replicas))
  , _state(state.weak_from_this()) {}

std::optional<allocated_partition::previous_replica>
allocated_partition::prepare_move(model::node_id prev_node) const {
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
    return prev;
}

model::broker_shard allocated_partition::add_replica(
  model::node_id node, const std::optional<previous_replica>& prev) {
    if (unlikely(!_state)) {
        throw concurrent_modification_error(
          "allocation_state was concurrently replaced");
    }

    if (!_original_node2shard) {
        _original_node2shard.emplace();
        for (const auto& bs : _replicas) {
            _original_node2shard->emplace(bs.node_id, bs.shard);
        }
    }

    if (prev) {
        if (!_original_node2shard->contains(prev->bs.node_id)) {
            _state->remove_allocation(prev->bs);
        }
        _state->remove_final_count(prev->bs);
    }

    model::broker_shard replica{.node_id = node};
    if (auto it = _original_node2shard->find(node);
        it != _original_node2shard->end()) {
        // this is an original replica, preserve the shard
        replica.shard = it->second;
        _state->add_final_count(replica);
    } else {
        // the replica is new, choose the shard and add allocation
        replica.shard = _state->allocate(node);
    }

    if (prev) {
        std::swap(_replicas[prev->idx], _replicas.back());
        _replicas.back() = replica;
    } else {
        _replicas.push_back(replica);
    }
    return replica;
}

replicas_t allocated_partition::release_new_partition(
  chunked_vector<model::broker_shard>& added_replicas) {
    for (const auto& bs : _replicas) {
        if (
          !_original_node2shard
          || !_original_node2shard->contains(bs.node_id)) {
            added_replicas.push_back(bs);
        }
    }
    _original_node2shard.reset();
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

errc allocated_partition::try_revert(const reallocation_step& step) {
    if (unlikely(!_state)) {
        throw concurrent_modification_error(
          "allocation_state was concurrently replaced");
    }

    if (!_original_node2shard) {
        return errc::no_update_in_progress;
    }

    auto it = std::find(_replicas.begin(), _replicas.end(), step.current());
    if (it == _replicas.end()) {
        return errc::node_does_not_exists;
    }

    if (step.previous()) {
        auto prev_it = std::find(
          _replicas.begin(), _replicas.end(), *step.previous());
        if (prev_it != _replicas.end()) {
            return errc::invalid_request;
        }
        *it = *step.previous();
    } else {
        std::swap(*it, _replicas.back());
        _replicas.pop_back();
    }

    _state->remove_final_count(step.current());
    if (!_original_node2shard->contains(step.current().node_id)) {
        _state->remove_allocation(step.current());
    }

    if (step.previous()) {
        _state->add_final_count(*step.previous());
        if (!_original_node2shard->contains(step.previous()->node_id)) {
            _state->add_allocation(*step.previous());
        }
    }

    return errc::success;
}

allocated_partition::~allocated_partition() {
    oncore_debug_verify(_oncore);

    if (!_original_node2shard || !_state) {
        // no new allocations took place or object was moved from
        return;
    }

    for (const auto& bs : _replicas) {
        auto orig_it = _original_node2shard->find(bs.node_id);
        if (orig_it == _original_node2shard->end()) {
            // new replica
            _state->remove_allocation(bs);
            _state->remove_final_count(bs);
        } else {
            // original replica that didn't change, erase from the map in
            // preparation for the loop below
            _original_node2shard->erase(orig_it);
        }
    }

    for (const auto& kv : *_original_node2shard) {
        model::broker_shard bs{kv.first, kv.second};
        _state->add_final_count(bs);
    }
}

std::ostream& operator<<(std::ostream& o, const partition_constraints& pc) {
    fmt::print(
      o,
      "{{partition_id: {}, replication_factor: {}, constraints: {}, "
      "existing_group: {}, existing_replicas: {}}}",
      pc.partition_id,
      pc.replication_factor,
      pc.constraints,
      pc.existing_group,
      pc.existing_replicas);
    return o;
}
std::ostream& operator<<(std::ostream& o, const allocation_request& req) {
    fmt::print(o, "{{partion_constraints: {}}}", req.partitions);
    return o;
}
} // namespace cluster
