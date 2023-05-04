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

void allocated_partition::add_replica(model::broker_shard replica) {
    if (!_original) {
        _original = absl::flat_hash_set<model::broker_shard>(
          _replicas.begin(), _replicas.end());
    }

    _replicas.push_back(replica);
}

std::vector<model::broker_shard> allocated_partition::release_new_partition() {
    vassert(
      _original && _original->empty(),
      "new partition shouldn't have previous replicas");
    _state = nullptr;
    return std::move(_replicas);
}

bool allocated_partition::is_original(
  const model::broker_shard& replica) const {
    if (_original) {
        return _original->contains(replica);
    }
    return std::find(_replicas.begin(), _replicas.end(), replica)
           != _replicas.end();
}

allocated_partition::~allocated_partition() {
    oncore_debug_verify(_oncore);

    if (!_original || !_state) {
        // no new allocations took place or object was moved from
        return;
    }

    for (const auto& bs : _replicas) {
        if (!_original->contains(bs)) {
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
