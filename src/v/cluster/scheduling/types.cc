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
  std::vector<partition_assignment> assignments,
  allocation_state& state,
  const partition_allocation_domain domain)
  : _assignments(std::move(assignments))
  , _state(state.weak_from_this())
  , _domain(domain) {}

allocation_units::allocation_units(
  std::vector<partition_assignment> assignments,
  std::vector<model::broker_shard> previous_allocations,
  allocation_state& state,
  const partition_allocation_domain domain)
  : _assignments(std::move(assignments))
  , _state(state.weak_from_this())
  , _domain(domain) {
    _previous.reserve(previous_allocations.size());
    for (auto& prev : previous_allocations) {
        _previous.emplace(prev);
    }
}

allocation_units::~allocation_units() {
    oncore_debug_verify(_oncore);
    for (auto& pas : _assignments) {
        for (auto& replica : pas.replicas) {
            if (!_previous.contains(replica) && _state) {
                _state->deallocate(replica, _domain);
            }
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
