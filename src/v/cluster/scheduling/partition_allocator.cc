// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/scheduling/partition_allocator.h"

#include "cluster/fwd.h"
#include "cluster/logger.h"
#include "cluster/scheduling/constraints.h"
#include "cluster/types.h"
#include "model/metadata.h"

#include <absl/container/node_hash_set.h>

#include <algorithm>
#include <exception>
#include <iterator>
#include <vector>

namespace cluster {

partition_allocator::partition_allocator()
  : _state(std::make_unique<allocation_state>())
  , _allocation_strategy(simple_allocation_strategy()) {}

allocation_constraints default_constraints() {
    allocation_constraints req;
    req.hard_constraints.push_back(
      ss::make_lw_shared<hard_constraint_evaluator>(not_fully_allocated()));
    req.hard_constraints.push_back(
      ss::make_lw_shared<hard_constraint_evaluator>(not_decommissioned()));
    req.soft_constraints.push_back(
      ss::make_lw_shared<soft_constraint_evaluator>(least_allocated()));
    return req;
}

result<std::vector<model::broker_shard>>
partition_allocator::allocate_partition(partition_constraints p_constraints) {
    vlog(
      clusterlog.trace,
      "allocating partition with constraints: {}",
      p_constraints);
    if (
      p_constraints.replication_factor <= 0
      || _state->available_nodes() < p_constraints.replication_factor) {
        return errc::topic_invalid_replication_factor;
    }

    intermediate_allocation<model::broker_shard> replicas(
      *_state, p_constraints.replication_factor);

    for (auto r = 0; r < p_constraints.replication_factor; ++r) {
        auto effective_constraits = default_constraints();
        effective_constraits.hard_constraints.push_back(
          ss::make_lw_shared<hard_constraint_evaluator>(
            distinct_from(replicas.get())));
        effective_constraits.add(std::move(p_constraints.constraints));

        auto replica = _allocation_strategy.allocate_replica(
          effective_constraits, *_state);

        if (!replica) {
            return replica.error();
        }
        replicas.push_back(replica.value());
    }

    return std::move(replicas).finish();
}

result<allocation_units>
partition_allocator::allocate(allocation_request request) {
    vlog(
      clusterlog.trace,
      "allocation request for {} partitions",
      request.partitions.size());

    intermediate_allocation<partition_assignment> assignments(
      *_state, request.partitions.size());

    for (auto& p_constraints : request.partitions) {
        auto const partition_id = p_constraints.partition_id;
        auto replicas = allocate_partition(std::move(p_constraints));
        if (!replicas) {
            return replicas.error();
        }
        assignments.push_back(partition_assignment{
          .group = _state->next_group_id(),
          .id = partition_id,
          .replicas = std::move(replicas.value()),
        });
    }

    return allocation_units(std::move(assignments).finish(), _state.get());
}

result<std::vector<model::broker_shard>>
partition_allocator::do_reallocate_partition(
  partition_constraints p_constraints,
  const std::vector<model::broker_shard>& not_changed_replicas) {
    vlog(
      clusterlog.debug,
      "reallocating {}, replicas left: {}",
      p_constraints,
      not_changed_replicas);
    /**
     * We do not have to reallocate any of the replicas, do nothing
     */
    if (p_constraints.replication_factor == not_changed_replicas.size()) {
        return not_changed_replicas;
    }
    p_constraints.constraints.hard_constraints.push_back(
      ss::make_lw_shared<hard_constraint_evaluator>(
        distinct_from(not_changed_replicas)));
    p_constraints.replication_factor -= not_changed_replicas.size();
    auto result = allocate_partition(std::move(p_constraints));
    if (!result) {
        return result.error();
    }
    auto new_replicas = std::move(result.value());
    std::move(
      not_changed_replicas.begin(),
      not_changed_replicas.end(),
      std::back_inserter(new_replicas));

    return new_replicas;
}

result<allocation_units> partition_allocator::reallocate_partition(
  partition_constraints partition_constraints,
  const partition_assignment& current_assignment) {
    auto replicas = do_reallocate_partition(
      std::move(partition_constraints), current_assignment.replicas);

    if (!replicas) {
        return replicas.error();
    }

    partition_assignment assignment{
      .group = current_assignment.group,
      .id = current_assignment.id,
      .replicas = std::move(replicas.value()),
    };

    return allocation_units(
      {std::move(assignment)}, current_assignment.replicas, _state.get());
}

result<allocation_units> partition_allocator::reassign_decommissioned_replicas(
  const partition_assignment& current_assignment) {
    uint16_t replication_factor = current_assignment.replicas.size();
    auto current_replicas = current_assignment.replicas;
    std::erase_if(current_replicas, [this](const model::broker_shard& bs) {
        auto it = _state->allocation_nodes().find(bs.node_id);
        return it == _state->allocation_nodes().end()
               || it->second->is_decommissioned();
    });

    auto req = default_constraints();
    req.hard_constraints.push_back(
      ss::make_lw_shared<hard_constraint_evaluator>(
        distinct_from(current_replicas)));

    auto replicas = do_reallocate_partition(
      partition_constraints(current_assignment.id, replication_factor),
      current_replicas);

    if (!replicas) {
        return replicas.error();
    }

    partition_assignment assignment{
      .group = current_assignment.group,
      .id = current_assignment.id,
      .replicas = std::move(replicas.value()),
    };

    return allocation_units(
      {std::move(assignment)}, current_replicas, _state.get());
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

} // namespace cluster
