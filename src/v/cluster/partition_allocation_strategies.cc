/*
 * Copyright 2020 Vectorized, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/partition_allocation_strategies.h"

#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"

#include <seastar/util/defer.hh>

#include <absl/container/node_hash_set.h>

namespace cluster {

inline bool contains_node_already(
  const std::vector<model::broker_shard>& current_allocations,
  model::node_id id) {
    auto it = std::find_if(
      current_allocations.cbegin(),
      current_allocations.cend(),
      [id](const model::broker_shard& replica) {
          return replica.node_id == id;
      });

    return it != current_allocations.end();
}

result<allocation_strategy::replicas_t>
round_robin_allocation_strategy::allocate_partition(
  const allocation_configuration& cfg, allocation_state& state) {
    allocation_strategy::replicas_t replicas;
    replicas.reserve(cfg.replication_factor);
    auto it = state.allocation_nodes().begin();

    // adjust iterator so we will continue where we left with previous
    // allocation, (we do not keep the iterator since it may be invalidated)
    if (_next_idx < state.allocation_nodes().size()) {
        it = std::next(it, _next_idx);
    } else {
        _next_idx = 0;
    }
    const auto partitions_to_allocate = cfg.replication_factor
                                        - cfg.current_allocations.size();
    uint32_t attempts = 0;
    while (replicas.size() < partitions_to_allocate) {
        // we went over all allocation nodes, terminate allocation with an
        // error
        if (attempts >= state.allocation_nodes().size()) {
            state.rollback(replicas);
            return errc::topic_invalid_partitions;
        }

        auto defer = ss::defer([this, &it, &attempts, &state] {
            ++_next_idx;
            ++it;
            ++attempts;
            if (it == state.allocation_nodes().end()) {
                it = state.allocation_nodes().begin();
                _next_idx = 0;
            }
        });

        if (it->second->is_full()) {
            continue;
        }

        if (contains_node_already(cfg.current_allocations, it->second->id())) {
            continue;
        }
        auto shard = state.allocate(it->first);

        if (shard) {
            replicas.push_back(model::broker_shard{
              .node_id = it->first, .shard = shard.value()});
        }
    }
    std::move(
      cfg.current_allocations.begin(),
      cfg.current_allocations.end(),
      std::back_inserter(replicas));
    return replicas;
}

result<allocation_strategy::replicas_t>
round_robin_allocation_strategy::allocate_partition(
  const custom_allocation_configuration& cfg, allocation_state& state) {
    // handle custom assignment first
    allocation_strategy::replicas_t replicas;
    replicas.reserve(cfg.nodes.size());

    for (auto n : cfg.nodes) {
        auto shard = state.allocate(n);
        if (!shard) {
            // rollback what we currently have and return an error
            state.rollback(replicas);
            return shard.error();
        }
        replicas.push_back(
          model::broker_shard{.node_id = n, .shard = shard.value()});
    }
    return replicas;
}
} // namespace cluster
