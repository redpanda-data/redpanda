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

#include "cluster/scheduling/allocation_strategy.h"

#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/scheduling/allocation_node.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/scheduling/types.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "random/generators.h"
#include "vassert.h"

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

std::vector<model::node_id> solve_hard_constraints(
  const std::vector<allocation_constraints::hard_constraint_ev_ptr>&
    constraints,
  const allocation_state::underlying_t& nodes) {
    // empty hard constraints, all nodes are eligible
    std::vector<model::node_id> possible_nodes;
    if (unlikely(constraints.empty())) {
        possible_nodes.reserve(nodes.size());
        std::transform(
          nodes.begin(),
          nodes.end(),
          std::back_inserter(possible_nodes),
          [](const allocation_state::underlying_t::value_type& v) {
              return v.first;
          });
        return possible_nodes;
    }
    for (auto& p : nodes) {
        auto& node = p.second;
        auto result = std::all_of(
          constraints.begin(),
          constraints.end(),
          [&node](const allocation_constraints::hard_constraint_ev_ptr& ev) {
              return ev->evaluate(*node);
          });

        if (result) {
            possible_nodes.push_back(p.first);
        }
    }
    return possible_nodes;
}

model::node_id find_best_fit(
  const std::vector<allocation_constraints::soft_constraint_ev_ptr>&
    constraints,
  const std::vector<model::node_id>& possible_nodes,
  const allocation_state::underlying_t& nodes) {
    if (possible_nodes.size() == 1) {
        return possible_nodes.front();
    }

    uint32_t best_score = 0;
    std::vector<model::node_id> best_fits;

    for (const auto& id : possible_nodes) {
        auto it = nodes.find(id);
        if (it == nodes.end()) {
            continue;
        }

        uint32_t score = std::accumulate(
          constraints.begin(),
          constraints.end(),
          0,
          [&node = it->second](
            uint32_t score,
            const allocation_constraints::soft_constraint_ev_ptr& ev) {
              return score + ev->score(*node);
          });

        if (score >= best_score) {
            if (score > best_score) {
                // untied, winner clear out existing winners
                best_score = score;
                best_fits.clear();
            }
            best_fits.push_back(it->first);
        }
    }

    vassert(!best_fits.empty(), "best_fits empty");

    // we break ties randomly, by selecting a random node out of those
    // with the highest score
    return best_fits.at(random_generators::get_int(best_fits.size() - 1));
}

allocation_strategy simple_allocation_strategy() {
    class impl : public allocation_strategy::impl {
    public:
        result<model::broker_shard> allocate_replica(
          const allocation_constraints& request,
          allocation_state& state) final {
            const auto& nodes = state.allocation_nodes();
            /**
             * evaluate hard constraints
             */
            std::vector<model::node_id> possible_nodes = solve_hard_constraints(
              request.hard_constraints, state.allocation_nodes());

            if (possible_nodes.empty()) {
                return errc::no_eligible_allocation_nodes;
            }

            /**
             * soft constraints
             */
            auto best_fit = find_best_fit(
              request.soft_constraints, possible_nodes, nodes);

            auto it = nodes.find(best_fit);
            vassert(
              it != nodes.end(),
              "allocated node with id {} have to be present",
              best_fit);
            auto core = (it->second)->allocate();
            return model::broker_shard{
              .node_id = it->first,
              .shard = core,
            };
        }
    };
    return make_allocation_strategy<impl>();
};

} // namespace cluster
