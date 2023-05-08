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

#include <seastar/core/shared_ptr.hh>

#include <vector>

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
  const std::vector<model::broker_shard>& current_replicas,
  const std::vector<hard_constraint_ptr>& constraints,
  const allocation_state::underlying_t& nodes) {
    std::vector<hard_constraint_evaluator> evaluators;
    evaluators.reserve(constraints.size());
    for (auto& c : constraints) {
        evaluators.push_back(c->make_evaluator(current_replicas));
    }

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
          evaluators.begin(),
          evaluators.end(),
          [&node](const hard_constraint_evaluator& ev) { return ev(*node); });

        if (result) {
            possible_nodes.push_back(p.first);
        }
    }
    return possible_nodes;
}
/**
 * Optimize a single level of constraints, i.e. it finds a best fit set of nodes
 * for a given level of constraints
 */
std::vector<model::node_id> optimize_constraints(
  const std::vector<model::node_id>& possible_nodes,
  const soft_constraints_level& constraints,
  const std::vector<model::broker_shard>& current_replicas,
  const allocation_state::underlying_t& allocation_nodes) {
    std::vector<soft_constraint_evaluator> evaluators;
    evaluators.reserve(constraints.size());
    for (auto& c : constraints) {
        evaluators.push_back(c->make_evaluator(current_replicas));
    }

    uint32_t best_score = 0;
    std::vector<model::node_id> best_fits;

    for (const auto& id : possible_nodes) {
        auto it = allocation_nodes.find(id);
        if (it == allocation_nodes.end()) {
            continue;
        }
        /**
         * Score is normalized so that it is always in range [0, max_score_size]
         */
        uint32_t score = std::accumulate(
                           evaluators.begin(),
                           evaluators.end(),
                           uint32_t{0},
                           [&node = it->second](
                             uint32_t score,
                             const soft_constraint_evaluator& ev) {
                               const auto current_score = ev(*node);
                               return score + current_score;
                           })
                         / constraints.size();

        vlog(
          clusterlog.trace, "node: {}, total normalized score: {}", id, score);
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
    return best_fits;
}

model::node_id find_best_fit(
  const std::vector<model::broker_shard>& current_replicas,
  const soft_constraints_hierarchy& constraints,
  std::vector<model::node_id> possible_nodes,
  const allocation_state::underlying_t& allocation_nodes) {
    std::vector<model::node_id> to_optimize = std::move(possible_nodes);
    // this loop optimizes each level of constrains and then move on to the next
    // one leaving the previous error at the minimum

    for (const auto& lvl : constraints) {
        to_optimize = optimize_constraints(
          to_optimize, lvl, current_replicas, allocation_nodes);
    }

    return random_generators::random_choice(to_optimize);
}

allocation_strategy simple_allocation_strategy() {
    class impl : public allocation_strategy::impl {
    public:
        result<model::broker_shard> allocate_replica(
          const std::vector<model::broker_shard>& current_replicas,
          const allocation_constraints& request,
          allocation_state& state,
          const partition_allocation_domain domain) final {
            const auto& nodes = state.allocation_nodes();
            /**
             * evaluate hard constraints
             */
            std::vector<model::node_id> possible_nodes = solve_hard_constraints(
              current_replicas,
              request.hard_constraints,
              state.allocation_nodes());

            if (possible_nodes.empty()) {
                return errc::no_eligible_allocation_nodes;
            }

            /**
             * soft constraints
             */
            auto best_fit = find_best_fit(
              current_replicas,
              request.soft_constraints,
              possible_nodes,
              nodes);

            auto it = nodes.find(best_fit);
            vassert(
              it != nodes.end(),
              "allocated node with id {} have to be present",
              best_fit);
            auto core = (it->second)->allocate(domain);
            return model::broker_shard{
              .node_id = it->first,
              .shard = core,
            };
        }
    };
    return make_allocation_strategy<impl>();
};

} // namespace cluster
