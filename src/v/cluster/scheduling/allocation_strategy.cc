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

#include "base/vassert.h"
#include "cluster/errc.h"
#include "cluster/logger.h"
#include "cluster/scheduling/allocation_node.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/scheduling/types.h"
#include "cluster/types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "random/generators.h"

#include <seastar/core/shared_ptr.hh>

#include <vector>

namespace cluster {

namespace {

std::vector<model::node_id> solve_hard_constraints(
  const allocation_state::underlying_t& nodes,
  const std::vector<hard_constraint_ptr>& constraints,
  const allocated_partition& partition,
  std::optional<model::node_id> prev) {
    vlog(clusterlog.trace, "applying hard constraints: {}", constraints);

    std::vector<hard_constraint_evaluator> evaluators;
    evaluators.reserve(constraints.size());
    for (auto& c : constraints) {
        evaluators.push_back(c->make_evaluator(partition, prev));
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
        bool result = true;
        for (size_t i = 0; i < constraints.size(); ++i) {
            auto res = evaluators[i](*node);
            if (clusterlog.is_enabled(ss::log_level::trace)) {
                vlog(
                  clusterlog.trace,
                  "{}(node: {}) = {}",
                  constraints[i]->name(),
                  node->id(),
                  res);
            }
            if (!res) {
                result = false;
                break;
            }
        }

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
  const allocation_state::underlying_t& allocation_nodes,
  std::vector<model::node_id> possible_nodes,
  const soft_constraints_level& constraints,
  const allocated_partition& partition,
  std::optional<model::node_id> prev) {
    vlog(
      clusterlog.trace, "optimizing soft constraints level: {}", constraints);

    if (possible_nodes.size() <= 1) {
        // nothing to optimize, choose the only node available after solving
        // hard constraints.
        return possible_nodes;
    }

    std::vector<soft_constraint_evaluator> evaluators;
    evaluators.reserve(constraints.size());
    for (auto& c : constraints) {
        evaluators.push_back(c->make_evaluator(partition, prev));
    }

    uint32_t best_score = 0;
    std::vector<model::node_id> best_fits;

    for (const auto& id : possible_nodes) {
        auto it = allocation_nodes.find(id);
        if (it == allocation_nodes.end()) {
            continue;
        }
        const auto& node = *it->second;

        uint32_t score = 0;
        for (size_t i = 0; i < constraints.size(); ++i) {
            const auto current_score = evaluators[i](node);
            if (clusterlog.is_enabled(ss::log_level::trace)) {
                vlog(
                  clusterlog.trace,
                  "{}(node: {}) = {} ({})",
                  constraints[i]->name(),
                  node.id(),
                  current_score,
                  (double)current_score / soft_constraint::max_score);
            }
            score += current_score;
        }

        /**
         * Score is normalized so that it is always in range [0, max_score_size]
         */
        score /= constraints.size();

        vlog(
          clusterlog.trace,
          "node: {}, total normalized score: {} ({})",
          id,
          score,
          (double)score / soft_constraint::max_score);
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

} // namespace

result<model::node_id> allocation_strategy::choose_node(
  const allocation_state& state,
  const allocation_constraints& request,
  const allocated_partition& partition,
  std::optional<model::node_id> prev) {
    const auto& allocation_nodes = state.allocation_nodes();
    /**
     * evaluate hard constraints
     */
    std::vector<model::node_id> possible_nodes = solve_hard_constraints(
      allocation_nodes, request.hard_constraints, partition, prev);

    vlog(
      clusterlog.trace,
      "after applying hard constraints, eligible nodes: {}",
      possible_nodes);

    if (possible_nodes.empty()) {
        return errc::no_eligible_allocation_nodes;
    }

    /**
     * soft constraints
     */

    // this loop optimizes each level of constrains and then move on to the next
    // one leaving the previous error at the minimum
    for (const auto& lvl : request.soft_constraints) {
        possible_nodes = optimize_constraints(
          allocation_nodes, std::move(possible_nodes), lvl, partition, prev);
        vlog(
          clusterlog.trace,
          "after optimizing soft constraints level, eligible nodes: {}",
          possible_nodes);
    }

    return random_generators::random_choice(possible_nodes);
}

} // namespace cluster
