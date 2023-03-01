/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cluster/scheduling/leader_balancer_constraints.h"

namespace cluster::leader_balancer_types {

std::optional<reassignment>
even_shard_load_constraint::recommended_reassignment() {
    const auto curr_error = error();

    // Consider each group from high load core, and record the reassignment
    // involving the lowest load "to" core.
    for (const auto& from : boost::adaptors::reverse(_load)) {
        if (mi().muted_nodes().contains(from->first.node_id)) {
            continue;
        }

        constexpr size_t load_unset = std::numeric_limits<size_t>::max();
        size_t lowest_load = load_unset;
        reassignment lowest_reassign{};

        // Consider each group from high load core, and record the
        // reassignment involving the lowest load "to" core.
        for (const auto& group : from->second) {
            if (mi().muted_groups().contains(group.first)) {
                continue;
            }

            // iterate over all the replicas and look for the lowest load
            // shard in the replica list
            for (const auto& to_shard : group.second) {
                auto load = _load_map.at(to_shard);
                if (likely(load >= lowest_load)) {
                    // there is no point in evaluating this move, it is
                    // worse than the best one we've found so far.
                    continue;
                }

                if (mi().muted_nodes().contains(to_shard.node_id)) {
                    continue;
                }

                lowest_load = load;
                lowest_reassign = {group.first, from->first, to_shard};
            }
        }

        if (lowest_load != load_unset) {
            // We found a possible reassignment while looking at the current
            // "from" shard, and while it is the best possible reassignment
            // found it may not improve the error
            auto new_error = adjusted_error(
              curr_error, lowest_reassign.from, lowest_reassign.to);
            if (new_error + error_jitter < curr_error) {
                return lowest_reassign;
            }
        }
    }

    return std::nullopt;
}

double even_shard_load_constraint::adjusted_error(
  double current_error,
  const model::broker_shard& from,
  const model::broker_shard& to) const {
    auto target_load = calc_target_load();
    const auto from_count = static_cast<double>(si().shards().at(from).size());
    const auto to_count = static_cast<double>(si().shards().at(to).size());
    // subtract original contribution
    current_error -= pow(from_count - target_load, 2);
    current_error -= pow(to_count - target_load, 2);
    // add back in the adjusted amount
    current_error += pow(from_count - 1 - target_load, 2);
    current_error += pow(to_count + 1 - target_load, 2);
    return current_error;
}

size_t even_shard_load_constraint::num_groups() const {
    return std::accumulate(
      si().shards().cbegin(),
      si().shards().cend(),
      size_t{0},
      [this](auto acc, const auto& e) {
          /*
           * we aren't going to attempt to move leadership between muted
           * nodes, so we only count groups with leaders on non-muted nodes
           * when calculating error / target load.
           */
          if (mi().muted_nodes().contains(e.first.node_id)) {
              return acc;
          }
          return acc + e.second.size();
      });
}

double even_shard_load_constraint::calc_target_load() const {
    return static_cast<double>(_num_groups) / static_cast<double>(_num_cores);
}

size_t even_shard_load_constraint::num_cores() const {
    return std::count_if(
      si().shards().cbegin(), si().shards().cend(), [this](const auto& core) {
          return !mi().muted_nodes().contains(core.first.node_id);
      });
}

void even_shard_load_constraint::rebuild_load_index() {
    _load.clear();
    _load_map.clear();

    _load.reserve(si().shards().size());
    _load_map.reserve(si().shards().size());
    for (auto it = si().shards().cbegin(); it != si().shards().cend(); ++it) {
        _load.push_back(it);
        _load_map.emplace(it->first, it->second.size());
    }
    std::sort(_load.begin(), _load.end(), [](const auto& a, const auto& b) {
        return a->second.size() < b->second.size();
    });
}

std::vector<shard_load> even_shard_load_constraint::stats() const {
    std::vector<shard_load> ret;
    ret.reserve(_load.size());
    std::transform(
      _load.cbegin(), _load.cend(), std::back_inserter(ret), [](const auto& e) {
          // oddly, absl::btree::size returns a signed type
          return shard_load{e->first, static_cast<size_t>(e->second.size())};
      });
    return ret;
}

} // namespace cluster::leader_balancer_types
