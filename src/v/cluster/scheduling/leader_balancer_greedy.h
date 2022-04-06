/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#pragma once

#include "cluster/scheduling/leader_balancer_strategy.h"

#include <boost/range/adaptor/reversed.hpp>

/*
 * Greedy shard balancer strategy is to move leaders from the most loaded core
 * to the least loaded core. The strategy treats all cores equally, ignoring
 * node-level balancing.
 */
namespace cluster {

class greedy_balanced_shards final : private leader_balancer_strategy {
    /*
     * avoid rounding errors when determining if a move improves balance by
     * adding a small amount of jitter. effectively a move needs to improve by
     * more than this value.
     */
    static constexpr double error_jitter = 0.000001;

public:
    explicit greedy_balanced_shards(
      index_type cores, absl::flat_hash_set<model::node_id> muted_nodes)
      : _cores(std::move(cores))
      , _muted_nodes(std::move(muted_nodes))
      , _num_cores(num_cores())
      , _num_groups(num_groups()) {
        rebuild_load_index();
    }

    double error() const final {
        auto target_load = static_cast<double>(_num_groups)
                           / static_cast<double>(_num_cores);
        return std::accumulate(
          _cores.cbegin(),
          _cores.cend(),
          double{0},
          [target_load](auto acc, const auto& e) {
              double num_groups = e.second.size();
              return acc + pow(num_groups - target_load, 2);
          });
    }

    /*
     * Compute new error for the given reassignment.
     */
    double adjusted_error(
      const model::broker_shard& from, const model::broker_shard& to) const {
        auto target_load = static_cast<double>(_num_groups)
                           / static_cast<double>(_num_cores);
        auto e = error();
        const auto from_count = static_cast<double>(_cores.at(from).size());
        const auto to_count = static_cast<double>(_cores.at(to).size());
        // subtract original contribution
        e -= pow(from_count - target_load, 2);
        e -= pow(to_count - target_load, 2);
        // add back in the adjusted amount
        e += pow(from_count - 1 - target_load, 2);
        e += pow(to_count + 1 - target_load, 2);
        return e;
    }

    /*
     * Find a group reassignment that improves overall error. The general
     * approach is to select a group from the highest loaded shard and move
     * leadership for that group to the least loaded shard that the group is
     * compatible with.
     *
     * Clearly this is a costly method in terms of runtime complexity.
     * Measurements for clusters with several thousand partitions indicate a
     * real time execution cost of at most a couple hundred micros. Other
     * strategies are sure to improve this as we tackle larger configurations.
     *
     * Muted nodes are nodes that should be treated as if they have no available
     * capacity. So do not move leadership to a muted node, but any leaders on a
     * muted node should not be touched in case the mute is temporary.
     */
    std::optional<reassignment>
    find_movement(const absl::flat_hash_set<raft::group_id>& skip) const final {
        const auto curr_error = error();

        /*
         * from: high load core
         * to: less loaded core
         */
        for (const auto& from : boost::adaptors::reverse(_load)) {
            if (_muted_nodes.contains(from->first.node_id)) {
                continue;
            }
            for (const auto& to : _load) {
                // skip cores on muted nodes
                if (_muted_nodes.contains(to->first.node_id)) {
                    continue;
                }
                // consider group from high load core
                for (const auto& group : from->second) {
                    if (skip.contains(group.first)) {
                        continue;
                    }
                    /*
                     * a valid move requires that the group's replica set
                     * contain the less loaded `to` core.
                     */
                    const auto it = std::find(
                      group.second.cbegin(), group.second.cend(), to->first);
                    if (it == group.second.cend()) {
                        continue;
                    }
                    /*
                     * choose reassignment if error improves
                     */
                    if (
                      (adjusted_error(from->first, to->first) + error_jitter)
                      < curr_error) {
                        return reassignment{
                          group.first, from->first, to->first};
                    }
                }
            }
        }

        return std::nullopt;
    }

    std::vector<shard_load> stats() const final {
        std::vector<shard_load> ret;
        ret.reserve(_load.size());
        std::transform(
          _load.cbegin(),
          _load.cend(),
          std::back_inserter(ret),
          [](const auto& e) {
              // oddly, absl::btree::size returns a signed type
              return shard_load{
                e->first, static_cast<size_t>(e->second.size())};
          });
        return ret;
    }

private:
    size_t num_groups() const {
        return std::accumulate(
          _cores.cbegin(),
          _cores.cend(),
          size_t{0},
          [this](auto acc, const auto& e) {
              /*
               * we aren't going to attempt to move leadership between muted
               * nodes, so we only count groups with leaders on non-muted nodes
               * when calculating error / target load.
               */
              if (_muted_nodes.contains(e.first.node_id)) {
                  return acc;
              }
              return acc + e.second.size();
          });
    }

    size_t num_cores() const {
        return std::count_if(
          _cores.cbegin(), _cores.cend(), [this](const auto& core) {
              return !_muted_nodes.contains(core.first.node_id);
          });
    }

    /*
     * build the load index, which is a vector of iterators to each element in
     * the core index, where the iterators in the load index are sorted by the
     * number of groups having their leader on a given core.
     */
    void rebuild_load_index() {
        _load.clear();
        _load.reserve(_cores.size());
        for (auto it = _cores.cbegin(); it != _cores.cend(); ++it) {
            _load.push_back(it);
        }
        std::sort(_load.begin(), _load.end(), [](const auto& a, const auto& b) {
            return a->second.size() < b->second.size();
        });
    }

    index_type _cores;
    absl::flat_hash_set<model::node_id> _muted_nodes;
    size_t _num_cores;
    size_t _num_groups;
    std::vector<index_type::const_iterator> _load;
};

} // namespace cluster
