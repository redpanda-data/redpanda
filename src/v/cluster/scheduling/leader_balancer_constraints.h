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
#pragma once

#include "cluster/scheduling/leader_balancer_types.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "raft/types.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <boost/range/adaptor/reversed.hpp>

#include <cstdint>
#include <functional>
#include <limits>
#include <numeric>
#include <optional>
#include <unordered_map>

namespace cluster::leader_balancer_types {

class shard_index final : public index {
public:
    explicit shard_index(index_type index)
      : _index(std::move(index)) {}

    shard_index(shard_index&& si) noexcept = default;
    shard_index(const shard_index& si) noexcept = default;

    shard_index& operator=(shard_index&&) = default;
    shard_index& operator=(const shard_index&) = default;

    ~shard_index() override = default;

    void update_index(const reassignment& r) override {
        auto replicas = std::move(_index[r.from][r.group]);
        _index[r.to].try_emplace(r.group, std::move(replicas));
        _index[r.from].erase(r.group);
    }

    const index_type& shards() const { return _index; }

private:
    index_type _index;
};

/*
 * Tracks nodes and groups being `muted` by the leader balancer.
 * Reassignments to and from muted nodes and groups should be avoided.
 */
class muted_index final : public index {
public:
    muted_index(
      absl::flat_hash_set<model::node_id> muted_nodes,
      absl::flat_hash_set<raft::group_id> muted_groups)
      : _muted_nodes(std::move(muted_nodes))
      , _muted_groups(std::move(muted_groups)) {}

    muted_index(muted_index&& mi) noexcept = default;
    muted_index& operator=(muted_index&&) = default;
    ~muted_index() override = default;

    const absl::flat_hash_set<model::node_id>& muted_nodes() const {
        return _muted_nodes;
    }

    const absl::flat_hash_set<raft::group_id>& muted_groups() const {
        return _muted_groups;
    }

    absl::flat_hash_set<raft::group_id>& muted_groups() {
        return _muted_groups;
    }

    void update_index(const reassignment&) override {
        // nothing to do here.
    }

    void update_muted_groups(absl::flat_hash_set<raft::group_id> new_groups) {
        _muted_groups = std::move(new_groups);
    }

private:
    absl::flat_hash_set<model::node_id> _muted_nodes;
    absl::flat_hash_set<raft::group_id> _muted_groups;
};

/*
 * Greedy shard balancer strategy is to move leaders from the most loaded core
 * to the least loaded core. The strategy treats all cores equally, ignoring
 * node-level balancing.
 */
class even_shard_load_constraint final
  : public soft_constraint
  , public index {
    /*
     * avoid rounding errors when determining if a move improves balance by
     * adding a small amount of jitter. effectively a move needs to improve by
     * more than this value.
     */
    static constexpr double error_jitter = 0.000001;

public:
    even_shard_load_constraint(shard_index si, const muted_index& mi)
      : _si(std::move(si))
      , _mi(mi)
      , _num_cores(num_cores())
      , _num_groups(num_groups()) {
        rebuild_load_index();
        calculate_error();
    }

    even_shard_load_constraint(even_shard_load_constraint&&) noexcept = default;
    even_shard_load_constraint&
    operator=(even_shard_load_constraint&&) noexcept = default;

    even_shard_load_constraint(const even_shard_load_constraint&) = delete;
    even_shard_load_constraint& operator=(const even_shard_load_constraint&)
      = delete;

    ~even_shard_load_constraint() override = default;

    double error() const { return _error; }
    void update_index(const reassignment& r) override {
        _error = adjusted_error(_error, r.from, r.to);
        _si.update_index(r);
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
    std::optional<reassignment> recommended_reassignment() override;

    std::vector<shard_load> stats() const;

    double calc_target_load() const;

private:
    shard_index _si;
    std::reference_wrapper<const muted_index> _mi;
    size_t _num_cores;
    size_t _num_groups;
    std::vector<index_type::const_iterator> _load;
    absl::flat_hash_map<model::broker_shard, size_t> _load_map;
    double _error{0};

    const shard_index& si() const { return _si; }
    const muted_index& mi() const { return _mi.get(); }

    /*
     * Compute new error for the given reassignment.
     */
    double adjusted_error(
      double current_error,
      const model::broker_shard& from,
      const model::broker_shard& to) const;

    void calculate_error() {
        _error = std::accumulate(
          si().shards().cbegin(),
          si().shards().cend(),
          double{0},
          [target_load = calc_target_load()](auto acc, const auto& e) {
              double num_groups = e.second.size();
              return acc + pow(num_groups - target_load, 2);
          });
    }

    double evaluate_internal(const reassignment& r) override {
        double err = error();
        return err - adjusted_error(err, r.from, r.to);
    }

    size_t num_groups() const;
    size_t num_cores() const;
    /*
     * build the load index, which is a vector of iterators to each element in
     * the core index, where the iterators in the load index are sorted by the
     * number of groups having their leader on a given core.
     */
    void rebuild_load_index();
};

} // namespace cluster::leader_balancer_types
