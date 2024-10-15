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
#include "container/chunked_hash_map.h"
#include "model/metadata.h"
#include "raft/fundamental.h"

#include <seastar/core/metrics.hh>
#include <seastar/core/sstring.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <boost/range/adaptor/reversed.hpp>

#include <functional>
#include <numeric>
#include <optional>

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
      leader_balancer_types::muted_groups_t muted_groups)
      : _muted_nodes(std::move(muted_nodes))
      , _muted_groups(std::move(muted_groups)) {}

    muted_index(muted_index&& mi) noexcept = default;
    muted_index& operator=(muted_index&&) = default;
    ~muted_index() override = default;

    const absl::flat_hash_set<model::node_id>& muted_nodes() const {
        return _muted_nodes;
    }

    const leader_balancer_types::muted_groups_t& muted_groups() const {
        return _muted_groups;
    }

    leader_balancer_types::muted_groups_t& muted_groups() {
        return _muted_groups;
    }

    void update_index(const reassignment&) override {
        // nothing to do here.
    }

    void update_muted_groups(leader_balancer_types::muted_groups_t new_groups) {
        _muted_groups = std::move(new_groups);
    }

private:
    absl::flat_hash_set<model::node_id> _muted_nodes;
    leader_balancer_types::muted_groups_t _muted_groups;
};

/*
 * Evaluates how evenly distributed leadership for a topic is on a
 * given cluster. Providing a way to evaluate a given reassignment of
 * leadership and a recommendation of reassignments to improve error.
 */
class even_topic_distribution_constraint final
  : public soft_constraint
  , public index {
    // Avoid rounding errors when determining if a move improves balance by
    // adding a small amount of jitter. Effectively a move needs to improve by
    // more than this value.
    static constexpr double error_jitter = 0.000001;

public:
    even_topic_distribution_constraint(
      const group_id_to_topic_id&,
      const shard_index& si,
      const muted_index& mi);

    even_topic_distribution_constraint(
      even_topic_distribution_constraint&&) noexcept
      = default;
    even_topic_distribution_constraint&
    operator=(even_topic_distribution_constraint&&) noexcept
      = default;

    even_topic_distribution_constraint(
      const even_topic_distribution_constraint&)
      = delete;
    even_topic_distribution_constraint&
    operator=(const even_topic_distribution_constraint&)
      = delete;

    ~even_topic_distribution_constraint() override = default;

    double error() const { return _error; }

    void update_index(const reassignment& r) override;

private:
    std::reference_wrapper<const shard_index> _si;
    std::reference_wrapper<const muted_index> _mi;
    std::reference_wrapper<const group_id_to_topic_id> _group_to_topic_id;
    double _error{0};

    // Stores the number of leaders on a given node per topic.
    topic_map<chunked_hash_map<model::broker_shard, size_t>> _topic_shard_index;
    topic_map<size_t> _topic_partition_index;
    topic_map<chunked_hash_set<model::broker_shard>> _topic_replica_index;
    topic_map<double> _topic_skew;
    topic_map<double> _topic_opt_leaders;

    const shard_index& si() const { return _si.get(); }
    const muted_index& mi() const { return _mi.get(); }
    const group_id_to_topic_id& group_to_topic_id() const {
        return _group_to_topic_id.get();
    }

    void calc_topic_skew();
    void rebuild_indexes();

    /*
     * Compute new error for the given reassignment.
     */
    double adjusted_error(
      double current_error,
      const topic_id_t& topic_id,
      const model::broker_shard& from,
      const model::broker_shard& to) const;

    void calculate_error() {
        _error = std::accumulate(
          _topic_skew.cbegin(),
          _topic_skew.cend(),
          double{0},
          [](auto acc, const auto& t_s) { return acc + t_s.second; });
    }

    double evaluate_internal(const reassignment& r) override {
        double current_error = error();
        const auto& topic_id = group_to_topic_id().at(r.group);
        return current_error
               - adjusted_error(current_error, topic_id, r.from, r.to);
    }
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
    even_shard_load_constraint(const shard_index& si, const muted_index& mi)
      : _si(si)
      , _mi(mi)
      , _num_cores(num_cores())
      , _num_groups(num_groups()) {
        calculate_error();
    }

    even_shard_load_constraint(even_shard_load_constraint&&) noexcept = default;
    even_shard_load_constraint& operator=(even_shard_load_constraint&&) noexcept
      = default;

    even_shard_load_constraint(const even_shard_load_constraint&) = delete;
    even_shard_load_constraint& operator=(const even_shard_load_constraint&)
      = delete;

    ~even_shard_load_constraint() override = default;

    double error() const { return _error; }
    void update_index(const reassignment& r) override {
        _error = adjusted_error(_error, r.from, r.to);
    }

    std::vector<shard_load> stats() const;

    double calc_target_load() const;

private:
    std::reference_wrapper<const shard_index> _si;
    std::reference_wrapper<const muted_index> _mi;
    size_t _num_cores;
    size_t _num_groups;
    double _error{0};

    const shard_index& si() const { return _si.get(); }
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

    using load_t = std::vector<index_type::const_iterator>;
    using load_map_t = absl::flat_hash_map<model::broker_shard, size_t>;

    /*
     * build the load index, which is a vector of iterators to each element in
     * the core index, where the iterators in the load index are sorted by the
     * number of groups having their leader on a given core.
     */
    std::pair<load_t, load_map_t> build_load_indexes() const;
};

// Constraint implementing leaders pinning preference objective.
class pinning_constraint final : public soft_constraint {
public:
    pinning_constraint(
      const group_id_to_topic_id& group2topic,
      preference_index&& preference_idx)
      : _group2topic(group2topic)
      , _preference_idx(std::move(preference_idx)) {}

private:
    double evaluate_internal(const reassignment& r) override;

private:
    std::reference_wrapper<const group_id_to_topic_id> _group2topic;
    preference_index _preference_idx;
};

} // namespace cluster::leader_balancer_types
