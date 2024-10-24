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

#include "base/vassert.h"
#include "cluster/scheduling/leader_balancer_constraints.h"
#include "cluster/scheduling/leader_balancer_strategy.h"
#include "cluster/scheduling/leader_balancer_types.h"
#include "container/chunked_hash_map.h"
#include "container/fragmented_vector.h"
#include "model/metadata.h"
#include "raft/fundamental.h"
#include "random/generators.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>

namespace cluster::leader_balancer_types {

/*
 * Given a `shard_index` this class will generate every possible reassignment.
 */
class random_reassignments {
public:
    explicit random_reassignments(const index_type& si)
      : _replicas() {
        for (const auto& [bs, leaders] : si) {
            for (const auto& [group, replicas] : leaders) {
                _current_leaders[group] = bs;

                for (const auto& replica : replicas) {
                    _replicas.push_back({group, replica});
                }
            }
        }
    }

    /*
     * This function randomly selects a reassignment from the set of all
     * possible reassignments for a given shard_index. It will only return
     * a given reassignment once, and when all possible reassignments have
     * been returned it'll return a std::nullopt.
     */
    std::optional<reassignment> generate_reassignment() {
        while (_replicas_begin < _replicas.size()) {
            // If all reassignments are consumed doing the random shuffling
            // incrementally is slower than using std::shuffle in the
            // constructor. However, its assumed that only a small minority
            // of the reassignments will be consumed. Hence the incremental
            // approach is taken.

            auto ri = random_generators::get_int<size_t>(
              _replicas_begin, _replicas.size() - 1);

            std::swap(_replicas[_replicas_begin], _replicas[ri]);

            const auto& replica = _replicas[_replicas_begin];
            _replicas_begin += 1;

            auto replica_leader_it = _current_leaders.find(replica.group_id);
            vassert(
              replica_leader_it != _current_leaders.end(),
              "replica_leader_it == _current_leaders.end()");
            const auto& replica_leader = replica_leader_it->second;
            if (replica_leader == replica.broker_shard) {
                continue;
            }

            return {{replica.group_id, replica_leader, replica.broker_shard}};
        }

        return std::nullopt;
    }

    void update_index(const reassignment& r) {
        _current_leaders[r.group] = r.to;
        _replicas_begin = 0;
    }

private:
    struct replica {
        raft::group_id group_id;
        model::broker_shard broker_shard;
    };
    chunked_hash_map<raft::group_id, model::broker_shard> _current_leaders;

    using replicas_t = fragmented_vector<replica>;
    replicas_t _replicas;
    size_t _replicas_begin{0};
};

class random_hill_climbing_strategy final : public leader_balancer_strategy {
public:
    random_hill_climbing_strategy(
      index_type index,
      group_id_to_topic_id g_to_topic,
      muted_index mi,
      std::optional<preference_index> preference_idx)
      : _mi(std::make_unique<muted_index>(std::move(mi)))
      , _group2topic(
          std::make_unique<group_id_to_topic_id>(std::move(g_to_topic)))
      , _si(std::make_unique<shard_index>(std::move(index)))
      , _reassignments(_si->shards())
      , _etdc(*_group2topic, *_si, *_mi)
      , _eslc(*_si, *_mi) {
        if (preference_idx) {
            _pinning_constr.emplace(
              *_group2topic, std::move(preference_idx.value()));
        }
    }

    double error() const override { return _eslc.error() + _etdc.error(); }

    /*
     * Find a group reassignment that reduces total error.
     */
    std::optional<reassignment>
    find_movement(const leader_balancer_types::muted_groups_t& skip) override {
        for (;;) {
            auto reassignment_opt = _reassignments.generate_reassignment();

            if (!reassignment_opt) {
                break;
            }

            auto reassignment = *reassignment_opt;
            if (
              skip.contains(static_cast<uint64_t>(reassignment.group))
              || _mi->muted_nodes().contains(reassignment.from.node_id)
              || _mi->muted_nodes().contains(reassignment.to.node_id)) {
                continue;
            }

            // Hierarchical optimization: first check if the proposed
            // reassignment improves the pinning objective (makes the leaders
            // distribution better conform to the provided pinning
            // configuration). If the pinning objective remains at the same
            // level, check balancing objectives.

            if (_pinning_constr) {
                auto pinning_diff = _pinning_constr->evaluate(reassignment);
                if (pinning_diff < -error_jitter) {
                    continue;
                } else if (pinning_diff > error_jitter) {
                    return reassignment_opt;
                }
            }

            auto balancing_diff = _etdc.evaluate(reassignment)
                                  + _eslc.evaluate(reassignment);
            if (balancing_diff <= error_jitter) {
                continue;
            }

            return reassignment_opt;
        }

        return std::nullopt;
    }

    void apply_movement(const reassignment& reassignment) override {
        _etdc.update_index(reassignment);
        _eslc.update_index(reassignment);
        _mi->update_index(reassignment);
        _si->update_index(reassignment);
        _reassignments.update_index(reassignment);
    }

    /*
     * Return current strategy stats.
     */
    std::vector<shard_load> stats() const override { return _eslc.stats(); }

private:
    static constexpr double error_jitter = 0.000001;

    std::unique_ptr<muted_index> _mi;
    std::unique_ptr<group_id_to_topic_id> _group2topic;
    std::unique_ptr<shard_index> _si;
    random_reassignments _reassignments;

    std::optional<pinning_constraint> _pinning_constr;
    even_topic_distribution_constraint _etdc;
    even_shard_load_constraint _eslc;
};

} // namespace cluster::leader_balancer_types
