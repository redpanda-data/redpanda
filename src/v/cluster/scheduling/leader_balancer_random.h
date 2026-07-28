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
#include "container/chunked_vector.h"
#include "model/metadata.h"
#include "raft/fundamental.h"
#include "random/generators.h"

#include <algorithm>
#include <cmath>
#include <compare>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <variant>

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

    void reset() { _replicas_begin = 0; }

    void update_index(const reassignment& r) {
        _current_leaders[r.group] = r.to;
        reset();
    }

private:
    struct replica {
        raft::group_id group_id;
        model::broker_shard broker_shard;
    };
    chunked_hash_map<raft::group_id, model::broker_shard> _current_leaders;

    using replicas_t = chunked_vector<replica>;
    replicas_t _replicas;
    size_t _replicas_begin{0};
};

template<typename Impl>
class climbing_strategy_base : public leader_balancer_strategy {
protected:
    using base = climbing_strategy_base<Impl>;

public:
    climbing_strategy_base(
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
      , _eslc(*_si, *_mi)
      , _enlc(*_si) {
        static_assert(
          requires(Impl& impl, const reassignment& reassignment) {
              typename Impl::reassignment_score;
              {
                  impl.get_reassignment_score(reassignment)
              } -> std::same_as<
                std::optional<typename Impl::reassignment_score>>;
              {
                  impl.generate_reassignment()
              } -> std::same_as<std::optional<leader_balancer_types::reassignment>>;
          },
          "Impl must satisfy the climbing strategy interface");
        if (preference_idx) {
            _pinning_constr.emplace(
              *_group2topic, std::move(preference_idx.value()));
        }
    }

    double error() const override { return _eslc.error() + _etdc.error(); }

    void apply_movement(const reassignment& reassignment) override {
        _etdc.update_index(reassignment);
        _eslc.update_index(reassignment);
        _enlc.update_index(reassignment);
        _mi->update_index(reassignment);
        _si->update_index(reassignment);
        _reassignments.update_index(reassignment);
    }

    /*
     * Return current strategy stats.
     */
    std::vector<shard_load> stats() const override { return _eslc.stats(); }

protected:
    static constexpr double error_jitter = 0.000001;

    struct reassignment_with_score {
        reassignment reassignment;
        Impl::reassignment_score score;
    };

    std::optional<reassignment> do_find_movement_without_score(
      const leader_balancer_types::muted_groups_t& skip) {
        return do_find_movement_with_score(skip).transform(
          [](auto&& s) { return s.reassignment; });
    }

    std::optional<reassignment_with_score> do_find_movement_with_score(
      const leader_balancer_types::muted_groups_t& skip) {
        for (;;) {
            auto reassignment_opt
              = static_cast<Impl&>(*this).generate_reassignment();

            if (!reassignment_opt) {
                return std::nullopt;
            }

            auto reassignment = *reassignment_opt;
            if (
              skip.contains(static_cast<uint64_t>(reassignment.group))
              || _mi->muted_nodes().contains(reassignment.from.node_id)
              || _mi->muted_nodes().contains(reassignment.to.node_id)) {
                continue;
            }

            if (
              auto maybe_score = static_cast<Impl&>(*this)
                                   .get_reassignment_score(reassignment)) {
                return {{reassignment, *maybe_score}};
            }
        }

        return std::nullopt;
    }

private:
    std::unique_ptr<muted_index> _mi;
    std::unique_ptr<group_id_to_topic_id> _group2topic;
    std::unique_ptr<shard_index> _si;

protected:
    random_reassignments _reassignments;
    std::optional<pinning_constraint> _pinning_constr;
    even_topic_distribution_constraint _etdc;
    even_shard_load_constraint _eslc;
    even_node_load_constraint _enlc;
};

/// A greedy random-walk hill-climbing strategy for leader rebalancing.
///
/// On each call to find_movement the strategy draws a random leader
/// reassignment from the full set of possible moves and returns the first one
/// that strictly reduces the combined error across a hierarchy of scores:
///   1. Pinning (leader-preference compliance)
///   2. Shard-level load balance (even topic distribution + even shard load)
///   3. Node-level load balance
/// Each score is only considered if all higher-level scores are not impacted by
/// the move (i.e. changes are within a small jitter threshold).
/// Each reassignment is only considered once.
class random_hill_climbing_strategy final
  : public climbing_strategy_base<random_hill_climbing_strategy> {
    friend base;

public:
    using base::base;

    std::optional<reassignment>
    find_movement(const leader_balancer_types::muted_groups_t& skip) override {
        return do_find_movement_without_score(skip);
    }

private:
    using reassignment_score = std::monostate;

    // returns score if reassignment is acceptable, nullopt otherwise
    std::optional<reassignment_score>
    get_reassignment_score(const reassignment& r) {
        // Hierarchical optimization: first check if the proposed
        // reassignment improves the pinning objective (makes the leaders
        // distribution better conform to the provided pinning
        // configuration). If the pinning objective remains at the same
        // level, check balancing objectives.
        if (_pinning_constr) {
            auto pinning_diff = _pinning_constr->evaluate(r);
            if (pinning_diff < -error_jitter) {
                return std::nullopt;
            } else if (pinning_diff > error_jitter) {
                return std::monostate{};
            }
        }

        auto shard_load_diff = _etdc.evaluate(r) + _eslc.evaluate(r);
        if (shard_load_diff < -error_jitter) {
            return std::nullopt;
        } else if (shard_load_diff > error_jitter) {
            return std::monostate{};
        }

        auto node_load_diff = _enlc.evaluate(r);
        if (node_load_diff < -error_jitter) {
            return std::nullopt;
        } else if (node_load_diff > error_jitter) {
            return std::monostate{};
        }

        return std::nullopt;
    }

    std::optional<reassignment> generate_reassignment() {
        return this->_reassignments.generate_reassignment();
    }
};

/// An adaptive hill-climbing strategy that raises the acceptance bar over
/// time so it preferentially selects high-impact moves. This makes the strategy
/// spend its early iterations on the most impactful transfers and reduce
/// repeated moves.
///
/// It uses the same hierarchical objective evaluation as
/// random_hill_climbing_strategy (pinning > shard load > node load),
/// but runs it in a recalibration loop:
///   1. Sample ~100 random improving moves and record the best score.
///   2. Set a threshold at 50 % of that best score.
///   3. Accept only moves above the threshold until ~100 sub-threshold
///      (but otherwise improving) moves have been rejected, then
///      recalibrate.
class calibrated_hill_climbing_strategy final
  : public climbing_strategy_base<calibrated_hill_climbing_strategy> {
    friend base;

public:
    using base::base;

    /*
     * Find a group reassignment that reduces total error.
     */
    std::optional<reassignment>
    find_movement(const leader_balancer_types::muted_groups_t& skip) override {
        // attempt to find a movement under existing calibration
        if (auto maybe_movement = do_find_movement(skip)) {
            return maybe_movement;
        }

        // nothing found under current calibration, recalibrate
        calibrate(skip);

        // freshly recalibrated, so a not found result will be final
        return do_find_movement(skip);
    }

private:
    struct reassignment_score {
        double pinning_diff;
        double shard_load_diff;
        double node_load_diff;

        std::partial_ordering
        operator<=>(const reassignment_score& other) const noexcept {
            if (
              std::fabs(pinning_diff) > error_jitter
              || std::fabs(other.pinning_diff) > error_jitter) {
                return pinning_diff <=> other.pinning_diff;
            }
            if (
              std::fabs(shard_load_diff) > error_jitter
              || std::fabs(other.shard_load_diff) > error_jitter) {
                return shard_load_diff <=> other.shard_load_diff;
            }
            if (
              std::fabs(node_load_diff) > error_jitter
              || std::fabs(other.node_load_diff) > error_jitter) {
                return node_load_diff <=> other.node_load_diff;
            }
            return std::partial_ordering::equivalent;
        }

        bool operator==(const reassignment_score& other) const& noexcept {
            return (*this <=> other) == std::partial_ordering::equivalent;
        }
    };

    static constexpr reassignment_score worst_score{
      .pinning_diff = std::numeric_limits<double>::lowest(),
      .shard_load_diff = std::numeric_limits<double>::lowest(),
      .node_load_diff = std::numeric_limits<double>::lowest(),
    };

    struct calibration {
        reassignment_score min_acceptable_score;
        size_t remaining_rejections;
    };
    static constexpr calibration allow_all_calibration = {
      .min_acceptable_score = worst_score,
      .remaining_rejections = std::numeric_limits<size_t>::max(),
    };
    static constexpr calibration block_all_calibration = {
      .min_acceptable_score = worst_score,
      .remaining_rejections = 0,
    };

    void calibrate(const leader_balancer_types::muted_groups_t& skip) {
        // take the best of 100 samples, demand at least half that score
        constexpr size_t sample_size = 100;
        constexpr double adjustment_coeff = 0.5;
        // and use this bar till it's a problem for 100 potential moves
        constexpr size_t max_rejections_per_calibration = sample_size;

        // de-calibrate temporarily to sample broadly
        set_calibration(allow_all_calibration);

        reassignment_score best_in_sample = worst_score;
        for (size_t i = 0; i < sample_size; ++i) {
            auto maybe_movement = do_find_movement_with_score(skip);
            if (!maybe_movement) {
                break;
            }
            best_in_sample = std::max(best_in_sample, maybe_movement->score);
        }

        if (best_in_sample == worst_score) {
            set_calibration(block_all_calibration);
        } else {
            reassignment_score adjusted = {
              .pinning_diff = adjustment_coeff * best_in_sample.pinning_diff,
              .shard_load_diff = adjustment_coeff
                                 * best_in_sample.shard_load_diff,
              .node_load_diff = adjustment_coeff
                                * best_in_sample.node_load_diff,
            };
            set_calibration(
              {.min_acceptable_score = adjusted,
               .remaining_rejections = max_rejections_per_calibration});
        }
    }

    void set_calibration(const calibration& c) {
        _calibration = c;
        _reassignments.reset();
    }

    std::optional<reassignment>
    do_find_movement(const leader_balancer_types::muted_groups_t& skip) {
        return do_find_movement_without_score(skip);
    }

    // returns score if reassignment is acceptable, nullopt otherwise
    std::optional<reassignment_score>
    get_reassignment_score(const reassignment& r) {
        const auto& mas = _calibration.min_acceptable_score;

        // A variant of random_hill_climbing_strategy::get_reassignment_score
        // that takes calibration into account. It only decreases remaining
        // attempts when a reassignment would not be viable without calibration.
        if (_pinning_constr) {
            auto pinning_diff = _pinning_constr->evaluate(r);
            if (pinning_diff < -error_jitter) {
                return std::nullopt;
            } else if (
              pinning_diff > error_jitter && pinning_diff > mas.pinning_diff) {
                return {
                  {.pinning_diff = pinning_diff,
                   .shard_load_diff = 0,
                   .node_load_diff = 0}};
            }
        }
        if (mas.pinning_diff > error_jitter) {
            --_calibration.remaining_rejections;
            return std::nullopt;
        }

        auto shard_load_diff = _etdc.evaluate(r) + _eslc.evaluate(r);
        if (shard_load_diff < -error_jitter) {
            return std::nullopt;
        } else if (
          shard_load_diff > error_jitter
          && shard_load_diff > mas.shard_load_diff) {
            return {
              {.pinning_diff = 0,
               .shard_load_diff = shard_load_diff,
               .node_load_diff = 0}};
        }
        if (mas.shard_load_diff > error_jitter) {
            --_calibration.remaining_rejections;
            return std::nullopt;
        }

        auto node_load_diff = _enlc.evaluate(r);
        if (node_load_diff < -error_jitter) {
            return std::nullopt;
        } else if (
          node_load_diff > error_jitter
          && node_load_diff > mas.node_load_diff) {
            return {
              {.pinning_diff = 0,
               .shard_load_diff = 0,
               .node_load_diff = node_load_diff}};
        }
        if (mas.node_load_diff > error_jitter) {
            --_calibration.remaining_rejections;
        }
        return std::nullopt;
    }

    std::optional<reassignment> generate_reassignment() {
        if (_calibration.remaining_rejections == 0) {
            return std::nullopt;
        }
        return _reassignments.generate_reassignment();
    }

    // force recalibration on first run
    calibration _calibration = block_all_calibration;
};

} // namespace cluster::leader_balancer_types
