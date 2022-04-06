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

#pragma once

#include "cluster/types.h"
#include "model/fundamental.h"
#include "vassert.h"

#include <absl/container/node_hash_set.h>

namespace cluster {
class allocation_node;
class allocation_state;

/**
 * Constraints evaluators loosely inspired by Fenzo Constrainst Solver.
 *
 * https://github.com/Netflix/Fenzo/blob/master/fenzo-core/src/main/java/com/netflix/fenzo/ConstraintEvaluator.java
 * https://github.com/Netflix/Fenzo/blob/master/fenzo-core/src/main/java/com/netflix/fenzo/VMTaskFitnessCalculator.java
 *
 */

class hard_constraint_evaluator final {
public:
    struct impl {
        virtual bool evaluate(const allocation_node&) const = 0;
        virtual void print(std::ostream&) const = 0;
        virtual ~impl() = default;
    };

    explicit hard_constraint_evaluator(std::unique_ptr<impl> impl) noexcept
      : _impl(std::move(impl)) {}

    hard_constraint_evaluator(hard_constraint_evaluator&&) noexcept = default;
    hard_constraint_evaluator(const hard_constraint_evaluator&) = delete;

    hard_constraint_evaluator&
    operator=(hard_constraint_evaluator&&) noexcept = default;
    hard_constraint_evaluator&
    operator=(const hard_constraint_evaluator&) noexcept = delete;

    ~hard_constraint_evaluator() noexcept = default;

    bool evaluate(const allocation_node& node) const {
        return _impl->evaluate(node);
    }

private:
    friend std::ostream&
    operator<<(std::ostream& o, const hard_constraint_evaluator& e) {
        e._impl->print(o);
        return o;
    }

    std::unique_ptr<impl> _impl;
};

class soft_constraint_evaluator final {
public:
    static constexpr uint64_t max_score = 10'000'000;
    struct impl {
        virtual uint64_t score(const allocation_node&) const = 0;
        virtual void print(std::ostream&) const = 0;
        virtual ~impl() = default;
    };

    explicit soft_constraint_evaluator(std::unique_ptr<impl> impl) noexcept
      : _impl(std::move(impl)) {}

    soft_constraint_evaluator(soft_constraint_evaluator&&) noexcept = default;
    soft_constraint_evaluator(const soft_constraint_evaluator&) = delete;

    soft_constraint_evaluator&
    operator=(soft_constraint_evaluator&&) noexcept = default;
    soft_constraint_evaluator&
    operator=(const soft_constraint_evaluator&) noexcept = delete;

    ~soft_constraint_evaluator() noexcept = default;

    uint64_t score(const allocation_node& node) const {
        auto ret = _impl->score(node);
        vassert(
          ret <= max_score,
          "Score returned from soft constraint evaluator must be in range of "
          "[0, 10'000'000]. Returned score: {}",
          ret);
        return ret;
    };

private:
    friend std::ostream&
    operator<<(std::ostream& o, const soft_constraint_evaluator& e) {
        e._impl->print(o);
        return o;
    }

    std::unique_ptr<impl> _impl;
};

/**
 * Configuration used to request partition allocation, if current allocations
 * are not empty then allocation strategy will allocate as many replis as
 * required to achieve requested replication factor.
 */
struct allocation_constraints {
    // we store pointers in here to make allocation constraints copyable
    using soft_constraint_ev_ptr = ss::lw_shared_ptr<soft_constraint_evaluator>;
    using hard_constraint_ev_ptr = ss::lw_shared_ptr<hard_constraint_evaluator>;

    std::vector<soft_constraint_ev_ptr> soft_constraints;
    std::vector<hard_constraint_ev_ptr> hard_constraints;

    void add(allocation_constraints);
    friend std::ostream&
    operator<<(std::ostream&, const allocation_constraints&);
};
/**
 * RAII based helper holding allocated partititions, allocation is reverted
 * after this object goes out of scope.
 */
struct allocation_units {
    allocation_units(std::vector<partition_assignment>, allocation_state*);
    allocation_units(
      std::vector<partition_assignment>,
      std::vector<model::broker_shard>,
      allocation_state*);
    allocation_units& operator=(allocation_units&&) = default;
    allocation_units& operator=(const allocation_units&) = delete;
    allocation_units(const allocation_units&) = delete;
    allocation_units(allocation_units&&) = default;

    ~allocation_units();

    const std::vector<partition_assignment>& get_assignments() const {
        return _assignments;
    }

private:
    std::vector<partition_assignment> _assignments;
    // set of previous replicas, they will not be reverted when allocation units
    // goes out of scope
    absl::node_hash_set<model::broker_shard> _previous;
    // keep the pointer to make this type movable
    allocation_state* _state;
};

/**
 * Configuration used to request manual allocation configuration for topic.
 * Custom allocation only designate nodes where partition should be placed but
 * not the shards on each node, allocation strategy will assing shards to each
 * replica
 */
struct partition_constraints {
    partition_constraints(model::partition_id, uint16_t);

    partition_constraints(
      model::partition_id, uint16_t, allocation_constraints);

    model::partition_id partition_id;
    uint16_t replication_factor;
    allocation_constraints constraints;
    friend std::ostream&
    operator<<(std::ostream&, const partition_constraints&);
};

struct allocation_request {
    allocation_request() = default;
    allocation_request(const allocation_request&) = delete;
    allocation_request(allocation_request&&) = default;
    allocation_request& operator=(const allocation_request&) = delete;
    allocation_request& operator=(allocation_request&&) = default;
    ~allocation_request() = default;

    std::vector<partition_constraints> partitions;

    friend std::ostream& operator<<(std::ostream&, const allocation_request&);
};

} // namespace cluster
