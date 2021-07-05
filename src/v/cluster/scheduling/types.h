/*
 * Copyright 2020 Vectorized, Inc.
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

namespace cluster {
class allocation_node;

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
} // namespace cluster
