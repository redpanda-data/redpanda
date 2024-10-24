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

#include "base/oncore.h"
#include "base/vassert.h"
#include "cluster/types.h"
#include "model/fundamental.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/weak_ptr.hh>
#include <seastar/util/noncopyable_function.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_set.h>

namespace cluster {
class allocation_node;
class allocation_state;

/**
 * Constraints evaluators loosely inspired by Fenzo Constraints Solver.
 *
 * https://github.com/Netflix/Fenzo/blob/master/fenzo-core/src/main/java/com/netflix/fenzo/ConstraintEvaluator.java
 * https://github.com/Netflix/Fenzo/blob/master/fenzo-core/src/main/java/com/netflix/fenzo/VMTaskFitnessCalculator.java
 *
 */
using replicas_t = std::vector<model::broker_shard>;
using hard_constraint_evaluator
  = ss::noncopyable_function<bool(const allocation_node&)>;

using soft_constraint_evaluator
  = ss::noncopyable_function<uint64_t(const allocation_node&)>;

class allocated_partition;

class hard_constraint {
public:
    struct impl {
        virtual hard_constraint_evaluator make_evaluator(
          const allocated_partition&, std::optional<model::node_id> prev) const
          = 0;

        virtual ss::sstring name() const = 0;
        virtual ~impl() = default;
    };

    explicit hard_constraint(std::unique_ptr<impl> impl) noexcept
      : _impl(std::move(impl)) {}

    hard_constraint(hard_constraint&&) noexcept = default;
    hard_constraint(const hard_constraint&) = delete;

    hard_constraint& operator=(hard_constraint&&) noexcept = default;
    hard_constraint& operator=(const hard_constraint&) noexcept = delete;

    ~hard_constraint() noexcept = default;

    hard_constraint_evaluator make_evaluator(
      const allocated_partition& partition,
      std::optional<model::node_id> prev) const {
        return _impl->make_evaluator(partition, prev);
    }

    ss::sstring name() const { return _impl->name(); }

private:
    friend std::ostream& operator<<(std::ostream& o, const hard_constraint& c) {
        fmt::print(o, "hard: [{}]", c.name());
        return o;
    }
    std::unique_ptr<impl> _impl;
};

class soft_constraint final {
public:
    static constexpr uint64_t max_score = 10'000'000;
    struct impl {
        virtual soft_constraint_evaluator make_evaluator(
          const allocated_partition& partition,
          std::optional<model::node_id> prev) const
          = 0;
        virtual ss::sstring name() const = 0;
        virtual ~impl() = default;
    };

    explicit soft_constraint(std::unique_ptr<impl> impl) noexcept
      : _impl(std::move(impl)) {}

    soft_constraint(soft_constraint&&) noexcept = default;
    soft_constraint(const soft_constraint&) = delete;

    soft_constraint& operator=(soft_constraint&&) noexcept = default;
    soft_constraint& operator=(const soft_constraint&) noexcept = delete;

    ~soft_constraint() noexcept = default;

    soft_constraint_evaluator make_evaluator(
      const allocated_partition& partition,
      std::optional<model::node_id> prev) const {
        return _impl->make_evaluator(partition, prev);
    }

    ss::sstring name() const { return _impl->name(); }

private:
    friend std::ostream& operator<<(std::ostream& o, const soft_constraint& c) {
        fmt::print(o, "soft: [{}]", c.name());
        return o;
    }

    std::unique_ptr<impl> _impl;
};

/**
 * Configuration used to request partition allocation, if current allocations
 * are not empty then allocation strategy will allocate as many replicas as
 * required to achieve requested replication factor.
 *
 * Allocation constraints define a constraints hierarchy in which a root level
 * is formed by hard constraints vector. Soft constraints hierarchy is expressed
 * as a list of list of constraints. Where the top level list defines a
 * constraints hierarchy while from most important to the least important
 * constraints while the sublist contain constraints with the same priority.
 */
// we store pointers in here to make allocation constraints copyable
using soft_constraint_ptr = ss::lw_shared_ptr<soft_constraint>;
using hard_constraint_ptr = ss::lw_shared_ptr<hard_constraint>;
using soft_constraints_level = std::vector<soft_constraint_ptr>;
using soft_constraints_hierarchy = std::vector<soft_constraints_level>;
struct allocation_constraints {
    /**
     * Hard constraints define a root level of constraints hierarchy
     */
    std::vector<hard_constraint_ptr> hard_constraints;

    /**
     * Each list of a constraints vector contains a level of soft constraints
     */
    soft_constraints_hierarchy soft_constraints;

    /**
     * Add constraints to the last hierarchy level (for backward compatibility)
     */
    void add(soft_constraint c) {
        if (soft_constraints.empty()) {
            soft_constraints.push_back({});
        }
        return soft_constraints.back().push_back(
          ss::make_lw_shared<soft_constraint>(std::move(c)));
    }

    void add_level(soft_constraints_level c) {
        return soft_constraints.push_back(std::move(c));
    }

    void ensure_new_level() {
        if (!soft_constraints.empty() && !soft_constraints.back().empty()) {
            soft_constraints.emplace_back();
        }
    }

    void add(hard_constraint c) {
        return hard_constraints.push_back(
          ss::make_lw_shared<hard_constraint>(std::move(c)));
    }

    void add(allocation_constraints);
    friend std::ostream&
    operator<<(std::ostream&, const allocation_constraints&);
};
/**
 * RAII based helper holding allocated partitions, allocation is reverted
 * after this object goes out of scope.
 *
 * WARNING: this object contains an embedded reference to the partition
 * allocator service (specifically, the allocation_state associated with that
 * allocator) and so it must be destroyed on the same shard the original units
 * were allocated on. I.e., if original units A are moved into B, B must be
 * destroyed the shard A was allocated on (or the shard of the object that was
 * moved into A and so on).
 */
struct allocation_units {
    /**
     * A foreign unique pointer to some units. Given the warning above about
     * cross-core destruction, it is best to use this pointer class when dealing
     * with units allocated on another core.
     */
    using pointer = ss::foreign_ptr<std::unique_ptr<allocation_units>>;

    allocation_units& operator=(allocation_units&&) = default;
    allocation_units& operator=(const allocation_units&) = delete;
    allocation_units(const allocation_units&) = delete;
    allocation_units(allocation_units&&) = default;

    ~allocation_units();

    const ss::chunked_fifo<partition_assignment>& get_assignments() const {
        return _assignments;
    }

    ss::chunked_fifo<partition_assignment> copy_assignments() {
        ss::chunked_fifo<partition_assignment> p_as;
        p_as.reserve(_assignments.size());
        std::copy(
          _assignments.begin(), _assignments.end(), std::back_inserter(p_as));
        return p_as;
    }

private:
    friend class partition_allocator;
    allocation_units(allocation_state&);

private:
    ss::chunked_fifo<partition_assignment> _assignments;
    chunked_vector<model::broker_shard> _added_replicas;
    // keep the pointer to make this type movable
    ss::weak_ptr<allocation_state> _state;
    // oncore checker to ensure destruction happens on the same core
    [[no_unique_address]] oncore _oncore;
};

/// Result of reallocating a single replica. Can be used for reverts.
class reallocation_step {
public:
    const model::broker_shard& current() const { return _current; }

    // nullopt if new replica
    const std::optional<model::broker_shard>& previous() const {
        return _previous;
    }

private:
    friend class partition_allocator;
    reallocation_step(
      model::broker_shard current, std::optional<model::broker_shard> previous)
      : _current(current)
      , _previous(previous) {}

    model::broker_shard _current;
    std::optional<model::broker_shard> _previous;
};

/// RAII helper for incremental partition (re)allocation.
///
/// Note: shard ids for original replicas are preserved.
class allocated_partition {
public:
    const replicas_t& replicas() const { return _replicas; }

    const model::ntp& ntp() const { return _ntp; }

    bool has_changes() const;
    bool is_original(model::node_id) const;

    // reverting the last step is always possible
    errc try_revert(const reallocation_step&);

    allocated_partition& operator=(allocated_partition&&) = default;
    allocated_partition& operator=(const allocated_partition&) = delete;
    allocated_partition(const allocated_partition&) = delete;
    allocated_partition(allocated_partition&&) = default;
    ~allocated_partition();

private:
    friend class partition_allocator;

    // construct an object from an original assignment
    allocated_partition(
      model::ntp, std::vector<model::broker_shard>, allocation_state&);

    struct previous_replica {
        model::broker_shard bs;
        size_t idx;
    };

    std::optional<previous_replica> prepare_move(model::node_id previous) const;
    model::broker_shard
    add_replica(model::node_id, const std::optional<previous_replica>&);

    // used to move the allocation to allocation_units
    replicas_t
    release_new_partition(chunked_vector<model::broker_shard>& added_replicas);

private:
    model::ntp _ntp;
    replicas_t _replicas;
    std::optional<absl::flat_hash_map<model::node_id, uint32_t>>
      _original_node2shard;
    ss::weak_ptr<allocation_state> _state;
    // oncore checker to ensure destruction happens on the same core
    [[no_unique_address]] oncore _oncore;
};

/**
 * Configuration used to request manual allocation configuration for topic.
 * Custom allocation only designate nodes where partition should be placed but
 * not the shards on each node, allocation strategy will assign shards to each
 * replica
 */
struct partition_constraints {
    // Constructor for allocating a new partition
    partition_constraints(
      model::partition_id id,
      uint16_t rf,
      allocation_constraints constraints = {})
      : partition_id(id)
      , replication_factor(rf)
      , constraints(std::move(constraints)) {}

    // Constructor for reallocating an partition
    partition_constraints(
      const partition_assignment& assignment,
      uint16_t rf,
      allocation_constraints constraints = {})
      : partition_id(assignment.id)
      , replication_factor(rf)
      , constraints(std::move(constraints))
      , existing_group(assignment.group)
      , existing_replicas(assignment.replicas) {}

    model::partition_id partition_id;
    uint16_t replication_factor;
    allocation_constraints constraints;
    std::optional<raft::group_id> existing_group;
    replicas_t existing_replicas;

    friend std::ostream&
    operator<<(std::ostream&, const partition_constraints&);
};

using node2count_t = absl::flat_hash_map<model::node_id, size_t>;

struct allocation_request {
    allocation_request() = delete;
    explicit allocation_request(model::topic_namespace nt)
      : _nt(std::move(nt)) {}
    allocation_request(const allocation_request&) = delete;
    allocation_request(allocation_request&&) = default;
    allocation_request& operator=(const allocation_request&) = delete;
    allocation_request& operator=(allocation_request&&) = default;
    ~allocation_request() = default;

    model::topic_namespace _nt;
    ss::chunked_fifo<partition_constraints> partitions;
    // if present, new partitions will be allocated using topic-aware counts
    // objective.
    std::optional<node2count_t> existing_replica_counts;

    friend std::ostream& operator<<(std::ostream&, const allocation_request&);
};

} // namespace cluster
