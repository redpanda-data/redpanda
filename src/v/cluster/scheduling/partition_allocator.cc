// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/scheduling/partition_allocator.h"

#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/scheduling/constraints.h"
#include "cluster/scheduling/types.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "model/metadata.h"
#include "storage/segment_appender.h"
#include "units.h"
#include "utils/human.h"

#include <seastar/core/shared_ptr.hh>

#include <absl/container/node_hash_set.h>
#include <sys/resource.h>

#include <algorithm>
#include <exception>
#include <iterator>
#include <vector>

namespace cluster {

partition_allocator::partition_allocator(
  ss::sharded<members_table>& members,
  config::binding<std::optional<size_t>> memory_per_partition,
  config::binding<std::optional<int32_t>> fds_per_partition,
  config::binding<size_t> fallocation_step)
  : _state(std::make_unique<allocation_state>())
  , _allocation_strategy(simple_allocation_strategy())
  , _members(members)
  , _memory_per_partition(memory_per_partition)
  , _fds_per_partition(fds_per_partition)
  , _fallocation_step(fallocation_step) {}

allocation_constraints default_constraints() {
    allocation_constraints req;
    req.hard_constraints.push_back(
      ss::make_lw_shared<hard_constraint_evaluator>(not_fully_allocated()));
    req.hard_constraints.push_back(
      ss::make_lw_shared<hard_constraint_evaluator>(is_active()));
    req.soft_constraints.push_back(
      ss::make_lw_shared<soft_constraint_evaluator>(least_allocated()));
    return req;
}

result<std::vector<model::broker_shard>>
partition_allocator::allocate_partition(partition_constraints p_constraints) {
    vlog(
      clusterlog.trace,
      "allocating partition with constraints: {}",
      p_constraints);
    if (
      p_constraints.replication_factor <= 0
      || _state->available_nodes() < p_constraints.replication_factor) {
        return errc::topic_invalid_replication_factor;
    }

    intermediate_allocation<model::broker_shard> replicas(
      *_state, p_constraints.replication_factor);

    for (auto r = 0; r < p_constraints.replication_factor; ++r) {
        auto effective_constraits = default_constraints();
        effective_constraits.hard_constraints.push_back(
          ss::make_lw_shared<hard_constraint_evaluator>(
            distinct_from(replicas.get())));

        // rack-placement contraint
        effective_constraits.soft_constraints.push_back(
          ss::make_lw_shared<soft_constraint_evaluator>(
            distinct_rack(replicas.get(), *_state)));

        effective_constraits.add(p_constraints.constraints);
        auto replica = _allocation_strategy.allocate_replica(
          effective_constraits, *_state);

        if (!replica) {
            return replica.error();
        }
        replicas.push_back(replica.value());
    }

    return std::move(replicas).finish();
}

/**
 * Check cluster-wide limits on total partition count vs available
 * system resources.  This is the 'sanity' check that the user doesn't
 * try and create a million partitions with only 8192 file handles, etc.
 * Without this check, a partition creation command could make a redpanda
 * cluster unavailable by exhausting available resources.
 *
 * - These limits are heuristic, and are configurable/disable-able via
 * configuration properties.  That's different to the limits we enforce per-node
 * deeper inside the allocator, which are concrete limits that may not be
 * exceeded.
 *
 * - We intentionally do this coarse global check rather than trying to
 * carefully fit partitions into any available disk space etc, to avoid getting
 * into situations where the partitions *only just* fit.  Partitions
 * need to fit into the worst case node size, worst case node memory,
 * to enable migrating partitions around during failures/rebalance/
 * decommissions.
 *
 * - These heuristic limits are not correct for heterogeneous clusters
 * in general: we behave as if all nodes are as small as
 * the smallest node.  A more general set of per-node limits may
 * be added in future, as/when we can handle heterogeneous edge
 * cases more cleanly (like decommissioning a large node and dealing
 * with partitions that cannot be re-accommodated on smaller peers).
 */
std::error_code partition_allocator::check_cluster_limits(
  allocation_request const& request) const {
    if (_members.local().all_brokers().empty()) {
        // Empty members table, we're probably running in a unit test
        return errc::success;
    }
    // Calculate how many partition-replicas already exist, so that we can
    // check if the new topic would take us past any limits.
    uint64_t existent_partitions{0};
    for (const auto& i : _state->allocation_nodes()) {
        existent_partitions += uint64_t(i.second->allocated_partitions());
    }

    // Partition-replicas requested
    uint64_t create_count{0};
    for (const auto& i : request.partitions) {
        create_count += uint64_t(i.replication_factor);
    }

    uint64_t proposed_total_partitions = existent_partitions + create_count;

    // Gather information about system-wide resource sizes
    uint32_t min_core_count = 0;
    uint64_t min_memory_bytes = 0;
    uint64_t min_disk_bytes = 0;
    auto all_brokers = _members.local().all_brokers();
    for (const auto& b : all_brokers) {
        if (min_core_count == 0) {
            min_core_count = b->properties().cores;
        } else {
            min_core_count = std::min(min_core_count, b->properties().cores);
        }

        // In redpanda <= 21.11.x, available_memory_gb and available_disk_gb
        // are not populated.  If they're zero we skip the check later.
        if (min_memory_bytes == 0) {
            min_memory_bytes = b->properties().available_memory_gb * 1_GiB;
        } else if (b->properties().available_memory_gb > 0) {
            min_memory_bytes = std::min(
              min_memory_bytes, b->properties().available_memory_gb * 1_GiB);
        }

        if (min_disk_bytes == 0) {
            min_disk_bytes = b->properties().available_disk_gb * 1_GiB;
        } else if (b->properties().available_disk_gb > 0) {
            min_disk_bytes = std::min(
              min_disk_bytes, b->properties().available_disk_gb * 1_GiB);
        }
    }

    // The effective values are the node count times the smallest node's
    // resources: this avoids wrongly assuming the system will handle partition
    // counts that only fit when scheduled onto certain nodes.
    uint64_t effective_cpu_count = all_brokers.size() * min_core_count;
    uint64_t effective_cluster_memory = all_brokers.size() * min_memory_bytes;
    uint64_t effective_cluster_disk = all_brokers.size() * min_disk_bytes;

    vlog(
      clusterlog.debug,
      "Effective cluster resources: CPU={} memory={} disk={}",
      effective_cpu_count,
      human::bytes(effective_cluster_memory),
      human::bytes(effective_cluster_disk));

    // Refuse to create a partition count that would violate the per-core
    // limit.
    const uint64_t core_limit = effective_cpu_count
                                * allocation_node::max_allocations_per_core;
    if (proposed_total_partitions > core_limit) {
        vlog(
          clusterlog.warn,
          "Refusing to create {} partitions, exceeds core limit {}",
          create_count,
          effective_cpu_count * allocation_node::max_allocations_per_core);
        return errc::topic_invalid_partitions;
    }

    // Refuse to create partitions that would violate the configured
    // memory per partition.
    auto memory_per_partition_replica = _memory_per_partition();
    if (
      memory_per_partition_replica.has_value()
      && memory_per_partition_replica.value() > 0) {
        const uint64_t memory_limit = effective_cluster_memory
                                      / memory_per_partition_replica.value();

        if (memory_limit > 0 && proposed_total_partitions > memory_limit) {
            vlog(
              clusterlog.warn,
              "Refusing to create {} partitions, exceeds memory limit {}",
              create_count,
              memory_limit);
            return errc::topic_invalid_partitions;
        }
    }

    // Refuse to create partitions that would exhaust our nfiles ulimit
    auto fds_per_partition_replica = _fds_per_partition();
    if (
      fds_per_partition_replica.has_value()
      && fds_per_partition_replica.value() > 0) {
        struct rlimit nofile = {0, 0};
        if (getrlimit(RLIMIT_NOFILE, &nofile) == 0) {
            if (nofile.rlim_cur != RLIM_INFINITY) {
                const uint64_t fds_limit = (all_brokers.size()
                                            * nofile.rlim_cur)
                                           / fds_per_partition_replica.value();
                if (proposed_total_partitions > fds_limit) {
                    vlog(
                      clusterlog.warn,
                      "Refusing to create {} partitions, exceeds FD limit {}",
                      create_count,
                      fds_limit);
                    return errc::topic_invalid_partitions;
                }
            }
        } else {
            // Unclear if this can ever happen, but let's be graceful.
            vlog(clusterlog.warn, "Error {} querying file handle limit", errno);
        }
    }

    // Refuse to create partitions if there isn't enough space to at least
    // falloc the first part of a segment for each partition
    if (_fallocation_step() > 0) {
        uint64_t disk_limit = effective_cluster_disk / _fallocation_step();
        if (disk_limit > 0 && (proposed_total_partitions > disk_limit)) {
            vlog(
              clusterlog.warn,
              "Refusing to create {} partitions, exceeds disk limit {}",
              create_count,
              disk_limit);
            return errc::topic_invalid_partitions;
        }
    }

    return errc::success;
}

result<allocation_units>
partition_allocator::allocate(allocation_request request) {
    vlog(
      clusterlog.trace,
      "allocation request for {} partitions",
      request.partitions.size());

    auto cluster_errc = check_cluster_limits(request);
    if (cluster_errc) {
        return cluster_errc;
    }

    intermediate_allocation<partition_assignment> assignments(
      *_state, request.partitions.size());

    for (auto& p_constraints : request.partitions) {
        auto const partition_id = p_constraints.partition_id;
        auto replicas = allocate_partition(std::move(p_constraints));
        if (!replicas) {
            return replicas.error();
        }
        assignments.push_back(partition_assignment{
          .group = _state->next_group_id(),
          .id = partition_id,
          .replicas = std::move(replicas.value()),
        });
    }

    return allocation_units(std::move(assignments).finish(), _state.get());
}

result<std::vector<model::broker_shard>>
partition_allocator::do_reallocate_partition(
  partition_constraints p_constraints,
  const std::vector<model::broker_shard>& not_changed_replicas) {
    vlog(
      clusterlog.debug,
      "reallocating {}, replicas left: {}",
      p_constraints,
      not_changed_replicas);
    /**
     * We do not have to reallocate any of the replicas, do nothing
     */
    if (p_constraints.replication_factor == not_changed_replicas.size()) {
        return not_changed_replicas;
    }
    p_constraints.constraints.hard_constraints.push_back(
      ss::make_lw_shared<hard_constraint_evaluator>(
        distinct_from(not_changed_replicas)));
    p_constraints.replication_factor -= not_changed_replicas.size();
    auto result = allocate_partition(std::move(p_constraints));
    if (!result) {
        return result.error();
    }
    auto new_replicas = std::move(result.value());
    std::move(
      not_changed_replicas.begin(),
      not_changed_replicas.end(),
      std::back_inserter(new_replicas));

    return new_replicas;
}

result<allocation_units> partition_allocator::reallocate_partition(
  partition_constraints partition_constraints,
  const partition_assignment& current_assignment) {
    auto replicas = do_reallocate_partition(
      std::move(partition_constraints), current_assignment.replicas);

    if (!replicas) {
        return replicas.error();
    }

    partition_assignment assignment{
      .group = current_assignment.group,
      .id = current_assignment.id,
      .replicas = std::move(replicas.value()),
    };

    return allocation_units(
      {std::move(assignment)}, current_assignment.replicas, _state.get());
}

result<allocation_units> partition_allocator::reassign_decommissioned_replicas(
  const partition_assignment& current_assignment) {
    uint16_t replication_factor = current_assignment.replicas.size();
    auto current_replicas = current_assignment.replicas;
    std::erase_if(current_replicas, [this](const model::broker_shard& bs) {
        auto it = _state->allocation_nodes().find(bs.node_id);
        return it == _state->allocation_nodes().end()
               || it->second->is_decommissioned();
    });

    auto req = default_constraints();
    req.hard_constraints.push_back(
      ss::make_lw_shared<hard_constraint_evaluator>(
        distinct_from(current_replicas)));

    auto replicas = do_reallocate_partition(
      partition_constraints(current_assignment.id, replication_factor),
      current_replicas);

    if (!replicas) {
        return replicas.error();
    }

    partition_assignment assignment{
      .group = current_assignment.group,
      .id = current_assignment.id,
      .replicas = std::move(replicas.value()),
    };

    return allocation_units(
      {std::move(assignment)}, current_replicas, _state.get());
}

void partition_allocator::deallocate(
  const std::vector<model::broker_shard>& replicas) {
    for (auto& r : replicas) {
        // find in brokers
        _state->deallocate(r);
    }
}

void partition_allocator::update_allocation_state(
  const std::vector<model::broker_shard>& shards, raft::group_id gid) {
    if (shards.empty()) {
        return;
    }

    _state->apply_update(shards, gid);
}

void partition_allocator::update_allocation_state(
  const std::vector<model::broker_shard>& current,
  const std::vector<model::broker_shard>& previous) {
    std::vector<model::broker_shard> to_add;
    std::vector<model::broker_shard> to_remove;

    std::copy_if(
      current.begin(),
      current.end(),
      std::back_inserter(to_add),
      [&previous](const model::broker_shard& current_bs) {
          auto it = std::find(previous.begin(), previous.end(), current_bs);
          return it == previous.end();
      });

    std::copy_if(
      previous.begin(),
      previous.end(),
      std::back_inserter(to_remove),
      [&current](const model::broker_shard& prev_bs) {
          auto it = std::find(current.begin(), current.end(), prev_bs);
          return it == current.end();
      });

    _state->apply_update(to_add, raft::group_id{});
    for (const auto& bs : to_remove) {
        _state->deallocate(bs);
    }
}

} // namespace cluster
