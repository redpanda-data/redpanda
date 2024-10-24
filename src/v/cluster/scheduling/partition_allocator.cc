// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/scheduling/partition_allocator.h"

#include "base/units.h"
#include "cluster/cluster_utils.h"
#include "cluster/controller_snapshot.h"
#include "cluster/logger.h"
#include "cluster/members_table.h"
#include "cluster/scheduling/constraints.h"
#include "cluster/scheduling/types.h"
#include "cluster/types.h"
#include "config/configuration.h"
#include "features/feature_table.h"
#include "model/metadata.h"
#include "random/generators.h"
#include "ssx/async_algorithm.h"
#include "utils/human.h"

#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/coroutine/maybe_yield.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/later.hh>

#include <absl/container/node_hash_set.h>
#include <sys/resource.h>

#include <algorithm>
#include <exception>
#include <iterator>
#include <memory>
#include <vector>

namespace cluster {

partition_allocator::partition_allocator(
  ss::sharded<members_table>& members,
  ss::sharded<features::feature_table>& feature_table,
  config::binding<std::optional<size_t>> memory_per_partition,
  config::binding<std::optional<int32_t>> fds_per_partition,
  config::binding<uint32_t> partitions_per_shard,
  config::binding<uint32_t> partitions_reserve_shard0,
  config::binding<std::vector<ss::sstring>> internal_kafka_topics,
  config::binding<bool> enable_rack_awareness)
  : _state(std::make_unique<allocation_state>(
      feature_table.local(),
      partitions_per_shard,
      partitions_reserve_shard0,
      internal_kafka_topics))
  , _members(members)
  , _feature_table(feature_table.local())
  , _memory_per_partition(std::move(memory_per_partition))
  , _fds_per_partition(std::move(fds_per_partition))
  , _partitions_per_shard(std::move(partitions_per_shard))
  , _partitions_reserve_shard0(std::move(partitions_reserve_shard0))
  , _internal_kafka_topics(std::move(internal_kafka_topics))
  , _enable_rack_awareness(std::move(enable_rack_awareness)) {}

allocation_constraints partition_allocator::default_constraints() {
    allocation_constraints req;

    // hard constraints
    req.add(distinct_nodes());
    req.add(not_fully_allocated());
    req.add(is_active());

    // soft constraints
    if (_enable_rack_awareness()) {
        req.add(distinct_rack_preferred(_members.local()));
    }

    return req;
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
  const allocation_request& request) const {
    if (_members.local().nodes().empty()) {
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
        if (i.replication_factor > i.existing_replicas.size()) {
            create_count += uint64_t(
              i.replication_factor - i.existing_replicas.size());
        }
    }

    uint64_t proposed_total_partitions = existent_partitions + create_count;

    // Gather information about system-wide resource sizes
    uint32_t min_core_count = 0;
    uint64_t min_memory_bytes = 0;
    uint64_t min_disk_bytes = 0;

    for (const auto& [id, b] : _members.local().nodes()) {
        auto& b_properties = b.broker.properties();
        if (min_core_count == 0) {
            min_core_count = b_properties.cores;
        } else {
            min_core_count = std::min(min_core_count, b_properties.cores);
        }

        if (min_memory_bytes == 0) {
            min_memory_bytes = b.broker.memory_bytes();
        } else {
            min_memory_bytes = std::min(
              min_memory_bytes, b.broker.memory_bytes());
        }

        if (min_disk_bytes == 0) {
            min_disk_bytes = b_properties.available_disk_gb * 1_GiB;
        } else if (b_properties.available_disk_gb > 0) {
            min_disk_bytes = std::min(
              min_disk_bytes, b_properties.available_disk_gb * 1_GiB);
        }
    }

    // The effective values are the node count times the smallest node's
    // resources: this avoids wrongly assuming the system will handle partition
    // counts that only fit when scheduled onto certain nodes.
    auto broker_count = _members.local().node_count();
    uint64_t effective_cpu_count = broker_count * min_core_count;
    uint64_t effective_cluster_memory = broker_count * min_memory_bytes;
    uint64_t effective_cluster_disk = broker_count * min_disk_bytes;

    vlog(
      clusterlog.debug,
      "Effective cluster resources: CPU={} memory={} disk={}",
      effective_cpu_count,
      human::bytes(effective_cluster_memory),
      human::bytes(effective_cluster_disk));

    // Refuse to create a partition count that would violate the per-core
    // limit.
    const uint64_t core_limit = (effective_cpu_count * _partitions_per_shard());
    if (proposed_total_partitions > core_limit) {
        vlog(
          clusterlog.warn,
          "Refusing to create {} partitions as total partition count {} would "
          "exceed core limit {}",
          create_count,
          proposed_total_partitions,
          effective_cpu_count * _partitions_per_shard());
        return errc::topic_invalid_partitions_core_limit;
    }

    // Refuse to create partitions that would violate the configured
    // memory per partition.
    auto memory_per_partition_replica = _memory_per_partition();
    if (
      memory_per_partition_replica.has_value()
      && memory_per_partition_replica.value() > 0) {
        const uint64_t memory_limit = effective_cluster_memory
                                      / memory_per_partition_replica.value();

        if (proposed_total_partitions > memory_limit) {
            vlog(
              clusterlog.warn,
              "Refusing to create {} new partitions as total partition count "
              "{} "
              "would exceed memory limit {}",
              create_count,
              proposed_total_partitions,
              memory_limit);
            return errc::topic_invalid_partitions_memory_limit;
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
                const uint64_t fds_limit = (broker_count * nofile.rlim_cur)
                                           / fds_per_partition_replica.value();
                if (proposed_total_partitions > fds_limit) {
                    vlog(
                      clusterlog.warn,
                      "Refusing to create {} partitions as total partition "
                      "count {} would exceed FD limit {}",
                      create_count,
                      proposed_total_partitions,
                      fds_limit);
                    return errc::topic_invalid_partitions_fd_limit;
                }
            }
        } else {
            // Unclear if this can ever happen, but let's be graceful.
            vlog(clusterlog.warn, "Error {} querying file handle limit", errno);
        }
    }

    return errc::success;
}

ss::future<result<allocation_units::pointer>>
partition_allocator::allocate(allocation_request request) {
    vlog(
      clusterlog.trace,
      "allocation request for {} partitions",
      request.partitions.size());

    auto cluster_errc = check_cluster_limits(request);
    if (cluster_errc) {
        co_return cluster_errc;
    }

    std::optional<node2count_t> node2count;
    if (request.existing_replica_counts) {
        node2count = std::move(*request.existing_replica_counts);
    }

    const auto& nt = request._nt;

    struct allocation_info {
        allocated_partition partition;
        allocation_constraints constraints;
        size_t replication_factor = 0;
        std::optional<raft::group_id> existing_group;
    };

    chunked_vector<allocation_info> allocations;
    allocations.reserve(request.partitions.size());
    for (auto& p_constraints : request.partitions) {
        int16_t num_new_replicas = p_constraints.replication_factor
                                   - p_constraints.existing_replicas.size();
        if (
          _state->available_nodes() < p_constraints.replication_factor
          || num_new_replicas < 0) {
            co_return errc::topic_invalid_replication_factor;
        }

        auto effective_constraints = default_constraints();
        if (node2count) {
            effective_constraints.ensure_new_level();
            effective_constraints.add(
              min_count_in_map("min topic-wise count", *node2count));
        }
        effective_constraints.ensure_new_level();
        effective_constraints.add(max_final_capacity());
        effective_constraints.add(p_constraints.constraints);

        auto ntp = model::ntp(nt.ns, nt.tp, p_constraints.partition_id);

        vlog(
          clusterlog.trace,
          "allocating new partition {} with constraints: {}",
          ntp,
          p_constraints);

        allocations.push_back(allocation_info{
          .partition = allocated_partition(
            std::move(ntp),
            std::move(p_constraints.existing_replicas),
            *_state),
          .constraints = std::move(effective_constraints),
          .replication_factor = p_constraints.replication_factor,
          .existing_group = p_constraints.existing_group,
        });

        co_await ss::coroutine::maybe_yield();
    }

    // Allocate replicas in random order (i.e. not all replicas for a partition
    // at once) to prevent formation of replica clusters - an undesireable
    // allocation pattern where many partitions have the exact same replica set.

    chunked_vector<size_t> in_progress;
    in_progress.reserve(allocations.size());
    for (size_t i = 0; i < allocations.size(); ++i) {
        if (
          allocations[i].replication_factor
          > allocations[i].partition.replicas().size()) {
            in_progress.push_back(i);
        }
    }

    while (!in_progress.empty()) {
        size_t idx = random_generators::get_int(in_progress.size() - 1);
        auto& allocation = allocations[in_progress[idx]];
        auto replica = do_allocate_replica(
          allocation.partition, std::nullopt, allocation.constraints);
        if (!replica) {
            co_return replica.error();
        }

        if (node2count) {
            (*node2count)[replica.value().current().node_id] += 1;
        }

        if (
          allocation.partition.replicas().size()
          == allocation.replication_factor) {
            std::swap(in_progress[idx], in_progress.back());
            in_progress.pop_back();
        }

        co_await ss::coroutine::maybe_yield();
    }

    allocation_units units(*_state);
    units._assignments.reserve(allocations.size());
    co_await ssx::async_for_each(
      allocations.begin(), allocations.end(), [&](allocation_info& p) {
          raft::group_id group = p.existing_group ? *p.existing_group
                                                  : _state->next_group_id();
          units._assignments.emplace_back(
            group,
            p.partition.ntp().tp.partition,
            p.partition.release_new_partition(units._added_replicas));
      });

    co_return ss::make_foreign(
      std::make_unique<allocation_units>(std::move(units)));
}

result<allocated_partition> partition_allocator::reallocate_partition(
  model::ntp ntp,
  std::vector<model::broker_shard> current_replicas,
  const std::vector<model::node_id>& replicas_to_reallocate,
  allocation_constraints constraints,
  node2count_t* existing_replica_counts) {
    vlog(
      clusterlog.debug,
      "reallocating {}, current replicas: {}, to move: {}",
      ntp,
      current_replicas,
      replicas_to_reallocate);

    allocated_partition res{
      std::move(ntp), std::move(current_replicas), *_state};

    if (replicas_to_reallocate.empty()) {
        // nothing to do
        return res;
    }

    std::optional<node2count_t> node2count;
    if (existing_replica_counts) {
        node2count = *existing_replica_counts;
    }

    auto effective_constraints = default_constraints();
    if (node2count) {
        effective_constraints.ensure_new_level();
        effective_constraints.add(
          min_count_in_map("min topic-wise count", *node2count));
    }
    effective_constraints.ensure_new_level();
    effective_constraints.add(max_final_capacity());
    effective_constraints.add(std::move(constraints));

    for (model::node_id prev : replicas_to_reallocate) {
        auto replica = do_allocate_replica(res, prev, effective_constraints);
        if (!replica) {
            return replica.error();
        }
        if (node2count) {
            node2count.value()[replica.value().current().node_id] += 1;
            size_t& prev_count = node2count.value().at(prev);
            prev_count -= 1;
            if (prev_count == 0) {
                node2count.value().erase(prev);
            }
        }
    }

    if (existing_replica_counts) {
        // update only at the very end, after all alocations have been
        // successful.
        *existing_replica_counts = std::move(node2count.value());
    }
    return res;
}

allocated_partition partition_allocator::make_allocated_partition(
  model::ntp ntp, std::vector<model::broker_shard> replicas) const {
    return allocated_partition{std::move(ntp), std::move(replicas), *_state};
}

result<reallocation_step> partition_allocator::reallocate_replica(
  allocated_partition& partition,
  model::node_id prev_node,
  allocation_constraints constraints) {
    vlog(
      clusterlog.trace,
      "reallocating replica {} for partition (all replicas: {}) with "
      "constraints: {}",
      prev_node,
      partition.replicas(),
      constraints);

    auto effective_constraints = default_constraints();
    effective_constraints.add(std::move(constraints));

    return do_allocate_replica(partition, prev_node, effective_constraints);
}

result<reallocation_step> partition_allocator::do_allocate_replica(
  allocated_partition& partition,
  std::optional<model::node_id> prev_node,
  const allocation_constraints& effective_constraints) {
    std::optional<allocated_partition::previous_replica> prev;
    if (prev_node) {
        prev = partition.prepare_move(*prev_node);
        if (!prev) {
            return errc::node_does_not_exists;
        }
    }

    auto node = _allocation_strategy.choose_node(
      *_state, effective_constraints, partition, prev_node);
    if (!node) {
        return node.error();
    }

    auto new_replica = partition.add_replica(node.value(), prev);
    std::optional<model::broker_shard> prev_replica;
    if (prev) {
        prev_replica = prev->bs;
    }
    return reallocation_step(new_replica, prev_replica);
}

void partition_allocator::add_allocations(
  const std::vector<model::broker_shard>& to_add) {
    for (const auto& bs : to_add) {
        _state->add_allocation(bs);
    }
}

void partition_allocator::remove_allocations(
  const std::vector<model::broker_shard>& to_remove) {
    for (const auto& bs : to_remove) {
        _state->remove_allocation(bs);
    }
}

void partition_allocator::add_final_counts(
  const std::vector<model::broker_shard>& to_add) {
    for (const auto& bs : to_add) {
        _state->add_final_count(bs);
    }
}

void partition_allocator::remove_final_counts(
  const std::vector<model::broker_shard>& to_remove) {
    for (const auto& bs : to_remove) {
        _state->remove_final_count(bs);
    }
}

ss::future<>
partition_allocator::apply_snapshot(const controller_snapshot& snap) {
    auto new_state = std::make_unique<allocation_state>(
      _feature_table,
      _partitions_per_shard,
      _partitions_reserve_shard0,
      _internal_kafka_topics);

    for (const auto& [id, node] : snap.members.nodes) {
        allocation_node::state state;
        switch (node.state.get_membership_state()) {
        case model::membership_state::active:
            state = allocation_node::state::active;
            break;
        case model::membership_state::draining:
            state = allocation_node::state::decommissioned;
            break;
        case model::membership_state::removed:
            state = allocation_node::state::deleted;
            break;
        default:
            vassert(
              false, "unknown membership state: {}", static_cast<int>(state));
        }

        new_state->register_node(node.broker, state);
    }

    const auto& topics_snap = snap.topics.topics;
    for (const auto& [ns_tp, topic] : topics_snap) {
        for (const auto& [p_id, partition] : topic.partitions) {
            for (const auto& bs : partition.replicas) {
                new_state->add_allocation(bs);
            }

            const std::vector<model::broker_shard>* final_replicas = nullptr;

            if (auto it = topic.updates.find(p_id); it != topic.updates.end()) {
                const auto& update = it->second;
                // Both old and new replicas contribute to allocator weights
                // regardless of the update state.
                auto additional_replicas = subtract(
                  update.target_assignment, partition.replicas);
                for (const auto& bs : additional_replicas) {
                    new_state->add_allocation(bs);
                }

                // final counts depend on the update state
                if (is_cancelled_state(update.state)) {
                    final_replicas = &partition.replicas;
                } else {
                    final_replicas = &update.target_assignment;
                }
            } else {
                final_replicas = &partition.replicas;
            }

            for (const auto& bs : *final_replicas) {
                new_state->add_final_count(bs);
            }

            co_await ss::coroutine::maybe_yield();
        }
    }

    new_state->update_highest_group_id(snap.topics.highest_group_id);

    // we substitute the state object for the new one so that in the unlikely
    // case there are in-flight allocation_units objects and they are destroyed,
    // they don't accidentally update the new state.
    _state = std::move(new_state);

    co_return;
}

} // namespace cluster
