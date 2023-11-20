// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/scheduling/partition_allocator.h"

#include "cluster/cluster_utils.h"
#include "cluster/controller_snapshot.h"
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
  config::binding<std::optional<size_t>> memory_per_partition,
  config::binding<std::optional<int32_t>> fds_per_partition,
  config::binding<uint32_t> partitions_per_shard,
  config::binding<uint32_t> partitions_reserve_shard0,
  config::binding<std::vector<ss::sstring>> internal_kafka_topics,
  config::binding<bool> enable_rack_awareness)
  : _state(std::make_unique<allocation_state>(
    partitions_per_shard, partitions_reserve_shard0, internal_kafka_topics))
  , _allocation_strategy(simple_allocation_strategy())
  , _members(members)
  , _memory_per_partition(std::move(memory_per_partition))
  , _fds_per_partition(std::move(fds_per_partition))
  , _partitions_per_shard(std::move(partitions_per_shard))
  , _partitions_reserve_shard0(std::move(partitions_reserve_shard0))
  , _internal_kafka_topics(std::move(internal_kafka_topics))
  , _enable_rack_awareness(std::move(enable_rack_awareness)) {}

allocation_constraints partition_allocator::default_constraints(
  const partition_allocation_domain domain) {
    allocation_constraints req;

    req.add(distinct_nodes());
    req.add(not_fully_allocated());
    req.add(is_active());

    if (domain == partition_allocation_domains::common) {
        req.add(max_final_capacity());
    } else {
        req.add(max_final_capacity_in_domain(domain));
    }
    if (_enable_rack_awareness()) {
        req.add(distinct_rack_preferred(_members.local()));
    }
    return req;
}

result<allocated_partition> partition_allocator::allocate_new_partition(
  model::topic_namespace nt,
  partition_constraints p_constraints,
  const partition_allocation_domain domain) {
    vlog(
      clusterlog.trace,
      "allocating new partition with constraints: {}",
      p_constraints);

    uint16_t replicas_to_allocate = p_constraints.replication_factor;
    if (
      replicas_to_allocate == 0
      || _state->available_nodes() < replicas_to_allocate) {
        return errc::topic_invalid_replication_factor;
    }

    auto effective_constraints = default_constraints(domain);
    effective_constraints.add(p_constraints.constraints);

    model::ntp ntp{
      std::move(nt.ns), std::move(nt.tp), p_constraints.partition_id};
    allocated_partition ret{std::move(ntp), {}, domain, *_state};
    for (auto r = 0; r < replicas_to_allocate; ++r) {
        auto replica = do_allocate_replica(
          ret, std::nullopt, effective_constraints);
        if (!replica) {
            return replica.error();
        }
    }
    return ret;
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
        create_count += uint64_t(i.replication_factor);
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

        // In redpanda <= 21.11.x, available_memory_gb and available_disk_gb
        // are not populated.  If they're zero we skip the check later.
        if (min_memory_bytes == 0) {
            min_memory_bytes = b_properties.available_memory_gb * 1_GiB;
        } else if (b_properties.available_memory_gb > 0) {
            min_memory_bytes = std::min(
              min_memory_bytes, b_properties.available_memory_gb * 1_GiB);
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
              "Refusing to create {} new partitions as total partition count "
              "{} "
              "would exceed memory limit {}",
              create_count,
              proposed_total_partitions,
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
                    return errc::topic_invalid_partitions;
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

    intermediate_allocation assignments(
      *_state, request.partitions.size(), request.domain);

    const auto& nt = request._nt;
    for (auto& p_constraints : request.partitions) {
        auto const partition_id = p_constraints.partition_id;
        auto allocated = allocate_new_partition(
          nt, std::move(p_constraints), request.domain);
        if (!allocated) {
            co_return allocated.error();
        }
        assignments.emplace_back(
          _state->next_group_id(),
          partition_id,
          allocated.value().release_new_partition());
        co_await ss::coroutine::maybe_yield();
    }

    co_return ss::make_foreign(std::make_unique<allocation_units>(
      std::move(assignments).finish(), *_state, request.domain));
}

result<allocated_partition> partition_allocator::reallocate_partition(
  model::topic_namespace nt,
  partition_constraints p_constraints,
  const partition_assignment& current_assignment,
  const partition_allocation_domain domain,
  const std::vector<model::node_id>& replicas_to_reallocate) {
    vlog(
      clusterlog.debug,
      "reallocating {}, current replicas: {}, to move: {}",
      p_constraints,
      current_assignment.replicas,
      replicas_to_reallocate);

    int16_t num_new_replicas = p_constraints.replication_factor
                               - current_assignment.replicas.size();
    if (
      _state->available_nodes() < p_constraints.replication_factor
      || num_new_replicas < 0) {
        return errc::topic_invalid_replication_factor;
    }

    model::ntp ntp{
      std::move(nt.ns), std::move(nt.tp), p_constraints.partition_id};
    allocated_partition res{
      std::move(ntp), current_assignment.replicas, domain, *_state};

    if (num_new_replicas == 0 && replicas_to_reallocate.empty()) {
        // nothing to do
        return res;
    }

    auto effective_constraints = default_constraints(domain);
    effective_constraints.add(std::move(p_constraints.constraints));

    // first, move existing replicas
    for (model::node_id prev : replicas_to_reallocate) {
        auto replica = do_allocate_replica(res, prev, effective_constraints);
        if (!replica) {
            return replica.error();
        }
    }

    // next, allocate new ones
    for (int i = 0; i < num_new_replicas; ++i) {
        auto replica = do_allocate_replica(
          res, std::nullopt, effective_constraints);
        if (!replica) {
            return replica.error();
        }
    }

    return res;
}

allocated_partition partition_allocator::make_allocated_partition(
  model::ntp ntp,
  std::vector<model::broker_shard> replicas,
  partition_allocation_domain domain) const {
    return allocated_partition{
      std::move(ntp), std::move(replicas), domain, *_state};
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

    auto effective_constraints = default_constraints(partition._domain);
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
    auto revert = ss::defer([&] {
        if (prev) {
            partition.cancel_move(*prev);
        }
    });

    auto node = _allocation_strategy.choose_node(
      partition._ntp,
      partition._replicas,
      effective_constraints,
      *_state,
      partition._domain);
    if (!node) {
        return node.error();
    }

    revert.cancel();
    auto new_replica = partition.add_replica(node.value(), prev);
    std::optional<model::broker_shard> prev_replica;
    if (prev) {
        prev_replica = prev->bs;
    }
    return reallocation_step(new_replica, prev_replica);
}

void partition_allocator::add_allocations(
  const std::vector<model::broker_shard>& to_add,
  const partition_allocation_domain domain) {
    for (const auto& bs : to_add) {
        _state->add_allocation(bs, domain);
    }
}

void partition_allocator::remove_allocations(
  const std::vector<model::broker_shard>& to_remove,
  const partition_allocation_domain domain) {
    for (const auto& bs : to_remove) {
        _state->remove_allocation(bs, domain);
    }
}

void partition_allocator::add_final_counts(
  const std::vector<model::broker_shard>& to_add,
  const partition_allocation_domain domain) {
    for (const auto& bs : to_add) {
        _state->add_final_count(bs, domain);
    }
}

void partition_allocator::remove_final_counts(
  const std::vector<model::broker_shard>& to_remove,
  const partition_allocation_domain domain) {
    for (const auto& bs : to_remove) {
        _state->remove_final_count(bs, domain);
    }
}

ss::future<>
partition_allocator::apply_snapshot(const controller_snapshot& snap) {
    auto new_state = std::make_unique<allocation_state>(
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
        auto domain = get_allocation_domain(ns_tp);
        for (const auto& [p_id, partition] : topic.partitions) {
            for (const auto& bs : partition.replicas) {
                new_state->add_allocation(bs, domain);
            }

            const std::vector<model::broker_shard>* final_replicas = nullptr;

            if (auto it = topic.updates.find(p_id); it != topic.updates.end()) {
                const auto& update = it->second;
                // Both old and new replicas contribute to allocator weights
                // regardless of the update state.
                auto additional_replicas = subtract_replica_sets(
                  update.target_assignment, partition.replicas);
                for (const auto& bs : additional_replicas) {
                    new_state->add_allocation(bs, domain);
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
                new_state->add_final_count(bs, domain);
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
