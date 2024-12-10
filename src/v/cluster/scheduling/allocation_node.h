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

#include "cluster/scheduling/types.h"
#include "cluster/types.h"
#include "config/property.h"
#include "model/fundamental.h"

#include <absl/container/node_hash_map.h>

namespace cluster {

class allocation_state;

/**
 * Allocation node represent a node where partitions may be allocated
 */
class allocation_node {
public:
    enum class state { active, decommissioned, deleted };
    using allocation_capacity
      = named_type<uint32_t, struct allocation_node_slot_tag>;

    allocation_node(
      model::node_id,
      uint32_t /*cpus*/,
      config::binding<uint32_t> /*partitions_per_shard*/,
      config::binding<uint32_t> /*partitions_reserve_shard0*/,
      config::binding<std::vector<ss::sstring>> /*internal_kafka_topics*/);

    allocation_node(allocation_node&& o) noexcept = default;
    allocation_node& operator=(allocation_node&&) = delete;
    allocation_node(const allocation_node&) = delete;
    allocation_node& operator=(const allocation_node&) = delete;
    ~allocation_node() = default;

    uint32_t cpus() const { return _weights.size(); }
    model::node_id id() const { return _id; }

    // Free partition space left for allocation in the node.
    // Reserved partitions are considered allocated
    allocation_capacity partition_capacity() const {
        // there might be a situation when node is over assigned, this state is
        // transient and it may be caused by holding allocation units while
        // state is being updated
        return _max_capacity - std::min(_allocated_partitions, _max_capacity);
    }
    // Would-be free partition space in the node if only the partitions of the
    // specified domain were allocated. Reserved partitions are considered
    // allocated regardless of the specified domain
    allocation_capacity
    domain_partition_capacity(const partition_allocation_domain domain) const {
        if (const auto domain_partitions_i = _allocated_domain_partitions.find(
              domain);
            domain_partitions_i != _allocated_domain_partitions.cend()) {
            return _max_capacity
                   - std::min(domain_partitions_i->second, _max_capacity);
        }
        return _max_capacity;
    }
    // Overall partition space of the node, less reserved partitions
    allocation_capacity max_capacity() const { return _max_capacity; }

    void decommission() {
        vassert(
          _state == state::active,
          "can only decommission active node, current node: {}",
          *this);
        _state = state::decommissioned;
    }

    void mark_as_removed() { _state = state::deleted; }
    void mark_as_active() { _state = state::active; }

    void recommission() {
        vassert(
          _state == state::decommissioned,
          "can only recommission decommissioned node, current node: {}",
          *this);
        _state = state::active;
    }

    bool is_decommissioned() const { return _state == state::decommissioned; }
    bool is_active() const { return _state == state::active; }
    bool is_removed() const { return _state == state::deleted; }

    void update_core_count(uint32_t);

    allocation_capacity allocated_partitions() const {
        return _allocated_partitions;
    }

    // number of partitions after all in-progress movements are finished
    allocation_capacity final_partitions() const { return _final_partitions; }

    allocation_capacity
    domain_allocated_partitions(partition_allocation_domain domain) const {
        if (auto it = _allocated_domain_partitions.find(domain);
            it != _allocated_domain_partitions.end()) {
            return it->second;
        }
        return allocation_capacity{0};
    }

    allocation_capacity
    domain_final_partitions(partition_allocation_domain domain) const {
        if (auto it = _final_domain_partitions.find(domain);
            it != _final_domain_partitions.end()) {
            return it->second;
        }
        return allocation_capacity{0};
    }

    bool empty() const {
        return _allocated_partitions == allocation_capacity{0};
    }
    bool is_full(const model::ntp&, bool will_add_allocation) const;

    // Internal topics are excluded from checks to prevent allocation failures
    // when creating them. This is okay because they are fairly small in number
    // compared to kafka user topic partitions.
    static bool is_internal_topic(
      const config::binding<std::vector<ss::sstring>>& internal_kafka_topics,
      model::topic_namespace_view ntp);

private:
    friend allocation_state;

    ss::shard_id allocate_shard();

    void add_allocation(partition_allocation_domain);
    void add_allocation(ss::shard_id core);
    void remove_allocation(partition_allocation_domain);
    void remove_allocation(ss::shard_id core);
    void add_final_count(partition_allocation_domain);
    void remove_final_count(partition_allocation_domain);

    model::node_id _id;
    /// each index is a CPU. A weight is roughly the number of assignments
    std::vector<uint32_t> _weights;
    allocation_capacity _max_capacity;
    // number of partitions allocated in all domains
    allocation_capacity _allocated_partitions{0};
    // number of partitions allocated in a specific allocation domain
    absl::flat_hash_map<partition_allocation_domain, allocation_capacity>
      _allocated_domain_partitions;
    // number of partitions after all movements are finished
    allocation_capacity _final_partitions{0};
    absl::flat_hash_map<partition_allocation_domain, allocation_capacity>
      _final_domain_partitions;

    state _state = state::active;

    config::binding<uint32_t> _partitions_per_shard;
    config::binding<uint32_t> _partitions_reserve_shard0;
    config::binding<std::vector<ss::sstring>> _internal_kafka_topics;
    // Keep track of how much weight we applied to shard0,
    // to enable runtime updates
    int32_t _shard0_reserved{0};
    uint32_t _cpus;

    friend std::ostream& operator<<(std::ostream&, const allocation_node&);
    friend std::ostream& operator<<(std::ostream& o, state s);
};
} // namespace cluster
