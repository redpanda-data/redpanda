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

#include "base/units.h"
#include "cluster/commands.h"
#include "cluster/members_table.h"
#include "cluster/scheduling/allocation_node.h"
#include "cluster/scheduling/allocation_strategy.h"
#include "cluster/scheduling/partition_allocator.h"
#include "config/configuration.h"
#include "config/mock_property.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "random/fast_prng.h"
#include "random/generators.h"
#include "utils/unresolved_address.h"

#include <seastar/core/chunked_fifo.hh>

#include <limits>

struct partition_allocator_fixture {
    static constexpr uint32_t partitions_per_shard = 1000;
    static constexpr uint32_t partitions_reserve_shard0 = 2;

    partition_allocator_fixture()
      : partition_allocator_fixture(std::nullopt, std::nullopt) {}

    ~partition_allocator_fixture() {
        _allocator.stop().get();
        features.stop().get();
        members.stop().get();
    }

    void register_node(
      int id,
      uint32_t core_count,
      std::optional<model::rack_id> rack = std::nullopt) {
        model::broker broker(
          model::node_id(id),
          net::unresolved_address("localhost", 9092 + id),
          net::unresolved_address("localhost", 33145 + id),
          std::move(rack),
          model::broker_properties{
            .cores = core_count,
            .available_memory_gb = 5 * core_count,
            .available_disk_gb = 10 * core_count});

        auto ec = members.local().apply(
          model::offset(0), cluster::add_node_cmd(0, broker));
        if (ec) {
            throw std::runtime_error(
              ss::format("unable to apply add node cmd: {}", ec.message()));
        }

        allocator().register_node(std::make_unique<cluster::allocation_node>(
          broker.id(),
          broker.properties().cores,
          config::mock_binding<uint32_t>(uint32_t{partitions_per_shard}),
          config::mock_binding<uint32_t>(uint32_t{partitions_reserve_shard0}),
          kafka_internal_topics.bind()));
    }

    void saturate_all_machines() {
        auto units = allocator()
                       .allocate(make_allocation_request(max_capacity(), 1))
                       .get();

        for (auto& pas : units.value()->get_assignments()) {
            allocator().add_allocations_for_new_partition(
              pas.replicas, pas.group);
        }
    }

    uint allocated_nodes_count(
      const ss::chunked_fifo<cluster::partition_assignment>& allocs) {
        return std::accumulate(
          allocs.begin(),
          allocs.end(),
          0U,
          [](size_t acc, const cluster::partition_assignment& p_as) {
              return acc + p_as.replicas.size();
          });
    }

    bool all_nodes_empty() {
        return std::all_of(
          allocator().state().allocation_nodes().begin(),
          allocator().state().allocation_nodes().end(),
          [](const auto& n) { return n.second->empty(); });
    }

    int32_t max_capacity() {
        return std::accumulate(
          allocator().state().allocation_nodes().begin(),
          allocator().state().allocation_nodes().end(),
          0,
          [](int acc, auto& n) {
              return acc + n.second->partition_capacity();
          });
    }

    cluster::allocation_request
    make_allocation_request(int partitions, uint16_t replication_factor) {
        return make_allocation_request(tn, partitions, replication_factor);
    }

    cluster::allocation_request make_allocation_request(
      model::topic_namespace tn, int partitions, uint16_t replication_factor) {
        cluster::allocation_request req(std::move(tn));
        req.partitions.reserve(partitions);
        for (int i = 0; i < partitions; ++i) {
            req.partitions.emplace_back(
              model::partition_id(i), replication_factor);
        }
        return req;
    }

    cluster::partition_allocator& allocator() { return _allocator.local(); }

    config::mock_property<std::vector<ss::sstring>> kafka_internal_topics{{}};
    model::topic_namespace tn{model::kafka_namespace, model::topic{"test"}};
    ss::sharded<cluster::members_table> members;
    ss::sharded<features::feature_table> features;
    ss::sharded<cluster::partition_allocator> _allocator;

    fast_prng prng;

protected:
    explicit partition_allocator_fixture(
      std::optional<size_t> memory_per_partition,
      std::optional<int32_t> fds_per_partition) {
        members.start().get();
        features.start().get();
        _allocator
          .start_single(
            std::ref(members),
            std::ref(features),
            config::mock_binding<std::optional<size_t>>(memory_per_partition),
            config::mock_binding<std::optional<int32_t>>(fds_per_partition),
            config::mock_binding<uint32_t>(uint32_t{partitions_per_shard}),
            config::mock_binding<uint32_t>(uint32_t{partitions_reserve_shard0}),
            kafka_internal_topics.bind(),
            config::mock_binding<bool>(true))
          .get();
        ss::smp::invoke_on_all([] {
            config::shard_local_cfg()
              .get("partition_autobalancing_mode")
              .set_value(model::partition_autobalancing_mode::node_add);
        }).get();
    }
};

struct partition_allocator_memory_limited_fixture
  : public partition_allocator_fixture {
    static constexpr size_t memory_per_partition
      = std::numeric_limits<size_t>::max();
    partition_allocator_memory_limited_fixture()
      : partition_allocator_fixture(memory_per_partition, std::nullopt) {}
};

struct partition_allocator_fd_limited_fixture
  : public partition_allocator_fixture {
    static constexpr int32_t fds_per_partition
      = std::numeric_limits<int32_t>::max();

    partition_allocator_fd_limited_fixture()
      : partition_allocator_fixture(std::nullopt, fds_per_partition) {}
};
