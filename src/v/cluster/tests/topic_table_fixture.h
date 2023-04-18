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

#include "cluster/members_table.h"
#include "cluster/partition_balancer_state.h"
#include "cluster/scheduling/allocation_node.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/tests/utils.h"
#include "cluster/topic_table.h"
#include "config/property.h"
#include "test_utils/fixture.h"
#include "units.h"

inline void validate_delta(
  const fragmented_vector<cluster::topic_table::delta>& d,
  int new_partitions,
  int removed_partitions) {
    size_t additions = std::count_if(
      d.begin(), d.end(), [](const cluster::topic_table::delta& d) {
          return d.type == cluster::topic_table::delta::op_type::add;
      });
    size_t deletions = std::count_if(
      d.begin(), d.end(), [](const cluster::topic_table::delta& d) {
          return d.type == cluster::topic_table::delta::op_type::del;
      });
    BOOST_REQUIRE_EQUAL(additions, new_partitions);
    BOOST_REQUIRE_EQUAL(deletions, removed_partitions);
}

struct topic_table_fixture {
    static constexpr uint32_t partitions_per_shard = 7000;
    static constexpr uint32_t partitions_reserve_shard0 = 2;

    topic_table_fixture() {
        table.start().get0();
        members.start_single().get0();
        allocator
          .start_single(
            std::ref(members),
            config::mock_binding<std::optional<size_t>>(std::nullopt),
            config::mock_binding<std::optional<int32_t>>(std::nullopt),
            config::mock_binding<uint32_t>(uint32_t{partitions_per_shard}),
            config::mock_binding<uint32_t>(uint32_t{partitions_reserve_shard0}),
            config::mock_binding<bool>(false))
          .get0();
        allocator.local().register_node(
          create_allocation_node(model::node_id(1), 8));
        allocator.local().register_node(
          create_allocation_node(model::node_id(2), 12));
        allocator.local().register_node(
          create_allocation_node(model::node_id(3), 4));
        pb_state
          .start_single(std::ref(table), std::ref(members), std::ref(allocator))
          .get();
    }

    ~topic_table_fixture() {
        pb_state.stop().get();
        table.stop().get0();
        allocator.stop().get0();
        members.stop().get0();
        as.request_abort();
    }

    static std::unique_ptr<cluster::allocation_node>
    create_allocation_node(model::node_id nid, uint32_t cores) {
        return std::make_unique<cluster::allocation_node>(
          nid,
          cores,
          config::mock_binding<uint32_t>(uint32_t{partitions_per_shard}),
          config::mock_binding<uint32_t>(uint32_t{partitions_reserve_shard0}));
    }

    cluster::topic_configuration_assignment make_tp_configuration(
      const ss::sstring& topic, int partitions, int16_t replication_factor) {
        cluster::topic_configuration cfg(
          test_ns, model::topic(topic), partitions, replication_factor);

        cluster::allocation_request req(
          cluster::partition_allocation_domains::common);
        req.partitions.reserve(partitions);
        for (auto p = 0; p < partitions; ++p) {
            req.partitions.emplace_back(
              model::partition_id(p), replication_factor);
        }

        auto pas = allocator.local()
                     .allocate(std::move(req))
                     .get()
                     .value()
                     ->get_assignments();

        return cluster::topic_configuration_assignment(cfg, std::move(pas));
    }

    cluster::create_topic_cmd make_create_topic_cmd(
      const ss::sstring& name, int partitions, int replication_factor) {
        return cluster::create_topic_cmd(
          make_tp_ns(name),
          make_tp_configuration(name, partitions, replication_factor));
    }

    model::topic_namespace make_tp_ns(const ss::sstring& tp) {
        return model::topic_namespace(test_ns, model::topic(tp));
    }

    void create_topics() {
        auto cmd_1 = make_create_topic_cmd("test_tp_1", 1, 3);
        cmd_1.value.cfg.properties.compaction_strategy
          = model::compaction_strategy::offset;
        cmd_1.value.cfg.properties.cleanup_policy_bitflags
          = model::cleanup_policy_bitflags::compaction;
        cmd_1.value.cfg.properties.compression = model::compression::lz4;
        cmd_1.value.cfg.properties.retention_bytes = tristate(
          std::make_optional(2_GiB));
        cmd_1.value.cfg.properties.retention_duration = tristate(
          std::make_optional(std::chrono::milliseconds(3600000)));
        auto cmd_2 = make_create_topic_cmd("test_tp_2", 12, 3);
        auto cmd_3 = make_create_topic_cmd("test_tp_3", 8, 1);

        auto res_1
          = table.local().apply(std::move(cmd_1), model::offset(0)).get0();
        auto res_2
          = table.local().apply(std::move(cmd_2), model::offset(0)).get0();
        auto res_3
          = table.local().apply(std::move(cmd_3), model::offset(0)).get0();

        BOOST_REQUIRE_EQUAL(res_1, cluster::errc::success);
        BOOST_REQUIRE_EQUAL(res_2, cluster::errc::success);
        BOOST_REQUIRE_EQUAL(res_3, cluster::errc::success);
    }

    size_t total_capacity() {
        size_t total = 0;
        for (const auto& [id, n] :
             allocator.local().state().allocation_nodes()) {
            total += n->partition_capacity();
        }
        return total;
    }

    ss::sharded<cluster::members_table> members;
    ss::sharded<cluster::partition_allocator> allocator;
    ss::sharded<cluster::topic_table> table;
    ss::sharded<cluster::partition_leaders_table> leaders;
    ss::sharded<cluster::partition_balancer_state> pb_state;
    ss::abort_source as;
};
