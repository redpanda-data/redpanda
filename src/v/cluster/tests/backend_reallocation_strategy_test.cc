
// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "cluster/cluster_utils.h"
#include "cluster/commands.h"
#include "cluster/members_backend.h"
#include "cluster/members_table.h"
#include "cluster/scheduling/allocation_node.h"
#include "cluster/scheduling/partition_allocator.h"
#include "cluster/scheduling/types.h"
#include "cluster/topic_table.h"
#include "cluster/types.h"
#include "config/property.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"
#include "net/unresolved_address.h"
#include "random/generators.h"
#include "test_utils/fixture.h"

#include <seastar/core/future.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/log.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/node_hash_map.h>
#include <boost/test/tools/old/interface.hpp>
#include <fmt/core.h>

#include <math.h>
#include <tuple>

static ss::logger tlog("test-logger");

struct strategy_test_fixture {
    strategy_test_fixture()
      : topics()
      , members()
      , allocator(
          members,
          config::mock_binding<std::optional<size_t>>(std::nullopt),
          config::mock_binding<std::optional<int32_t>>(std::nullopt),
          config::mock_binding<uint32_t>(1000),
          config::mock_binding<uint32_t>(10),
          config::mock_binding<bool>(true)) {}

    cluster::topic_table topics;
    ss::sharded<cluster::members_table> members;
    cluster::partition_allocator allocator;
    model::offset rev_offset{0};

    model::broker make_broker(
      model::node_id id, uint32_t cores, std::optional<model::rack_id> rack) {
        return model::broker(
          id,
          net::unresolved_address("localhost", 9092),
          net::unresolved_address("localhost", 33145),
          std::move(rack),
          model::broker_properties{.cores = cores});
    }

    void add_node(
      int id,
      uint32_t cores,
      std::optional<model::rack_id> rack = std::nullopt) {
        model::node_id n_id(id);
        auto broker = make_broker(n_id, cores, std::move(rack));
        members.local().apply(rev_offset++, cluster::add_node_cmd(0, broker));
        allocator.upsert_allocation_node(broker);
    }

    ss::future<> add_topic(int partition_count, int rf) {
        cluster::allocation_request request(
          cluster::partition_allocation_domains::common);
        for (int i = 0; i < partition_count; ++i) {
            request.partitions.emplace_back(model::partition_id(i), rf);
        }

        auto res = co_await allocator.allocate(std::move(request));
        BOOST_REQUIRE(res.has_value());

        auto units = std::move(res.value());

        for (auto& p_as : units->get_assignments()) {
            allocator.update_allocation_state(
              p_as.replicas,
              p_as.group,
              cluster::partition_allocation_domains::common);
        }

        cluster::topic_configuration_assignment t_cfg;

        t_cfg.assignments = units->get_assignments();
        t_cfg.cfg.tp_ns = model::topic_namespace(
          model::kafka_namespace,
          model::topic(fmt::format("test-topic-{}", rev_offset())));
        t_cfg.cfg.partition_count = partition_count;
        t_cfg.cfg.replication_factor = rf;

        co_await topics.apply(
          cluster::create_topic_cmd(t_cfg.cfg.tp_ns, t_cfg), rev_offset++);
    }

    absl::flat_hash_map<model::node_id, size_t> replicas_per_node() {
        absl::flat_hash_map<model::node_id, size_t> ret;
        for (auto& n : allocator.state().allocation_nodes()) {
            ret[n.first] = n.second->allocated_partitions();
        }
        return ret;
    }

    void print_replicas_per_node() {
        auto per_node = replicas_per_node();
        for (auto& [id, cnt] : per_node) {
            fmt::print("node {} = {} replicas\n", id, cnt);
        }
    }

    size_t total_replicas() {
        auto per_node = replicas_per_node();
        size_t ret = 0;
        for (auto [_, cnt] : per_node) {
            ret += cnt;
        }
        return ret;
    }

    void validate_even_replica_distribution() {
        static constexpr double max_skew = 0.01;
        auto per_node = replicas_per_node();
        auto total = total_replicas();
        auto expected_per_node = floor(
          static_cast<double>(total) / per_node.size());

        for (auto& [id, replicas] : per_node) {
            tlog.info(
              "node {} has {} replicas, expected: {}",
              id,
              replicas,
              expected_per_node);
            BOOST_REQUIRE_GE(
              replicas, expected_per_node - ceil(max_skew * expected_per_node));
            BOOST_REQUIRE_LE(
              replicas, expected_per_node + ceil(max_skew * expected_per_node));
        }
    }

    void validate_even_topic_distribution() {
        absl::node_hash_map<
          model::topic_namespace,
          absl::flat_hash_map<model::node_id, size_t>>
          topic_replica_distribution;

        absl::node_hash_map<model::topic_namespace, size_t>
          total_topic_replicas;

        for (auto& [tp_ns, topic_md] : topics.all_topics_metadata()) {
            for (auto& p_as : topic_md.get_assignments()) {
                total_topic_replicas[tp_ns] += p_as.replicas.size();
                for (auto& r : p_as.replicas) {
                    topic_replica_distribution[tp_ns][r.node_id]++;
                }
            }
        }

        for (auto& [tp, node_replicas] : topic_replica_distribution) {
            auto expected_replicas_per_node = ceil(
              static_cast<double>(total_topic_replicas[tp])
              / allocator.state().available_nodes());

            if (
              total_topic_replicas[tp] < allocator.state().available_nodes()) {
                continue;
            }
            for (auto& [id, _] : allocator.state().allocation_nodes()) {
                auto it = node_replicas.find(id);
                const auto replicas_on_node = it == node_replicas.end()
                                                ? 0
                                                : it->second;
                tlog.info(
                  "topic {} has {} replicas on {}, expected: {} total "
                  "replicas: {}",
                  tp,
                  replicas_on_node,
                  id,
                  expected_replicas_per_node,
                  total_topic_replicas[tp]);
                auto err = std::abs(
                             expected_replicas_per_node - replicas_on_node)
                           / (double)total_topic_replicas[tp];
                // assert that the skew is smaller than 30%
                BOOST_REQUIRE_LE(err, 0.3);
            }
        }
    }

    std::unique_ptr<cluster::members_backend::reallocation_strategy>
    make_strategy() {
        return std::make_unique<
          cluster::members_backend::default_reallocation_strategy>();
    }

    ss::future<>
    apply_reallocations(cluster::members_backend::update_meta& meta) {
        for (auto& [ntp, pr] : meta.partition_reallocations) {
            auto added = cluster::subtract_replica_sets(
              pr.new_replica_set, pr.current_replica_set);
            auto removed = cluster::subtract_replica_sets(
              pr.current_replica_set, pr.new_replica_set);
            // update allocator
            allocator.add_allocations(
              added, cluster::partition_allocation_domains::common);
            allocator.remove_allocations(
              removed, cluster::partition_allocation_domains::common);

            auto ec = co_await topics.apply(
              cluster::move_partition_replicas_cmd(ntp, pr.new_replica_set),
              ++rev_offset);
            BOOST_REQUIRE(!ec);

            ec = co_await topics.apply(
              cluster::finish_moving_partition_replicas_cmd(
                ntp, pr.new_replica_set),
              ++rev_offset);
            BOOST_REQUIRE(!ec);
        }
        meta.partition_reallocations.clear();
    }

    ss::future<>
    rebalance(cluster::members_backend::reallocation_strategy& strategy) {
        cluster::members_backend::update_meta meta;
        while (true) {
            strategy.reallocations_for_even_partition_count(
              300,
              allocator,
              topics,
              meta,
              cluster::partition_allocation_domains::common);
            if (meta.partition_reallocations.empty()) {
                break;
            }

            co_await apply_reallocations(meta);
        }
    }

    ss::future<> start() { return members.start_single(); }

    ~strategy_test_fixture() { members.stop().get(); }
};

FIXTURE_TEST(rebalance_with_two_nodes_added, strategy_test_fixture) {
    start().get();

    add_node(0, 8);
    add_node(1, 8);
    add_node(2, 8);

    for (int i = 0; i < 50; ++i) {
        add_topic(random_generators::get_int(5, 100), 3).get();
    }

    // add two more nodes
    add_node(3, 8);
    add_node(4, 8);

    auto strategy = make_strategy();
    rebalance(*strategy).get();

    validate_even_replica_distribution();
    validate_even_topic_distribution();
}

FIXTURE_TEST(rebalance_with_one_node_added, strategy_test_fixture) {
    start().get();

    add_node(0, 8);
    add_node(1, 8);
    add_node(2, 8);

    for (int i = 0; i < 50; ++i) {
        add_topic(random_generators::get_int(5, 100), 3).get();
    }

    // add one node
    add_node(3, 8);

    auto strategy = make_strategy();
    rebalance(*strategy).get();

    validate_even_replica_distribution();
    validate_even_topic_distribution();
}

FIXTURE_TEST(rebalance_single_topic, strategy_test_fixture) {
    start().get();

    add_node(0, 8);
    add_node(1, 8);
    add_node(2, 8);

    add_topic(3, 3).get();

    // add one node
    add_node(3, 8);

    auto strategy = make_strategy();
    rebalance(*strategy).get();

    validate_even_replica_distribution();
    validate_even_topic_distribution();
}
