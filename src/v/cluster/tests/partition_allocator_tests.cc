// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/cluster_utils.h"
#include "cluster/scheduling/allocation_node.h"
#include "cluster/scheduling/types.h"
#include "cluster/tests/partition_allocator_fixture.h"
#include "model/metadata.h"
#include "raft/types.h"
#include "random/fast_prng.h"
#include "random/generators.h"
#include "test_utils/fixture.h"

#include <seastar/core/sharded.hh>

#include <absl/container/flat_hash_set.h>
#include <boost/test/tools/old/interface.hpp>

void validate_replica_set_diversity(
  const std::vector<cluster::partition_assignment> assignments) {
    for (const auto& assignment : assignments) {
        if (assignment.replicas.size() > 1) {
            auto sentinel = assignment.replicas.front();
            BOOST_TEST_REQUIRE(std::all_of(
              std::next(assignment.replicas.begin()),
              assignment.replicas.end(),
              [sentinel](const model::broker_shard bs) {
                  return sentinel.node_id != bs.node_id;
              }));
        }
    }
}

FIXTURE_TEST(register_node, partition_allocator_fixture) {
    register_node(0, 32);
    register_node(1, 64);
    register_node(2, 12);
    BOOST_REQUIRE(allocator.contains_node(model::node_id(0)));
    BOOST_REQUIRE(allocator.contains_node(model::node_id(1)));
    BOOST_REQUIRE(allocator.contains_node(model::node_id(2)));
    BOOST_REQUIRE_EQUAL(allocator.state().available_nodes(), 3);
}

model::broker create_broker(
  int node_id,
  uint32_t core_count,
  std::optional<model::rack_id> rack = std::nullopt) {
    return model::broker(
      model::node_id(node_id),
      net::unresolved_address("localhost", 1024),
      net::unresolved_address("localhost", 1024),
      std::move(rack),
      model::broker_properties{.cores = core_count});
}

FIXTURE_TEST(unregister_node, partition_allocator_fixture) {
    register_node(0, 32);
    register_node(1, 64);
    register_node(2, 12);

    allocator.update_allocation_nodes(
      std::vector<model::broker>{create_broker(0, 32), create_broker(2, 12)});
    BOOST_REQUIRE(allocator.contains_node(model::node_id(0)));
    // allocator MUST still contain the node. it has to be marked as removed
    BOOST_REQUIRE(allocator.contains_node(model::node_id(1)));
    BOOST_REQUIRE(allocator.state()
                    .allocation_nodes()
                    .find(model::node_id(1))
                    ->second->is_removed());
    BOOST_REQUIRE(allocator.contains_node(model::node_id(2)));
    BOOST_REQUIRE_EQUAL(allocator.state().available_nodes(), 2);
}

FIXTURE_TEST(invalid_allocation_over_capacity, partition_allocator_fixture) {
    register_node(0, 6);
    register_node(1, 6);
    register_node(2, 6);

    saturate_all_machines();
    auto gr = allocator.state().last_group_id();
    BOOST_REQUIRE(
      allocator.allocate(make_allocation_request(1, 1)).get().has_error());
    // group id hasn't changed
    BOOST_REQUIRE_EQUAL(allocator.state().last_group_id(), gr);
}

FIXTURE_TEST(max_allocation, partition_allocator_fixture) {
    register_node(0, 2);
    register_node(1, 2);
    register_node(2, 2);
    // available capacity
    // 3 * 1000 * 2 - 3*2 = 5994

    auto req = make_allocation_request(max_capacity() / 3, 3);

    auto units = allocator.allocate(std::move(req)).get().value();

    BOOST_REQUIRE_EQUAL(units->get_assignments().size(), 1998);
    BOOST_REQUIRE_EQUAL(allocated_nodes_count(units->get_assignments()), 5994);
    BOOST_REQUIRE_EQUAL(allocator.state().last_group_id()(), 1998);
    validate_replica_set_diversity(units->get_assignments());

    // make sure there is no room left after
    auto single_partition_req = make_allocation_request(1, 1);
    auto result = allocator.allocate(std::move(single_partition_req)).get();
    BOOST_REQUIRE(result.has_error());
}

FIXTURE_TEST(unsatisfyable_diversity_assignment, partition_allocator_fixture) {
    register_node(0, 8);
    register_node(1, 4);
    register_node(2, 6);

    auto req = make_allocation_request(1, 5);
    auto allocs = allocator.allocate(std::move(req)).get();
    BOOST_TEST_REQUIRE(allocs.has_error());
    BOOST_REQUIRE_EQUAL(
      cluster::errc(allocs.error().value()),
      cluster::errc::topic_invalid_replication_factor);

    // ensure rollback happened
    BOOST_REQUIRE(all_nodes_empty());

    BOOST_REQUIRE_EQUAL(allocator.state().last_group_id()(), 0);
}

FIXTURE_TEST(diverse_replica_sets, partition_allocator_fixture) {
    // This tests that all possible replica sets are chosen, rather than some
    // fixed subset (e.g., if the allocator uses a repeating sequential pattern
    // for allocating replica sets, most sets won't be chosen).

    constexpr int node_count = 6;
    constexpr int r = 3;
    constexpr int possible_sets = 20; // 6 choose 3

    for (int node = 0; node < node_count; node++) {
        register_node(node, 2);
    }

    // for the 6 nodes, r=3 case, all replica sets will be chosen with
    // probability about 1 - 1e-21 (i.e., with an astronomically high chance)
    // after 1,000 samples.
    absl::flat_hash_set<std::vector<model::broker_shard>> seen_replicas;
    for (int i = 0; i < 1000; i++) {
        auto req = make_allocation_request(1, r);
        auto result = allocator.allocate(std::move(req)).get();
        BOOST_REQUIRE(result);
        auto assignments = result.value()->get_assignments();
        BOOST_REQUIRE(assignments.size() == 1);
        auto replicas = assignments.front().replicas;
        // we need to sort the replica set
        std::sort(replicas.begin(), replicas.end());
        seen_replicas.insert(replicas);
    }

    BOOST_REQUIRE_EQUAL(seen_replicas.size(), possible_sets);
}

FIXTURE_TEST(partial_assignment, partition_allocator_fixture) {
    register_node(0, 2);
    register_node(1, 2);
    register_node(2, 2);
    auto max_partitions_in_cluster = max_capacity() / 3;

    auto units_1 = allocator
                     .allocate(make_allocation_request(
                       max_partitions_in_cluster - 1, 3))
                     .get()
                     .value();
    BOOST_REQUIRE_EQUAL(
      units_1->get_assignments().size(), max_partitions_in_cluster - 1);
    validate_replica_set_diversity(units_1->get_assignments());
    // allocate 2 partitions - one should fail, returning null & deallocating

    auto req_2 = make_allocation_request(2, 3);
    auto units_2 = allocator.allocate(std::move(req_2)).get();
    BOOST_REQUIRE(units_2.has_error());

    BOOST_REQUIRE_EQUAL(3, max_capacity());
    BOOST_REQUIRE_EQUAL(
      allocator.state().last_group_id()(), max_partitions_in_cluster - 1);
}
FIXTURE_TEST(max_deallocation, partition_allocator_fixture) {
    register_node(0, 3);
    register_node(1, 3);
    register_node(2, 3);
    // This test performs - 209994 partition assignments
    const auto max = max_capacity();

    {
        auto allocs = allocator.allocate(make_allocation_request(max / 3, 3))
                        .get()
                        .value();

        BOOST_REQUIRE_EQUAL(allocs->get_assignments().size() * 3, max);

        BOOST_REQUIRE_EQUAL(allocator.state().last_group_id()(), max / 3);
    }

    BOOST_REQUIRE_EQUAL(allocator.state().last_group_id()(), max / 3);
    BOOST_REQUIRE_EQUAL(max_capacity(), max);
}

FIXTURE_TEST(recovery_test, partition_allocator_fixture) {
    register_node(0, 3);
    register_node(1, 3);
    register_node(2, 3);
    auto create_replicas = [this](int topics, int partitions) {
        std::vector<model::broker_shard> ret;

        for (int t = 0; t < topics; t++) {
            for (int p = 0; p < partitions; p++) {
                std::vector<model::broker_shard> replicas;
                for (int r = 0; r < 3; r++) {
                    ret.push_back(
                      model::broker_shard{model::node_id(r), prng() % 3});
                }
            }
        }
        return ret;
    };
    // 100 topics with 12 partitions each replicated on 3 nodes each
    auto replicas = create_replicas(100, 12);
    allocator.update_allocation_state(
      replicas,
      raft::group_id(0),
      cluster::partition_allocation_domains::common);
    // each node in the cluster holds one replica for each partition,
    // so it has to have topics * partitions shards allocated
    cluster::allocation_node::allocation_capacity allocated_shards{100 * 12};
    // Remaining capacity on node 0
    BOOST_REQUIRE_EQUAL(
      allocator.state()
        .allocation_nodes()
        .find(model::node_id(0))
        ->second->allocated_partitions(),
      allocated_shards);
    // Remaining capacity on node 1
    BOOST_REQUIRE_EQUAL(
      allocator.state()
        .allocation_nodes()
        .find(model::node_id(1))
        ->second->allocated_partitions(),
      allocated_shards);
    // Remaining capacity on node 2
    BOOST_REQUIRE_EQUAL(
      allocator.state()
        .allocation_nodes()
        .find(model::node_id(2))
        ->second->allocated_partitions(),
      allocated_shards);
}

FIXTURE_TEST(allocation_units_test, partition_allocator_fixture) {
    register_node(1, 10);
    register_node(2, 11);
    register_node(3, 12);
    // just fill up the cluster partially

    {
        auto allocs
          = allocator.allocate(make_allocation_request(10, 3)).get().value();
        BOOST_REQUIRE_EQUAL(allocs->get_assignments().size(), 10);
        BOOST_REQUIRE_EQUAL(
          allocated_nodes_count(allocs->get_assignments()), 3 * 10);
    }

    BOOST_REQUIRE(all_nodes_empty());

    // we do not decrement the highest raft group
    BOOST_REQUIRE_EQUAL(allocator.state().last_group_id()(), 10);
}
FIXTURE_TEST(decommission_node, partition_allocator_fixture) {
    register_node(0, 32);
    register_node(1, 64);
    register_node(2, 12);
    allocator.decommission_node(model::node_id(1));

    // only two of machines are available as one of them is decommissioned
    BOOST_REQUIRE_EQUAL(allocator.state().available_nodes(), 2);
}

cluster::hard_constraint make_throwing_hard_evaluator() {
    struct impl : cluster::hard_constraint::impl {
        cluster::hard_constraint_evaluator
        make_evaluator(const cluster::replicas_t&) const final {
            return [](const cluster::allocation_node&) -> bool {
                throw std::runtime_error("evaluation exception");
            };
        }
        ss::sstring name() const final {
            return "exception throwing hard constraint evaluator";
        }
    };

    return cluster::hard_constraint(std::make_unique<impl>());
}

cluster::hard_constraint make_false_evaluator() {
    struct impl : cluster::hard_constraint::impl {
        cluster::hard_constraint_evaluator
        make_evaluator(const cluster::replicas_t&) const final {
            return [](const cluster::allocation_node&) { return true; };
        }
        ss::sstring name() const final {
            return "false returning constraint evaluator";
        }
    };

    return cluster::hard_constraint(std::make_unique<impl>());
}

cluster::hard_constraint make_nop_evaluator() {
    struct impl : cluster::hard_constraint::impl {
        cluster::hard_constraint_evaluator
        make_evaluator(const cluster::replicas_t&) const final {
            return [](const cluster::allocation_node&) { return true; };
        }
        ss::sstring name() const final { return "NOP evaluator"; }
    };

    return cluster::hard_constraint(std::make_unique<impl>());
}

cluster::hard_constraint random_evaluator() {
    auto gen_id = random_generators::get_int(0, 2);
    switch (gen_id) {
    case 0:
        return make_throwing_hard_evaluator();
    case 1:
        return make_false_evaluator();
    default:
        return make_nop_evaluator();
    }
}

FIXTURE_TEST(allocator_exception_safety_test, partition_allocator_fixture) {
    register_node(0, 2);
    register_node(1, 4);
    register_node(2, 7);

    auto capacity = max_capacity();
    for (int i = 0; i < 500; ++i) {
        auto req = make_allocation_request(1, 1);
        req.partitions[0].constraints.hard_constraints.push_back(
          ss::make_lw_shared<cluster::hard_constraint>(random_evaluator()));
        try {
            auto res = allocator.allocate(std::move(req)).get();
            if (res) {
                capacity--;
                for (auto& as : res.value()->get_assignments()) {
                    allocator.update_allocation_state(
                      as.replicas,
                      as.group,
                      cluster::partition_allocation_domains::common);
                }
            }

        } catch (...) {
        }
        BOOST_REQUIRE_EQUAL(capacity, max_capacity());
    }
}

FIXTURE_TEST(updating_nodes_properties, partition_allocator_fixture) {
    register_node(0, 2);
    register_node(1, 4);
    register_node(2, 7);

    // change node 1 core coung from 4 to 10
    for (int i = 0; i < 50; ++i) {
        // try to allocate single partition
        auto req = make_allocation_request(1, 1);
        auto res = allocator.allocate(std::move(req)).get();
        if (res) {
            for (auto& as : res.value()->get_assignments()) {
                allocator.update_allocation_state(
                  as.replicas,
                  as.group,
                  cluster::partition_allocation_domains::common);
            }
        }
    }
    auto it = allocator.state().allocation_nodes().find(model::node_id(1));
    auto allocated = it->second->allocated_partitions();
    auto new_rack = model::rack_id{"rack_A"};
    allocator.update_allocation_nodes(std::vector<model::broker>{
      create_broker(0, 2),
      create_broker(1, 10, new_rack),
      create_broker(2, 7)});
    BOOST_REQUIRE_EQUAL(it->second->cpus(), 10);
    // changing core count doesn't change number of allocated partitions
    BOOST_REQUIRE_EQUAL(it->second->allocated_partitions(), allocated);
    BOOST_REQUIRE_EQUAL(
      it->second->max_capacity(),
      10 * partition_allocator_fixture::partitions_per_shard
        - partition_allocator_fixture::partitions_reserve_shard0);
}

FIXTURE_TEST(change_replication_factor, partition_allocator_fixture) {
    register_node(0, 2);
    register_node(1, 4);
    auto req = make_allocation_request(1, 1);
    auto res = allocator.allocate(std::move(req)).get();

    BOOST_CHECK_EQUAL(res.has_value(), true);

    // try to allocate 3 replicas no 2 nodes - should fail
    auto expected_failure = allocator.reallocate_partition(
      cluster::partition_constraints(model::partition_id(0), 3),
      res.value()->get_assignments().front(),
      cluster::partition_allocation_domains::common);

    BOOST_CHECK_EQUAL(expected_failure.has_error(), true);

    // add new node and allocate again
    register_node(3, 4);

    auto expected_success = allocator.reallocate_partition(
      cluster::partition_constraints(model::partition_id(0), 3),
      res.value()->get_assignments().front(),
      cluster::partition_allocation_domains::common);

    BOOST_CHECK_EQUAL(expected_success.has_value(), true);
    validate_replica_set_diversity(expected_success.value().get_assignments());
}
FIXTURE_TEST(rack_aware_assignment_1, partition_allocator_fixture) {
    std::vector<std::tuple<int, model::rack_id, int>> id_rack_ncpu = {
      std::make_tuple(0, model::rack_id("rack-a"), 1),
      std::make_tuple(1, model::rack_id("rack-a"), 1),
      std::make_tuple(2, model::rack_id("rack-b"), 1),
      std::make_tuple(3, model::rack_id("rack-b"), 1),
      std::make_tuple(4, model::rack_id("rack-c"), 1),
      std::make_tuple(5, model::rack_id("rack-c"), 1),
    };

    for (auto [id, rack, ncpu] : id_rack_ncpu) {
        register_node(id, ncpu, rack);
    }

    auto units
      = allocator.allocate(make_allocation_request(1, 3)).get().value();

    BOOST_REQUIRE(!units->get_assignments().empty());
    auto group = units->get_assignments().front();
    std::set<model::node_id> nodes;
    for (auto [node_id, shard] : group.replicas) {
        nodes.insert(node_id);
    }
    BOOST_REQUIRE(nodes.size() == 3);
    BOOST_REQUIRE(
      nodes.contains(model::node_id(0))
      || nodes.contains(model::node_id(1))); // rack-a
    BOOST_REQUIRE(
      nodes.contains(model::node_id(2))
      || nodes.contains(model::node_id(3))); // rack-b
    BOOST_REQUIRE(
      nodes.contains(model::node_id(4))
      || nodes.contains(model::node_id(5))); // rack-c
}
FIXTURE_TEST(rack_aware_assignment_2, partition_allocator_fixture) {
    std::vector<std::tuple<int, model::rack_id, int>> id_rack_ncpu = {
      std::make_tuple(0, model::rack_id("rack-a"), 10),
      std::make_tuple(1, model::rack_id("rack-a"), 10),
      std::make_tuple(2, model::rack_id("rack-a"), 10),
      std::make_tuple(3, model::rack_id("rack-b"), 1),
    };

    for (auto [id, rack, ncpu] : id_rack_ncpu) {
        register_node(id, ncpu, rack);
    }

    auto units
      = allocator.allocate(make_allocation_request(1, 3)).get().value();

    BOOST_REQUIRE(!units->get_assignments().empty());
    auto group = units->get_assignments().front();
    std::set<ss::sstring> racks;
    for (auto [node_id, shard] : group.replicas) {
        auto rack_it = std::lower_bound(
          id_rack_ncpu.begin(),
          id_rack_ncpu.end(),
          std::make_tuple(node_id(), ss::sstring(), 0));
        BOOST_REQUIRE(rack_it != id_rack_ncpu.end());
        BOOST_REQUIRE(std::get<0>(*rack_it) == node_id());
        auto rack = std::get<1>(*rack_it);
        racks.insert(rack);
    }
    BOOST_REQUIRE(racks.size() == 2);
    BOOST_REQUIRE(racks.contains("rack-a"));
    BOOST_REQUIRE(racks.contains("rack-b"));
}

FIXTURE_TEST(even_distribution_pri_allocation, partition_allocator_fixture) {
    // allocate some regular partitions in the cluster but leave space
    register_node(0, 2);
    register_node(1, 2);
    register_node(2, 2);
    auto req_reg = make_allocation_request(max_capacity() / 4, 3);
    auto units_reg = allocator.allocate(std::move(req_reg)).get().value();
    // add empty nodes
    register_node(3, 2);
    register_node(4, 2);

    // do several rounds of priority allocation
    std::list<cluster::allocation_units::pointer> units;
    for (int i = 0; i != 21; ++i) {
        auto req = make_allocation_request(11 + i * 3, 1);
        // there is only one priority allocation domain yet
        static constexpr auto prio_domain
          = cluster::partition_allocation_domains::consumer_offsets;
        req.domain = prio_domain;
        units.push_back(
          std::move(allocator.allocate(std::move(req)).get().value()));

        // invariant: number of partitions allocated in the priority domain
        // across all nodes must be even, i.e. must not vary by more than one
        // partition
        const auto priority_part_capacity_minmax = std::minmax_element(
          allocator.state().allocation_nodes().cbegin(),
          allocator.state().allocation_nodes().cend(),
          [](const auto& lhs, const auto& rhs) {
              return lhs.second->domain_partition_capacity(prio_domain)
                     < rhs.second->domain_partition_capacity(prio_domain);
          });
        BOOST_CHECK_LE(
          priority_part_capacity_minmax.second->second
              ->domain_partition_capacity(prio_domain)
            - priority_part_capacity_minmax.first->second
                ->domain_partition_capacity(prio_domain),
          cluster::allocation_node::allocation_capacity(1));

        // invariant: sum(max_capacity()-domain_partition_capacity(d)) for d in
        // all_domains == max_capacity()-partition_capacity()
        // as long as node is not overallocated
        BOOST_CHECK(std::all_of(
          allocator.state().allocation_nodes().cbegin(),
          allocator.state().allocation_nodes().cend(),
          [](const auto& allocation_nodes_v) {
              const cluster::allocation_node& n = *allocation_nodes_v.second;
              return n.domain_partition_capacity(
                       cluster::partition_allocation_domains::consumer_offsets)
                       + n.domain_partition_capacity(
                         cluster::partition_allocation_domains::common)
                       - n.max_capacity()
                     == n.partition_capacity();
          }));

        // occassionaly deallocate prior allocations
        if (i % 2 == 0) {
            units.pop_front();
            // after deallocation, partitions in the priority domain are not
            // necessarily allocated evenly any more. However the next iteration
            // of the test would fill the irregularities because there will be
            // more partitions allocated (re: i*3) than what has been
            // deallocated
        }
    }
}
