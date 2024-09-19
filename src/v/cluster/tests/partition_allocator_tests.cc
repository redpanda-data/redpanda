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
#include "cluster/scheduling/constraints.h"
#include "cluster/scheduling/types.h"
#include "cluster/tests/partition_allocator_fixture.h"
#include "model/metadata.h"
#include "raft/fundamental.h"
#include "random/fast_prng.h"
#include "random/generators.h"
#include "test_utils/fixture.h"

#include <seastar/core/sharded.hh>

#include <absl/container/flat_hash_set.h>
#include <boost/test/tools/old/interface.hpp>

ss::logger logger{"allocator_test"};

static void validate_replica_set_diversity(
  const std::vector<model::broker_shard>& replicas) {
    if (replicas.size() > 1) {
        auto sentinel = replicas.front();
        BOOST_TEST_REQUIRE(std::all_of(
          std::next(replicas.begin()),
          replicas.end(),
          [sentinel](const model::broker_shard bs) {
              return sentinel.node_id != bs.node_id;
          }));
    }
}

static void validate_replica_set_diversity(
  const ss::chunked_fifo<cluster::partition_assignment>& assignments) {
    for (const auto& assignment : assignments) {
        validate_replica_set_diversity(assignment.replicas);
    }
}

FIXTURE_TEST(register_node, partition_allocator_fixture) {
    register_node(0, 32);
    register_node(1, 64);
    register_node(2, 12);
    BOOST_REQUIRE(allocator().contains_node(model::node_id(0)));
    BOOST_REQUIRE(allocator().contains_node(model::node_id(1)));
    BOOST_REQUIRE(allocator().contains_node(model::node_id(2)));
    BOOST_REQUIRE_EQUAL(allocator().state().available_nodes(), 3);
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

    allocator().update_allocation_nodes(
      std::vector<model::broker>{create_broker(0, 32), create_broker(2, 12)});
    BOOST_REQUIRE(allocator().contains_node(model::node_id(0)));
    // allocator MUST still contain the node. it has to be marked as removed
    BOOST_REQUIRE(allocator().contains_node(model::node_id(1)));
    BOOST_REQUIRE(allocator()
                    .state()
                    .allocation_nodes()
                    .find(model::node_id(1))
                    ->second->is_removed());
    BOOST_REQUIRE(allocator().contains_node(model::node_id(2)));
    BOOST_REQUIRE_EQUAL(allocator().state().available_nodes(), 2);
}

FIXTURE_TEST(allocation_over_core_capacity, partition_allocator_fixture) {
    const auto partition_count
      = partition_allocator_fixture::partitions_per_shard + 1;
    register_node(0, 1);
    auto result
      = allocator().allocate(make_allocation_request(partition_count, 1)).get();
    BOOST_REQUIRE(result.has_error());
    BOOST_REQUIRE_EQUAL(
      result.assume_error(),
      cluster::make_error_code(
        cluster::errc::topic_invalid_partitions_core_limit));
}

FIXTURE_TEST(
  allocation_over_memory_capacity, partition_allocator_memory_limited_fixture) {
    register_node(0, 1);
    auto result = allocator().allocate(make_allocation_request(1, 1)).get();
    BOOST_REQUIRE(result.has_error());
    BOOST_REQUIRE_EQUAL(
      result.assume_error(),
      cluster::make_error_code(
        cluster::errc::topic_invalid_partitions_memory_limit));
}

FIXTURE_TEST(
  allocation_over_fds_capacity, partition_allocator_fd_limited_fixture) {
    register_node(0, 1);
    auto result = allocator().allocate(make_allocation_request(1, 1)).get();
    BOOST_REQUIRE(result.has_error());
    BOOST_REQUIRE_EQUAL(
      result.assume_error(),
      cluster::make_error_code(
        cluster::errc::topic_invalid_partitions_fd_limit));
}

FIXTURE_TEST(allocation_over_capacity, partition_allocator_fixture) {
    register_node(0, 6);
    register_node(1, 6);
    register_node(2, 6);

    saturate_all_machines();
    auto gr = allocator().state().last_group_id();
    BOOST_REQUIRE(
      allocator().allocate(make_allocation_request(1, 1)).get().has_error());
    // group id hasn't changed
    BOOST_REQUIRE_EQUAL(allocator().state().last_group_id(), gr);

    // Make the topic internal and retry, should work.
    kafka_internal_topics.update({tn.tp()});
    BOOST_REQUIRE(allocator().allocate(make_allocation_request(1, 1)).get());
    BOOST_REQUIRE_GT(allocator().state().last_group_id(), gr);

    // Undo the configuration, should fail again.
    kafka_internal_topics.update({});
    BOOST_REQUIRE(
      allocator().allocate(make_allocation_request(1, 1)).get().has_error());

    auto int_1 = model::topic_namespace{
      model::ns{"redpanda"}, model::topic{"controller"}};
    auto int_2 = model::topic_namespace{
      model::ns{"kafka_internal"}, model::topic{"controller"}};
    // Internal namespaces should work too.
    BOOST_REQUIRE(
      allocator().allocate(make_allocation_request(int_1, 1, 1)).get());
    BOOST_REQUIRE(
      allocator().allocate(make_allocation_request(int_2, 1, 1)).get());
}

FIXTURE_TEST(max_allocation, partition_allocator_fixture) {
    register_node(0, 2);
    register_node(1, 2);
    register_node(2, 2);
    // available capacity
    // 3 * 1000 * 2 - 3*2 = 5994

    auto req = make_allocation_request(max_capacity() / 3, 3);

    auto units = allocator().allocate(std::move(req)).get().value();

    BOOST_REQUIRE_EQUAL(units->get_assignments().size(), 1998);
    BOOST_REQUIRE_EQUAL(allocated_nodes_count(units->get_assignments()), 5994);
    BOOST_REQUIRE_EQUAL(allocator().state().last_group_id()(), 1998);
    validate_replica_set_diversity(units->get_assignments());

    // make sure there is no room left after
    auto single_partition_req = make_allocation_request(1, 1);
    auto result = allocator().allocate(std::move(single_partition_req)).get();
    BOOST_REQUIRE(result.has_error());
}

FIXTURE_TEST(unsatisfyable_diversity_assignment, partition_allocator_fixture) {
    register_node(0, 8);
    register_node(1, 4);
    register_node(2, 6);

    auto req = make_allocation_request(1, 5);
    auto allocs = allocator().allocate(std::move(req)).get();
    BOOST_TEST_REQUIRE(allocs.has_error());
    BOOST_REQUIRE_EQUAL(
      cluster::errc(allocs.error().value()),
      cluster::errc::topic_invalid_replication_factor);

    // ensure rollback happened
    BOOST_REQUIRE(all_nodes_empty());

    BOOST_REQUIRE_EQUAL(allocator().state().last_group_id()(), 0);
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
        auto result = allocator().allocate(std::move(req)).get();
        BOOST_REQUIRE(result);
        auto assignments = result.value()->copy_assignments();
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

    auto units_1 = allocator()
                     .allocate(make_allocation_request(
                       max_partitions_in_cluster - 1, 3))
                     .get()
                     .value();
    BOOST_REQUIRE_EQUAL(
      units_1->get_assignments().size(), max_partitions_in_cluster - 1);
    validate_replica_set_diversity(units_1->get_assignments());
    // allocate 2 partitions - one should fail, returning null & deallocating

    auto req_2 = make_allocation_request(2, 3);
    auto units_2 = allocator().allocate(std::move(req_2)).get();
    BOOST_REQUIRE(units_2.has_error());

    BOOST_REQUIRE_EQUAL(3, max_capacity());
    BOOST_REQUIRE_EQUAL(
      allocator().state().last_group_id()(), max_partitions_in_cluster - 1);
}
FIXTURE_TEST(max_deallocation, partition_allocator_fixture) {
    register_node(0, 3);
    register_node(1, 3);
    register_node(2, 3);
    // This test performs - 209994 partition assignments
    const auto max = max_capacity();

    {
        auto allocs = allocator()
                        .allocate(make_allocation_request(max / 3, 3))
                        .get()
                        .value();

        BOOST_REQUIRE_EQUAL(allocs->get_assignments().size() * 3, max);

        BOOST_REQUIRE_EQUAL(allocator().state().last_group_id()(), max / 3);
    }

    BOOST_REQUIRE_EQUAL(allocator().state().last_group_id()(), max / 3);
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
    allocator().add_allocations(replicas);
    // each node in the cluster holds one replica for each partition,
    // so it has to have topics * partitions shards allocated
    cluster::allocation_node::allocation_capacity allocated_shards{100 * 12};
    // Remaining capacity on node 0
    BOOST_REQUIRE_EQUAL(
      allocator()
        .state()
        .allocation_nodes()
        .find(model::node_id(0))
        ->second->allocated_partitions(),
      allocated_shards);
    // Remaining capacity on node 1
    BOOST_REQUIRE_EQUAL(
      allocator()
        .state()
        .allocation_nodes()
        .find(model::node_id(1))
        ->second->allocated_partitions(),
      allocated_shards);
    // Remaining capacity on node 2
    BOOST_REQUIRE_EQUAL(
      allocator()
        .state()
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
          = allocator().allocate(make_allocation_request(10, 3)).get().value();
        BOOST_REQUIRE_EQUAL(allocs->get_assignments().size(), 10);
        BOOST_REQUIRE_EQUAL(
          allocated_nodes_count(allocs->get_assignments()), 3 * 10);
    }

    BOOST_REQUIRE(all_nodes_empty());

    // we do not decrement the highest raft group
    BOOST_REQUIRE_EQUAL(allocator().state().last_group_id()(), 10);
}
FIXTURE_TEST(decommission_node, partition_allocator_fixture) {
    register_node(0, 32);
    register_node(1, 64);
    register_node(2, 12);
    allocator().decommission_node(model::node_id(1));

    // only two of machines are available as one of them is decommissioned
    BOOST_REQUIRE_EQUAL(allocator().state().available_nodes(), 2);
}

cluster::hard_constraint make_throwing_hard_evaluator() {
    struct impl : cluster::hard_constraint::impl {
        cluster::hard_constraint_evaluator make_evaluator(
          const cluster::allocated_partition&,
          std::optional<model::node_id>) const final {
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
        cluster::hard_constraint_evaluator make_evaluator(
          const cluster::allocated_partition&,
          std::optional<model::node_id>) const final {
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
        cluster::hard_constraint_evaluator make_evaluator(
          const cluster::allocated_partition&,
          std::optional<model::node_id>) const final {
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
        req.partitions.front().constraints.hard_constraints.push_back(
          ss::make_lw_shared<cluster::hard_constraint>(random_evaluator()));
        try {
            auto res = allocator().allocate(std::move(req)).get();
            if (res) {
                capacity--;
                for (auto& as : res.value()->get_assignments()) {
                    allocator().add_allocations_for_new_partition(
                      as.replicas, as.group);
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
        auto res = allocator().allocate(std::move(req)).get();
        if (res) {
            for (auto& as : res.value()->get_assignments()) {
                allocator().add_allocations_for_new_partition(
                  as.replicas, as.group);
            }
        }
    }
    auto it = allocator().state().allocation_nodes().find(model::node_id(1));
    auto allocated = it->second->allocated_partitions();
    auto new_rack = model::rack_id{"rack_A"};
    allocator().update_allocation_nodes(std::vector<model::broker>{
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
    register_node(0, 4);
    register_node(1, 4);
    auto req = make_allocation_request(2, 1);
    auto res = allocator().allocate(std::move(req)).get();

    BOOST_CHECK_EQUAL(res.has_value(), true);
    const auto& orig_assignments = res.value()->get_assignments();

    auto make_reallocate_req = [&] {
        cluster::allocation_request req(tn);
        for (const auto& assignment : orig_assignments) {
            req.partitions.push_back(
              cluster::partition_constraints(assignment, 3));
        }
        return req;
    };

    // try to allocate 3 replicas on 2 nodes - should fail
    auto expected_failure = allocator().allocate(make_reallocate_req()).get();

    BOOST_CHECK_EQUAL(expected_failure.has_error(), true);

    // add new node and allocate again
    register_node(2, 4);

    auto expected_success = allocator().allocate(make_reallocate_req()).get();

    BOOST_CHECK_EQUAL(expected_success.has_value(), true);
    BOOST_REQUIRE_EQUAL(
      expected_success.value()->get_assignments().size(),
      orig_assignments.size());
    const auto& new_assignments = expected_success.value()->get_assignments();

    for (auto [it1, it2] = std::make_pair(
           orig_assignments.begin(), new_assignments.begin());
         it1 != orig_assignments.end();
         ++it1, ++it2) {
        BOOST_CHECK_EQUAL(it1->id, it2->id);
        BOOST_CHECK_EQUAL(it1->group, it2->group);
        BOOST_CHECK_EQUAL(it2->replicas.size(), 3);
    }
    validate_replica_set_diversity(expected_success.value()->get_assignments());
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
      = allocator().allocate(make_allocation_request(1, 3)).get().value();

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
      = allocator().allocate(make_allocation_request(1, 3)).get().value();

    BOOST_REQUIRE(!units->get_assignments().empty());
    auto group = units->get_assignments().front();
    std::set<ss::sstring> racks;
    for (auto [node_id, shard] : group.replicas) {
        auto rack_it = std::lower_bound(
          id_rack_ncpu.begin(),
          id_rack_ncpu.end(),
          std::make_tuple(node_id(), model::rack_id(""), 0));
        BOOST_REQUIRE(rack_it != id_rack_ncpu.end());
        BOOST_REQUIRE(std::get<0>(*rack_it) == node_id());
        auto rack = std::get<1>(*rack_it);
        racks.insert(rack);
    }
    BOOST_REQUIRE(racks.size() == 2);
    BOOST_REQUIRE(racks.contains("rack-a"));
    BOOST_REQUIRE(racks.contains("rack-b"));
}

void check_allocated_counts(
  const cluster::partition_allocator& allocator,
  const std::vector<size_t>& expected) {
    std::vector<size_t> counts;
    for (const auto& [id, node] : allocator.state().allocation_nodes()) {
        BOOST_REQUIRE(id() == counts.size());
        counts.push_back(node->allocated_partitions());
    }
    logger.debug("allocated counts: {}, expected: {}", counts, expected);
    BOOST_CHECK_EQUAL(counts, expected);
};

void check_final_counts(
  const cluster::partition_allocator& allocator,
  const std::vector<size_t>& expected) {
    std::vector<size_t> counts;
    for (const auto& [id, node] : allocator.state().allocation_nodes()) {
        BOOST_REQUIRE(id() == counts.size());
        counts.push_back(node->final_partitions());
    }
    logger.debug("final counts: {}, expected: {}", counts, expected);
    BOOST_CHECK_EQUAL(counts, expected);
};

std::vector<model::node_id>
to_node_ids(const std::vector<model::broker_shard>& replicas) {
    std::vector<model::node_id> ids;
    ids.reserve(replicas.size());
    for (const auto& bs : replicas) {
        ids.push_back(bs.node_id);
    }
    std::sort(ids.begin(), ids.end());
    return ids;
}

FIXTURE_TEST(incrementally_reallocate_replicas, partition_allocator_fixture) {
    register_node(0, 1);
    register_node(1, 1);
    register_node(2, 1);

    // allocate a partition with 3 replicas on 3 nodes
    auto req = make_allocation_request(1, 3);
    auto res = allocator().allocate(std::move(req)).get();
    auto original_replicas = res.value()->get_assignments().front().replicas;

    // add another node to move replicas to and from
    register_node(3, 1);

    check_allocated_counts(allocator(), {1, 1, 1, 0});
    check_final_counts(allocator(), {1, 1, 1, 0});

    {
        auto partition_id = res.value()->get_assignments().front().id;
        auto ntp = model::ntp{tn.ns, tn.tp, partition_id};
        cluster::allocated_partition reallocated
          = allocator().make_allocated_partition(
            std::move(ntp), original_replicas);

        cluster::allocation_constraints not_on_old_nodes;
        not_on_old_nodes.add(cluster::distinct_from(original_replicas));

        // move to node 3
        auto moved = allocator().reallocate_replica(
          reallocated, model::node_id{0}, not_on_old_nodes);
        BOOST_REQUIRE(moved.has_value());
        BOOST_REQUIRE_EQUAL(moved.value().current().node_id, model::node_id{3});
        BOOST_REQUIRE(reallocated.has_changes());
        BOOST_REQUIRE_EQUAL(
          reallocated.replicas().at(2).node_id, model::node_id{3});

        check_allocated_counts(allocator(), {1, 1, 1, 1});
        check_final_counts(allocator(), {0, 1, 1, 1});

        // there is no replica on node 0 any more
        auto moved2 = allocator().reallocate_replica(
          reallocated, model::node_id{0}, not_on_old_nodes);
        BOOST_REQUIRE(moved2.has_error());
        BOOST_REQUIRE_EQUAL(
          moved2.error(), cluster::errc::node_does_not_exists);

        // node 3 already has a replica so the replica on node 1 has nowhere to
        // move
        auto moved3 = allocator().reallocate_replica(
          reallocated, model::node_id{1}, not_on_old_nodes);
        BOOST_REQUIRE(moved3.has_error());
        BOOST_REQUIRE_EQUAL(
          moved3.error(), cluster::errc::no_eligible_allocation_nodes);

        check_allocated_counts(allocator(), {1, 1, 1, 1});
        check_final_counts(allocator(), {0, 1, 1, 1});

        // replicas can move to the same place
        auto moved4 = allocator().reallocate_replica(
          reallocated, model::node_id{3}, not_on_old_nodes);
        BOOST_REQUIRE(moved4.has_value());
        BOOST_REQUIRE_EQUAL(
          moved4.value().current().node_id, model::node_id{3});
        BOOST_REQUIRE_EQUAL(
          reallocated.replicas().at(2).node_id, model::node_id{3});

        check_allocated_counts(allocator(), {1, 1, 1, 1});
        check_final_counts(allocator(), {0, 1, 1, 1});

        std::vector node_0(
          {model::broker_shard{.node_id = model::node_id{0}, .shard = 0}});
        cluster::allocation_constraints not_on_node0;
        not_on_node0.add(cluster::distinct_from(node_0));

        auto moved5 = allocator().reallocate_replica(
          reallocated, model::node_id{2}, not_on_node0);
        BOOST_REQUIRE(moved5.has_value());
        BOOST_REQUIRE_EQUAL(
          moved5.value().current().node_id, model::node_id{2});
        BOOST_REQUIRE_EQUAL(
          reallocated.replicas().at(2).node_id, model::node_id{2});

        check_allocated_counts(allocator(), {1, 1, 1, 1});
        check_final_counts(allocator(), {0, 1, 1, 1});

        std::vector new_replicas(reallocated.replicas());
        cluster::allocation_constraints not_on_new_nodes;
        not_on_new_nodes.add(cluster::distinct_from(new_replicas));

        // move reallocated replica back to node 0
        auto moved6 = allocator().reallocate_replica(
          reallocated, model::node_id{3}, not_on_new_nodes);
        BOOST_REQUIRE(moved6.has_value());
        BOOST_REQUIRE_EQUAL(
          moved6.value().current().node_id, model::node_id{0});
        BOOST_REQUIRE(!reallocated.has_changes());
        BOOST_REQUIRE_EQUAL(
          reallocated.replicas().at(2).node_id, model::node_id{0});

        check_allocated_counts(allocator(), {1, 1, 1, 0});
        check_final_counts(allocator(), {1, 1, 1, 0});

        // do another move so that we have something to revert
        auto moved7 = allocator().reallocate_replica(
          reallocated, model::node_id{1}, not_on_old_nodes);
        BOOST_REQUIRE(moved7.has_value());
        BOOST_REQUIRE_EQUAL(
          moved7.value().current().node_id, model::node_id{3});
        BOOST_REQUIRE(reallocated.has_changes());
        BOOST_REQUIRE_EQUAL(
          reallocated.replicas().at(2).node_id, model::node_id{3});

        check_allocated_counts(allocator(), {1, 1, 1, 1});
        check_final_counts(allocator(), {1, 0, 1, 1});
    }

    check_allocated_counts(allocator(), {1, 1, 1, 0});
    check_final_counts(allocator(), {1, 1, 1, 0});
}

FIXTURE_TEST(reallocate_partition_with_move, partition_allocator_fixture) {
    register_node(0, 1);
    register_node(1, 1);
    register_node(2, 1);

    // allocate a partition with 3 replicas on 3 nodes
    auto req = make_allocation_request(1, 3);
    auto res = allocator().allocate(std::move(req)).get();
    auto original_assignment = res.value()->get_assignments().front();
    model::ntp ntp(tn.ns, tn.tp, original_assignment.id);

    // add a couple more nodes
    register_node(3, 1);
    register_node(4, 1);

    check_allocated_counts(allocator(), {1, 1, 1, 0, 0});
    check_final_counts(allocator(), {1, 1, 1, 0, 0});

    cluster::allocation_constraints not_on_old_nodes;
    not_on_old_nodes.add(cluster::distinct_from(original_assignment.replicas));

    {
        auto res = allocator().reallocate_partition(
          ntp,
          original_assignment.replicas,
          {model::node_id{0}, model::node_id{1}},
          not_on_old_nodes,
          nullptr);
        BOOST_REQUIRE(res.has_value());
        BOOST_REQUIRE(res.value().has_changes());
        BOOST_REQUIRE_EQUAL(res.value().replicas().size(), 3);
        absl::flat_hash_set<model::node_id> replicas_set;
        for (const auto& bs : res.value().replicas()) {
            replicas_set.insert(bs.node_id);
        }
        BOOST_REQUIRE_EQUAL(replicas_set.size(), 3);
        BOOST_REQUIRE(!replicas_set.contains(model::node_id{0}));
        BOOST_REQUIRE(!replicas_set.contains(model::node_id{1}));

        check_allocated_counts(allocator(), {1, 1, 1, 1, 1});
        check_final_counts(allocator(), {0, 0, 1, 1, 1});
    }

    {
        auto res = allocator().reallocate_partition(
          ntp,
          original_assignment.replicas,
          {model::node_id{0}, model::node_id{1}, model::node_id{2}},
          not_on_old_nodes,
          nullptr);
        BOOST_REQUIRE(res.has_error());
        BOOST_REQUIRE_EQUAL(
          res.error(), cluster::errc::no_eligible_allocation_nodes);
    }

    {
        auto res = allocator().reallocate_partition(
          ntp,
          original_assignment.replicas,
          {model::node_id{3}},
          not_on_old_nodes,
          nullptr);
        BOOST_REQUIRE(res.has_error());
        BOOST_REQUIRE_EQUAL(res.error(), cluster::errc::node_does_not_exists);
    }
}

FIXTURE_TEST(
  preserve_shard_for_original_replicas, partition_allocator_fixture) {
    register_node(0, 4);
    register_node(1, 4);
    register_node(2, 4);
    register_node(3, 4);

    cluster::allocation_constraints not_on_0;
    std::vector<model::broker_shard> node_0{{model::node_id{0}, 0}};
    not_on_0.add(cluster::distinct_from(node_0));

    cluster::allocation_constraints not_on_3;
    std::vector<model::broker_shard> node_3{{model::node_id{3}, 0}};
    not_on_3.add(cluster::distinct_from(node_3));

    cluster::allocation_units::pointer partition_1;
    {
        auto res = allocator().allocate(make_allocation_request(1, 4)).get();
        BOOST_REQUIRE(res);
        partition_1 = std::move(res.value());
    }

    // This is the partition that we are going to modify and check that the
    // original shards are preserved.

    cluster::allocation_units::pointer partition_2;
    {
        auto req = make_allocation_request(1, 3);
        req.partitions.front().constraints.add(cluster::distinct_from(node_3));
        auto res = allocator().allocate(std::move(req)).get();
        BOOST_REQUIRE(res);
        partition_2 = std::move(res.value());
    }

    auto original_replicas = partition_2->get_assignments().front().replicas;
    absl::flat_hash_map<model::node_id, uint32_t> replica2shard;
    for (const auto& bs : original_replicas) {
        replica2shard.emplace(bs.node_id, bs.shard);
    }

    check_allocated_counts(allocator(), {2, 2, 2, 1});
    check_final_counts(allocator(), {2, 2, 2, 1});

    auto id = partition_2->get_assignments().front().id;
    auto ntp = model::ntp{tn.ns, tn.tp, id};
    cluster::allocated_partition reallocated
      = allocator().make_allocated_partition(std::move(ntp), original_replicas);

    auto moved = allocator().reallocate_replica(
      reallocated, model::node_id{0}, not_on_0);
    BOOST_REQUIRE(moved);
    BOOST_REQUIRE_EQUAL(moved.value().current().node_id, model::node_id{3});
    replica2shard[moved.value().current().node_id]
      = moved.value().current().shard;

    // Delete partition 1. This frees up the first shard on all nodes, making it
    // more attractive. But replicas on nodes 0, 1, and 2 should still end up on
    // shard 2
    partition_1.reset();
    check_allocated_counts(allocator(), {1, 1, 1, 1});
    check_final_counts(allocator(), {0, 1, 1, 1});

    // Reallocate replica on node 1 to itself.
    moved = allocator().reallocate_replica(
      reallocated, model::node_id{1}, not_on_0);
    BOOST_REQUIRE(moved);
    BOOST_REQUIRE_EQUAL(moved.value().current().node_id, model::node_id{1});
    BOOST_REQUIRE_EQUAL(
      moved.value().current().shard,
      replica2shard.at(moved.value().current().node_id));

    // Reallocate replica on node 3 to itself.
    moved = allocator().reallocate_replica(
      reallocated, model::node_id{3}, not_on_0);
    BOOST_REQUIRE(moved);
    BOOST_REQUIRE_EQUAL(moved.value().current().node_id, model::node_id{3});
    // Node 3 is not in the original set, so we don't care that the shard is
    // preserved.
    BOOST_REQUIRE_NE(
      moved.value().current().shard,
      replica2shard.at(moved.value().current().node_id));

    // Reallocate replica on node 3 back to the original node 0.
    moved = allocator().reallocate_replica(
      reallocated, model::node_id{3}, not_on_3);
    BOOST_REQUIRE(moved);
    BOOST_REQUIRE_EQUAL(moved.value().current().node_id, model::node_id{0});
    BOOST_REQUIRE_EQUAL(
      moved.value().current().shard,
      replica2shard.at(moved.value().current().node_id));

    // The end result is the same: replicas on nodes 0, 1, 2
    BOOST_REQUIRE(!reallocated.has_changes());
}

static cluster::allocation_constraints on_node(model::node_id id) {
    class impl : public cluster::hard_constraint::impl {
    public:
        explicit impl(model::node_id id)
          : _id(id) {}

        cluster::hard_constraint_evaluator make_evaluator(
          const cluster::allocated_partition&,
          std::optional<model::node_id>) const final {
            return [this](const cluster::allocation_node& node) {
                return node.id() == _id;
            };
        }

        ss::sstring name() const final {
            return ssx::sformat("on node: {}", _id);
        }

    private:
        model::node_id _id;
    };

    cluster::allocation_constraints ret;
    ret.add(cluster::hard_constraint(std::make_unique<impl>(id)));
    return ret;
}

FIXTURE_TEST(revert_allocation_step, partition_allocator_fixture) {
    register_node(0, 1);
    register_node(1, 1);
    register_node(2, 1);

    // allocate a partition with 3 replicas on 3 nodes
    auto req = make_allocation_request(1, 3);
    auto res = allocator().allocate(std::move(req)).get();
    auto original_replicas = res.value()->get_assignments().front().replicas;

    // add a couple more nodes
    register_node(3, 1);
    register_node(4, 1);

    auto n = [](int id) { return model::node_id{id}; };

    {
        auto partition_id = res.value()->get_assignments().front().id;
        auto ntp = model::ntp{tn.ns, tn.tp, partition_id};
        cluster::allocated_partition reallocated
          = allocator().make_allocated_partition(
            std::move(ntp), original_replicas);
        auto step1 = allocator().reallocate_replica(
          reallocated, n(0), on_node(n(3)));
        BOOST_REQUIRE(step1);
        BOOST_REQUIRE_EQUAL(step1.value().current().node_id, n(3));
        BOOST_REQUIRE(step1.value().previous());
        BOOST_REQUIRE_EQUAL(step1.value().previous()->node_id, n(0));

        auto step2 = allocator().reallocate_replica(
          reallocated, n(3), on_node(n(4)));
        BOOST_REQUIRE(step2);

        auto ec = reallocated.try_revert(step1.value());
        BOOST_REQUIRE_EQUAL(ec, cluster::errc::node_does_not_exists);

        ec = reallocated.try_revert(step2.value());
        BOOST_REQUIRE_EQUAL(ec, cluster::errc::success);
        BOOST_REQUIRE_EQUAL(
          to_node_ids(reallocated.replicas()), std::vector({n(1), n(2), n(3)}));
        check_allocated_counts(allocator(), {1, 1, 1, 1, 0});
        check_final_counts(allocator(), {0, 1, 1, 1, 0});
    }
}

FIXTURE_TEST(topic_aware_reallocate_partition, partition_allocator_fixture) {
    register_node(0, 4);

    cluster::allocation_units::pointer topic1;
    {
        auto res = allocator().allocate(make_allocation_request(10, 1)).get();
        topic1 = std::move(res.value());
    }
    check_allocated_counts(allocator(), {10});

    register_node(1, 4);
    register_node(2, 4);
    register_node(3, 4);

    cluster::allocation_units::pointer topic2;
    {
        auto res = allocator().allocate(make_allocation_request(3, 1)).get();
        topic2 = std::move(res.value());
    }
    // 1-replica partitions of topic2 should end up on empty nodes
    check_allocated_counts(allocator(), {10, 1, 1, 1});

    cluster::node2count_t topic2_counts;
    for (const auto& p_as : topic2->get_assignments()) {
        for (const auto& bs : p_as.replicas) {
            topic2_counts[bs.node_id] += 1;
        }
    }
    BOOST_CHECK(!topic2_counts.contains(model::node_id{0}));

    auto p0_replicas = topic2->get_assignments().front().replicas;
    BOOST_REQUIRE_EQUAL(p0_replicas.size(), 1);
    model::broker_shard original = p0_replicas[0];

    cluster::allocation_constraints constraints;
    constraints.add(cluster::distinct_from(p0_replicas));

    auto reallocated = allocator().reallocate_partition(
      model::ntp(tn.ns, tn.tp, model::partition_id(0)),
      {original},
      {original.node_id},
      std::move(constraints),
      &topic2_counts);
    BOOST_REQUIRE(reallocated);

    // reallocated partition should have its single replica on node 0, despite
    // partitions of topic1 being there.
    BOOST_REQUIRE_EQUAL(reallocated.value().replicas().size(), 1);
    BOOST_CHECK_EQUAL(
      reallocated.value().replicas()[0].node_id, model::node_id{0});
    BOOST_CHECK_EQUAL(topic2_counts.at(model::node_id{0}), 1);
    size_t total = 0;
    for (const auto& [n, count] : topic2_counts) {
        total += count;
    }
    BOOST_CHECK_EQUAL(total, 3);
}
