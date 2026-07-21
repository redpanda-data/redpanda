/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/errc.h"
#include "cluster/scheduling/allocation_node.h"
#include "cluster/scheduling/allocation_state.h"
#include "cluster/scheduling/types.h"
#include "config/mock_property.h"
#include "config/property.h"
#include "container/chunked_vector.h"
#include "features/feature_table.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "model/namespace.h"

#include <seastar/core/sstring.hh>

#include <gtest/gtest.h>

#include <algorithm>
#include <iterator>
#include <memory>
#include <numeric>
#include <optional>
#include <vector>

namespace cluster {

class allocated_partition_test : public ::testing::Test {
protected:
    void fresh_partition_has_no_changes();
    void noop_reallocate_owns_nothing();
    void new_partition_all_replicas_are_new();
    void move_allocates_new_preserves_original();
    void try_revert_restores_original();
    void try_revert_on_fresh_partition_returns_no_update();
    void try_revert_rejects_invalid_steps();
    void destructor_reconciles_allocation_state();

    // Move the replica on `from` to a freshly allocated one on `to`; returns
    // the step so it can be reverted.
    reallocation_step move(
      allocated_partition& partition, model::node_id from, model::node_id to) {
        auto previous = partition.prepare_move(from);
        std::optional<model::broker_shard> previous_shard;
        if (previous) {
            previous_shard = previous->bs;
        }
        return reallocation_step(
          partition.add_replica(to, previous), previous_shard);
    }

    static model::broker_shard
    make_broker_shard(model::node_id node, uint32_t shard = 0) {
        return model::broker_shard{node, shard};
    }

    // node ids [1, count].
    static std::vector<model::node_id> node_id_range(int count) {
        std::vector<int> values(count);
        std::iota(values.begin(), values.end(), 1);
        std::vector<model::node_id> ids;
        ids.reserve(values.size());
        std::ranges::transform(values, std::back_inserter(ids), [](int value) {
            return model::node_id(value);
        });
        return ids;
    }

    static std::vector<model::broker_shard>
    broker_shards(const std::vector<model::node_id>& ids) {
        std::vector<model::broker_shard> shards;
        shards.reserve(ids.size());
        std::ranges::transform(
          ids, std::back_inserter(shards), [](model::node_id id) {
              return make_broker_shard(id);
          });
        return shards;
    }

    void register_node_range(const std::vector<model::node_id>& ids) {
        for (auto id : ids) {
            _state.register_node(
              std::make_unique<allocation_node>(
                id,
                /*cpus=*/4,
                config::mock_binding<uint32_t>(_partitions_per_shard),
                _partitions_reserve_shard0.bind(),
                _internal_topics.bind()));
        }
    }

    // Reflect an already-existing partition's replicas in the allocation-state
    // counters, mirroring replicas a previous allocation request placed.
    void add_existing(const std::vector<model::broker_shard>& replicas) {
        for (const auto& replica : replicas) {
            _state.add_allocation(replica);
            _state.add_final_count(replica);
        }
    }

    const allocation_node& node(model::node_id id) const {
        return *_state.allocation_nodes().at(id);
    }
    uint32_t allocated(model::node_id id) const {
        return node(id).allocated_partitions()();
    }
    uint32_t final_count(model::node_id id) const {
        return node(id).final_partitions()();
    }

    static bool
    has_replica(const allocated_partition& partition, model::node_id id) {
        return std::ranges::any_of(
          partition.replicas(),
          [id](const auto& replica) { return replica.node_id == id; });
    }

    model::ntp test_ntp{
      model::kafka_namespace, model::topic("test"), model::partition_id(0)};
    config::mock_property<std::vector<ss::sstring>> _internal_topics{{}};
    config::mock_property<uint32_t> _partitions_reserve_shard0{0};
    uint32_t _partitions_per_shard{1000};
    features::feature_table _features;
    allocation_state _state{
      _features,
      config::mock_binding<uint32_t>(_partitions_per_shard),
      _partitions_reserve_shard0.bind(),
      _internal_topics.bind()};
};

void allocated_partition_test::fresh_partition_has_no_changes() {
    auto nodes = node_id_range(3);
    register_node_range(nodes);

    allocated_partition partition(test_ntp, broker_shards(nodes), _state);

    EXPECT_FALSE(partition.has_changes());
    EXPECT_EQ(partition.replicas().size(), 3u);
    // Without a snapshot, is_original() falls back to scanning current
    // replicas.
    EXPECT_TRUE(partition.is_original(model::node_id(1)));
    EXPECT_FALSE(partition.is_original(model::node_id(9)));
}

// The no-op reallocate: add_replica() never runs, so no snapshot is taken and
// the pre-existing replicas must NOT be reported as newly allocated. Regression
// test for the allocation-counter underflow fix.
void allocated_partition_test::noop_reallocate_owns_nothing() {
    auto nodes = node_id_range(3);
    register_node_range(nodes);
    auto replicas = broker_shards(nodes);
    add_existing(replicas);

    allocated_partition partition(test_ntp, replicas, _state);
    chunked_vector<model::broker_shard> added;
    auto released = partition.release_new_partition(added);

    EXPECT_TRUE(added.empty());
    EXPECT_EQ(released.size(), 3u);
    for (auto id : nodes) {
        EXPECT_EQ(allocated(id), 1u);
        EXPECT_EQ(final_count(id), 1u);
    }
}

// A brand-new partition allocated from zero existing replicas: the snapshot is
// present but empty, so every replica is newly allocated and owned by these
// units.
void allocated_partition_test::new_partition_all_replicas_are_new() {
    auto nodes = node_id_range(3);
    register_node_range(nodes);

    allocated_partition partition(test_ntp, {}, _state);
    std::ranges::for_each(nodes, [&](model::node_id id) {
        partition.add_replica(id, std::nullopt);
    });

    EXPECT_TRUE(partition.has_changes());
    EXPECT_FALSE(partition.is_original(model::node_id(1)));
    for (auto id : nodes) {
        EXPECT_EQ(allocated(id), 1u);
        EXPECT_EQ(final_count(id), 1u);
    }

    chunked_vector<model::broker_shard> added;
    auto released = partition.release_new_partition(added);
    EXPECT_EQ(added.size(), 3u);
    EXPECT_EQ(released.size(), 3u);
}

// Moving a replica preserves the original replica's allocation (its shard is
// kept and its allocation is not released) and allocates a new one on the
// target node.
void allocated_partition_test::move_allocates_new_preserves_original() {
    register_node_range(node_id_range(4));
    auto replicas = broker_shards(node_id_range(3));
    add_existing(replicas);

    allocated_partition partition(test_ntp, replicas, _state);
    move(partition, model::node_id(3), model::node_id(4));

    EXPECT_TRUE(partition.has_changes());
    EXPECT_TRUE(partition.is_original(model::node_id(3)));
    EXPECT_FALSE(partition.is_original(model::node_id(4)));
    EXPECT_FALSE(has_replica(partition, model::node_id(3)));
    EXPECT_TRUE(has_replica(partition, model::node_id(4)));

    // node 3 keeps its allocation (still original) but loses its final count;
    // node 4 gains both.
    EXPECT_EQ(allocated(model::node_id(3)), 1u);
    EXPECT_EQ(final_count(model::node_id(3)), 0u);
    EXPECT_EQ(allocated(model::node_id(4)), 1u);
    EXPECT_EQ(final_count(model::node_id(4)), 1u);
}

// Reverting the move restores the original replica set and allocation counters
// without underflow. The snapshot latches: even though the replica set matches
// the original again, a further revert is treated as an in-progress
// reallocation.
void allocated_partition_test::try_revert_restores_original() {
    register_node_range(node_id_range(4));
    auto replicas = broker_shards(node_id_range(3));
    add_existing(replicas);

    allocated_partition partition(test_ntp, replicas, _state);
    auto move_step = move(partition, model::node_id(3), model::node_id(4));

    ASSERT_EQ(partition.try_revert(move_step), errc::success);

    EXPECT_FALSE(partition.has_changes());
    EXPECT_TRUE(has_replica(partition, model::node_id(3)));
    EXPECT_FALSE(has_replica(partition, model::node_id(4)));
    EXPECT_EQ(allocated(model::node_id(3)), 1u);
    EXPECT_EQ(final_count(model::node_id(3)), 1u);
    EXPECT_EQ(allocated(model::node_id(4)), 0u);
    EXPECT_EQ(final_count(model::node_id(4)), 0u);

    // Snapshot latches: node 4 is no longer a replica, so a second revert
    // reports node_does_not_exists, not no_update_in_progress.
    EXPECT_EQ(partition.try_revert(move_step), errc::node_does_not_exists);
}

void allocated_partition_test::
  try_revert_on_fresh_partition_returns_no_update() {
    register_node_range(node_id_range(4));
    allocated_partition partition(
      test_ntp, broker_shards(node_id_range(3)), _state);

    reallocation_step fabricated_step(
      make_broker_shard(model::node_id(4)),
      make_broker_shard(model::node_id(3)));
    EXPECT_EQ(
      partition.try_revert(fabricated_step), errc::no_update_in_progress);
}

void allocated_partition_test::try_revert_rejects_invalid_steps() {
    register_node_range(node_id_range(4));
    auto replicas = broker_shards(node_id_range(3));
    add_existing(replicas);

    allocated_partition partition(test_ntp, replicas, _state);
    auto move_step = move(partition, model::node_id(3), model::node_id(4));

    // current() node isn't a replica.
    EXPECT_EQ(
      partition.try_revert(
        reallocation_step(make_broker_shard(model::node_id(9)), std::nullopt)),
      errc::node_does_not_exists);

    // previous() node is already a replica.
    EXPECT_EQ(
      partition.try_revert(reallocation_step(
        move_step.current(), make_broker_shard(model::node_id(1)))),
      errc::invalid_request);
}

// Dropping an allocated_partition without releasing it must free the replicas
// it allocated and restore the final counts of originals that were moved away,
// leaving the allocation state exactly as it was before.
void allocated_partition_test::destructor_reconciles_allocation_state() {
    register_node_range(node_id_range(4));
    auto existing = node_id_range(3);
    add_existing(broker_shards(existing));

    {
        allocated_partition partition(
          test_ntp, broker_shards(existing), _state);
        move(partition, model::node_id(3), model::node_id(4));
        EXPECT_EQ(allocated(model::node_id(4)), 1u);
        EXPECT_EQ(final_count(model::node_id(3)), 0u);
    }

    for (auto id : existing) {
        EXPECT_EQ(allocated(id), 1u);
        EXPECT_EQ(final_count(id), 1u);
    }
    EXPECT_EQ(allocated(model::node_id(4)), 0u);
    EXPECT_EQ(final_count(model::node_id(4)), 0u);
}

TEST_F(allocated_partition_test, FreshPartitionHasNoChanges) {
    fresh_partition_has_no_changes();
}
TEST_F(allocated_partition_test, NoopReallocateOwnsNothing) {
    noop_reallocate_owns_nothing();
}
TEST_F(allocated_partition_test, NewPartitionAllReplicasAreNew) {
    new_partition_all_replicas_are_new();
}
TEST_F(allocated_partition_test, MoveAllocatesNewPreservesOriginal) {
    move_allocates_new_preserves_original();
}
TEST_F(allocated_partition_test, TryRevertRestoresOriginal) {
    try_revert_restores_original();
}
TEST_F(allocated_partition_test, TryRevertOnFreshPartitionReturnsNoUpdate) {
    try_revert_on_fresh_partition_returns_no_update();
}
TEST_F(allocated_partition_test, TryRevertRejectsInvalidSteps) {
    try_revert_rejects_invalid_steps();
}
TEST_F(allocated_partition_test, DestructorReconcilesAllocationState) {
    destructor_reconciles_allocation_state();
}

} // namespace cluster
