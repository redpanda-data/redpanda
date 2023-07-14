/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "config/configuration.h"
#include "config/property.h"
#include "random/generators.h"
#include "resource_mgmt/storage.h"

#include <seastar/core/future.hh>
#include <seastar/testing/thread_test_case.hh>

using ep = storage::eviction_policy;

SEASTAR_THREAD_TEST_CASE(test_partition_iteration) {
    // generate unique group ids across all tests
    int64_t group_id = 0;

    const auto make_schedule = [&](int cores, int partitions) {
        std::vector<ep::shard_partitions> shards;
        shards.resize(cores);
        for (int i = 0; i < partitions; ++i) {
            auto core = random_generators::get_int(0, cores - 1);
            shards[core].partitions.push_back(ep::partition{
              .group = raft::group_id(group_id++),
            });
        }
        return ep::schedule(std::move(shards), partitions);
    };

    // shared across tests
    size_t cursor = 0;

    for (int repeat = 0; repeat < 100; ++repeat) {
        auto schedule = make_schedule(
          random_generators::get_int(1, 5), random_generators::get_int(1, 20));
        BOOST_REQUIRE(schedule.sched_size > 0);
        schedule.seek(cursor);

        // capture the ordering of partitions in the first iteration
        auto partition = schedule.current();
        const auto first = partition;
        std::vector<raft::group_id> first_loop_ordering;
        for (size_t i = 0; i < schedule.sched_size; ++i) {
            first_loop_ordering.push_back(partition->group);
            schedule.next();
            ++cursor;
            partition = schedule.current();
        }
        BOOST_REQUIRE(partition == first);
        BOOST_REQUIRE(first_loop_ordering.size() == schedule.sched_size);

        // capture the ordering of partitions in the second iteration
        std::vector<raft::group_id> second_loop_ordering;
        for (size_t i = 0; i < schedule.sched_size; ++i) {
            second_loop_ordering.push_back(partition->group);
            schedule.next();
            ++cursor;
            partition = schedule.current();
        }
        BOOST_REQUIRE(partition == first);
        // the ordering should be the same
        BOOST_REQUIRE(first_loop_ordering == second_loop_ordering);

        // no duplicates should be in the ordering
        std::sort(first_loop_ordering.begin(), first_loop_ordering.end());
        BOOST_REQUIRE(
          std::adjacent_find(
            first_loop_ordering.begin(), first_loop_ordering.end())
          == first_loop_ordering.end());
    }
}
