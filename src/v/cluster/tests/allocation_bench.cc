// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tests/partition_allocator_tester.h"
#include "raft/types.h"

#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/testing/perf_tests.hh>

using namespace cluster; // NOLINT

PERF_TEST_F(partition_allocator_tester, allocation_3) {
    auto cfg = gen_topic_configuration(1, 3);

    perf_tests::start_measuring_time();
    auto vals = pa.allocate(cfg);
    perf_tests::do_not_optimize(vals);
    perf_tests::stop_measuring_time();
}
PERF_TEST_F(partition_allocator_tester, deallocation_3) {
    auto cfg = gen_topic_configuration(1, 3);
    auto vals = std::move(pa.allocate(cfg).value());
    perf_tests::do_not_optimize(vals);
    perf_tests::start_measuring_time();
    for (auto& v : vals.get_assignments()) {
        for (auto& bs : v.replicas) {
            pa.deallocate(bs);
        }
    }
    perf_tests::stop_measuring_time();
}
PERF_TEST_F(partition_allocator_tester, recovery) {
    const auto node_capacity = (cpus_per_node
                                * allocation_node::max_allocations_per_core)
                               - allocation_node::core0_extra_weight;
    const auto partitions_per_topic = 7;
    auto topics = node_capacity / partitions_per_topic;
    auto md = create_topic_metadata(topics, partitions_per_topic);
    perf_tests::start_measuring_time();
    pa.update_allocation_state(md, raft::group_id(partitions_per_topic));
    perf_tests::stop_measuring_time();
}
