// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/tm_stm.h"
#include "cluster/tx_coordinator_mapper.h"
#include "cluster/tx_hash_ranges.h"

#include <seastar/testing/thread_test_case.hh>

#include <cstdint>

void check_hash_range_distribuiton(int32_t partitions_amount) {
    cluster::tx_hash_ranges_set hash_ranges{};
    for (int i = 0; i < partitions_amount; ++i) {
        hash_ranges.ranges.push_back(cluster::default_hash_range(
          model::partition_id(i), partitions_amount));
    }
    BOOST_REQUIRE_EQUAL(hash_ranges.ranges[0].first, cluster::tx_id_hash(0));
    for (int i = 0; i < partitions_amount - 1; ++i) {
        BOOST_REQUIRE_EQUAL(
          hash_ranges.ranges[i].last + cluster::tx_id_hash(1),
          hash_ranges.ranges[i + 1].first);
    }
    BOOST_REQUIRE_EQUAL(
      hash_ranges.ranges[partitions_amount - 1].last,
      cluster::tx_id_hash::max());

    // Next checks not applicable to 1 partition
    if (partitions_amount == 1) {
        return;
    }

    // Check size of hash range for each partition
    for (int i = 0; i < partitions_amount - 2; ++i) {
        BOOST_REQUIRE_EQUAL(
          hash_ranges.ranges[i].last - hash_ranges.ranges[i].first,
          hash_ranges.ranges[i + 1].last - hash_ranges.ranges[i + 1].first);
    }
    uint32_t last_segment_size
      = hash_ranges.ranges[partitions_amount - 1].last
        - hash_ranges.ranges[partitions_amount - 1].first;
    uint32_t first_segment_size = hash_ranges.ranges[0].last
                                  - hash_ranges.ranges[0].first;
    BOOST_REQUIRE_LE(first_segment_size, last_segment_size);
    BOOST_REQUIRE_LE(last_segment_size, first_segment_size + partitions_amount);
}

SEASTAR_THREAD_TEST_CASE(tm_hash_range_test) {
    check_hash_range_distribuiton(1);
    check_hash_range_distribuiton(16);
    check_hash_range_distribuiton(1000);
}

void check_get_partition_from_default_distribution(int32_t partitions_amount) {
    BOOST_REQUIRE_EQUAL(
      cluster::get_tx_coordinator_partition(
        cluster::tx_id_hash(0), partitions_amount),
      model::partition_id(0));

    BOOST_REQUIRE_EQUAL(
      cluster::get_tx_coordinator_partition(
        cluster::tx_id_hash::max(), partitions_amount),
      model::partition_id(partitions_amount - 1));

    cluster::tx_hash_ranges_set hash_ranges{};
    for (int i = 0; i < partitions_amount; ++i) {
        hash_ranges.ranges.push_back(cluster::default_hash_range(
          model::partition_id(i), partitions_amount));
    }

    for (int i = 0; i < partitions_amount; ++i) {
        BOOST_REQUIRE_EQUAL(
          cluster::get_tx_coordinator_partition(
            hash_ranges.ranges[i].first, partitions_amount),
          model::partition_id(i));

        BOOST_REQUIRE_EQUAL(
          cluster::get_tx_coordinator_partition(
            hash_ranges.ranges[i].last, partitions_amount),
          model::partition_id(i));

        BOOST_REQUIRE_EQUAL(
          cluster::get_tx_coordinator_partition(
            cluster::tx_id_hash(
              (uint64_t(hash_ranges.ranges[i].first)
               + uint64_t(hash_ranges.ranges[i].last))
              / 2),
            partitions_amount),
          model::partition_id(i));
    }
}

SEASTAR_THREAD_TEST_CASE(get_partition_from_default_distribution_test) {
    check_get_partition_from_default_distribution(1);
    check_get_partition_from_default_distribution(16);
    check_get_partition_from_default_distribution(1000);
}
