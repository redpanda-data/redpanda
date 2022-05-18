// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "random/generators.h"

#include <iterator>
#include <limits>
#define BOOST_TEST_MODULE raft
#include "model/tests/random_batch.h"
#include "raft/consensus_utils.h"
#include "storage/tests/utils/disk_log_builder.h"

#include <seastar/core/circular_buffer.hh>

#include <boost/test/unit_test.hpp>

model::broker test_broker(int32_t id) {
    return model::broker(
      model::node_id{id},
      net::unresolved_address("127.0.0.1", 9092),
      net::unresolved_address("127.0.0.1", 1234),
      std::nullopt,
      model::broker_properties{});
}
std::vector<model::broker> test_brokers() {
    return {test_broker(1), test_broker(2), test_broker(3)};
}

BOOST_AUTO_TEST_CASE(test_lookup_existing) {
    auto brokers = test_brokers();
    auto it = raft::details::find_machine(
      std::begin(brokers), std::end(brokers), model::node_id(2));
    BOOST_CHECK(it != brokers.end());
    BOOST_CHECK(it->id() == 2);
}

BOOST_AUTO_TEST_CASE(test_lookup_non_existing) {
    auto brokers = test_brokers();
    auto it = raft::details::find_machine(
      std::begin(brokers), std::end(brokers), model::node_id(4));

    BOOST_CHECK(it == brokers.end());
}

BOOST_AUTO_TEST_CASE(test_filling_gaps) {
    auto batches = model::test::make_random_batches(
      model::offset(20), 50, true);

    // cut some holes in log
    for (size_t i = 0; i < 10; ++i) {
        auto idx = random_generators::get_int(batches.size() - 1);
        auto it = std::next(batches.begin(), idx);
        batches.erase(it, std::next(it));
    }

    model::offset first_expected = model::offset(10);

    auto without_gaps = raft::details::make_ghost_batches_in_gaps(
      first_expected, std::move(batches));

    for (auto& b : without_gaps) {
        BOOST_REQUIRE_EQUAL(b.base_offset(), first_expected);
        first_expected = model::next_offset(b.last_offset());
    }
}

BOOST_AUTO_TEST_CASE(test_filling_first_gap) {
    auto batches = model::test::make_random_batches(model::offset(1), 50, true);

    // cut some holes in log
    for (size_t i = 0; i < 10; ++i) {
        auto idx = random_generators::get_int(batches.size() - 1);
        auto it = std::next(batches.begin(), idx);
        batches.erase(it, std::next(it));
    }

    model::offset first_expected = model::offset(0);

    auto without_gaps = raft::details::make_ghost_batches_in_gaps(
      first_expected, std::move(batches));

    for (auto& b : without_gaps) {
        BOOST_REQUIRE_EQUAL(b.base_offset(), first_expected);
        first_expected = model::next_offset(b.last_offset());
    }
}

BOOST_AUTO_TEST_CASE(test_filling_gaps_larger_than_batch_size) {
    auto batches = model::test::make_random_batches(
      model::offset(20), 50, true);

    // cut some holes in log
    for (size_t i = 0; i < 10; ++i) {
        auto idx = random_generators::get_int(batches.size() - 1);
        auto it = std::next(batches.begin(), idx);
        batches.erase(it, std::next(it));
    }

    auto batches_after = model::test::make_random_batches(
      model::offset(
        3 * static_cast<int64_t>(std::numeric_limits<int32_t>::max())
        + random_generators::get_int<int64_t>(
          std::numeric_limits<int32_t>::max())),
      10,
      true);

    std::move(
      batches_after.begin(), batches_after.end(), std::back_inserter(batches));

    model::offset first_expected = model::offset(10);

    auto without_gaps = raft::details::make_ghost_batches_in_gaps(
      first_expected, std::move(batches));

    for (auto& b : without_gaps) {
        BOOST_REQUIRE_EQUAL(b.base_offset(), first_expected);
        first_expected = model::next_offset(b.last_offset());
    }
}
