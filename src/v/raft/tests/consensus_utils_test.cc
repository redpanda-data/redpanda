// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include <iterator>
#include <limits>
#define BOOST_TEST_MODULE raft
#include "raft/consensus_utils.h"

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
