// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/find_coordinator.h"
#include "redpanda/tests/fixture.h"
#include "resource_mgmt/io_priority.h"
#include "test_utils/async.h"

#include <seastar/core/smp.hh>

#include <chrono>
#include <limits>

FIXTURE_TEST(find_coordinator_unsupported_key, redpanda_thread_fixture) {
    auto client = make_kafka_client().get0();
    client.connect().get();

    using underlying_t = std::underlying_type_t<kafka::coordinator_type>;
    kafka::find_coordinator_request req("key");
    req.data.key_type = kafka::coordinator_type(
      std::numeric_limits<underlying_t>::max());

    auto resp = client.dispatch(req, kafka::api_version(1)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_TEST(resp.data.error_code == kafka::error_code::unsupported_version);
    BOOST_TEST(resp.data.node_id == model::node_id(-1));
    BOOST_TEST(resp.data.host == "");
    BOOST_TEST(resp.data.port == -1);
}

FIXTURE_TEST(find_coordinator, redpanda_thread_fixture) {
    wait_for_controller_leadership().get();

    auto client = make_kafka_client().get0();
    client.connect().get();

    kafka::find_coordinator_request req("key");

    auto resp = client.dispatch(req, kafka::api_version(1)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_TEST(resp.data.error_code == kafka::error_code::none);
    BOOST_TEST(resp.data.node_id == model::node_id(1));
    BOOST_TEST(resp.data.host == "127.0.0.1");
    BOOST_TEST(resp.data.port == 9092);
}
