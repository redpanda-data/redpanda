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
#include "test_utils/boost_fixture.h"

#include <seastar/core/smp.hh>

#include <limits>

namespace {

void test_single_key(
  redpanda_thread_fixture* fixture,
  kafka::coordinator_type request_type,
  const kafka::find_coordinator_response_data& expected_response) {
    fixture->wait_for_controller_leadership().get();

    auto client = fixture->make_kafka_client().get();
    client.connect().get();

    // default is group
    kafka::find_coordinator_request req("key", request_type);

    auto resp = client.dispatch(std::move(req), kafka::api_version(1)).get();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_TEST(resp.data.error_code == expected_response.error_code);
    BOOST_TEST(resp.data.node_id == expected_response.node_id);
    BOOST_TEST(resp.data.host == expected_response.host);
    BOOST_TEST(resp.data.port == expected_response.port);
}

void test_single_key_success(
  redpanda_thread_fixture* fixture, kafka::coordinator_type request_type) {
    kafka::find_coordinator_response_data expected_response;
    expected_response.error_code = kafka::error_code::none;
    expected_response.node_id = model::node_id(1);
    expected_response.host = "127.0.0.1";
    expected_response.port = 9092;

    test_single_key(fixture, request_type, expected_response);
}

void test_single_key_unsupported(redpanda_thread_fixture* fixture) {
    kafka::find_coordinator_response_data expected_response;
    expected_response.error_code = kafka::error_code::unsupported_version;
    expected_response.node_id = model::node_id(-1);
    expected_response.host = "";
    expected_response.port = -1;

    using underlying_t = std::underlying_type_t<kafka::coordinator_type>;
    auto request_type = kafka::coordinator_type(
      std::numeric_limits<underlying_t>::max());

    test_single_key(fixture, request_type, expected_response);
}

void test_multi_key(
  redpanda_thread_fixture* fixture,
  kafka::coordinator_type request_type,
  const kafka::coordinator& expected_response) {
    fixture->wait_for_controller_leadership().get();

    auto client = fixture->make_kafka_client().get();
    client.connect().get();

    const std::initializer_list<ss::sstring> init_keys = {
      "key1", "key2", "key3"};

    kafka::find_coordinator_request request{init_keys, request_type};

    auto resp
      = client.dispatch(std::move(request), kafka::api_version(4)).get();
    client.stop().then([&client] { client.shutdown(); }).get();

    auto& coordinator_responses = resp.data.coordinators;
    std::vector<ss::sstring> validation_keys = init_keys;
    for (const auto& coordinator_response : coordinator_responses) {
        // make sure we found the key
        auto found_key_it = std::ranges::find(
          validation_keys, coordinator_response.key);

        bool key_was_found = (found_key_it != validation_keys.end());
        BOOST_TEST(key_was_found);

        // remove the key so it can only be used once
        validation_keys.erase(found_key_it);

        BOOST_TEST(
          coordinator_response.error_code == expected_response.error_code);
        BOOST_TEST(coordinator_response.node_id == expected_response.node_id);
        BOOST_TEST(coordinator_response.host == expected_response.host);
        BOOST_TEST(coordinator_response.port == expected_response.port);
    }
    BOOST_TEST(validation_keys.size() == 0);
}

void test_multi_key_success(
  redpanda_thread_fixture* fixture, kafka::coordinator_type request_type) {
    kafka::coordinator expected_response{};
    expected_response.error_code = kafka::error_code::none;
    expected_response.node_id = model::node_id(1);
    expected_response.host = "127.0.0.1";
    expected_response.port = 9092;
    test_multi_key(fixture, request_type, expected_response);
}

void test_multi_key_unsupported(redpanda_thread_fixture* fixture) {
    using underlying_t = std::underlying_type_t<kafka::coordinator_type>;
    auto request_type = kafka::coordinator_type(
      std::numeric_limits<underlying_t>::max());
    kafka::coordinator expected_response{};
    expected_response.error_code = kafka::error_code::unsupported_version;
    expected_response.node_id = model::node_id(-1);
    expected_response.host = "";
    expected_response.port = -1;
    test_multi_key(fixture, request_type, expected_response);
}

} // namespace

FIXTURE_TEST(find_coordinator_unsupported_key, redpanda_thread_fixture) {
    test_single_key_unsupported(this);
}

FIXTURE_TEST(find_coordinator_group, redpanda_thread_fixture) {
    test_single_key_success(this, kafka::coordinator_type::group);
}

FIXTURE_TEST(find_coordinator_tx, redpanda_thread_fixture) {
    test_single_key_success(this, kafka::coordinator_type::transaction);
}

FIXTURE_TEST(find_coordinator_tx_v4, redpanda_thread_fixture) {
    test_multi_key_success(this, kafka::coordinator_type::transaction);
}

FIXTURE_TEST(find_coordinator_group_v4, redpanda_thread_fixture) {
    test_multi_key_success(this, kafka::coordinator_type::group);
}

FIXTURE_TEST(find_coordinator_unsupported_v4, redpanda_thread_fixture) {
    test_multi_key_unsupported(this);
}
