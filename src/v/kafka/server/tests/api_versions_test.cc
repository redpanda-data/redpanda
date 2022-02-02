// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/handlers/api_versions.h"
#include "kafka/types.h"
#include "redpanda/tests/fixture.h"
#include "resource_mgmt/io_priority.h"

#include <seastar/core/smp.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/ip.hh>

// https://github.com/apache/kafka/blob/eaccb92/core/src/test/scala/unit/kafka/server/ApiVersionsRequestTest.scala

// version 3 is not supported
#if 0
FIXTURE_TEST(validate_latest_version, redpanda_thread_fixture) {
    auto client = make_kafka_client().get0();
    client.connect().get();

    kafka::api_versions_request request;
    request.client_software_name = "name";
    request.client_software_version = "version";
    auto response = client.dispatch(request).get0();
    BOOST_TEST(response.error == kafka::error_code::none);
    client.stop().then([&client] { client.shutdown(); }).get();

    auto expected = kafka::get_supported_apis();
    BOOST_TEST(response.apis == expected);
}
#endif

FIXTURE_TEST(validate_v0, redpanda_thread_fixture) {
    wait_for_controller_leadership().get0();
    auto client = make_kafka_client().get0();
    client.connect().get();

    kafka::api_versions_request request;
    auto response = client.dispatch(request, kafka::api_version(0)).get0();
    BOOST_TEST(response.data.error_code == kafka::error_code::none);
    client.stop().then([&client] { client.shutdown(); }).get();

    auto expected = kafka::get_supported_apis();
    BOOST_TEST(response.data.api_keys == expected);
}

// version 3 is not supported
#if 0
FIXTURE_TEST(validate_v3, redpanda_thread_fixture) {
    auto client = make_kafka_client().get0();
    client.connect().get();

    kafka::api_versions_request request;
    auto response = client.dispatch(request, kafka::api_version(3)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    // invalid since name/version are empty in the request
    BOOST_TEST(response.error == kafka::error_code::invalid_request);
}
#endif

FIXTURE_TEST(unsupported_version, redpanda_thread_fixture) {
    wait_for_controller_leadership().get0();
    auto client = make_kafka_client().get0();
    client.connect().get();

    kafka::api_versions_request request;
    auto max_version = kafka::api_version(
      std::numeric_limits<kafka::api_version::type>::max());
    auto response
      = client.dispatch(request, max_version, kafka::api_version(0)).get0();
    client.stop().then([&client] { client.shutdown(); }).get();

    BOOST_TEST(
      response.data.error_code == kafka::error_code::unsupported_version);
    BOOST_REQUIRE(!response.data.api_keys.empty());

    // get the versions supported by the api versions request itself
    auto api = std::find_if(
      response.data.api_keys.cbegin(),
      response.data.api_keys.cend(),
      [](const kafka::api_versions_response_key& api) {
          return api.api_key == kafka::api_versions_api::key;
      });

    BOOST_REQUIRE(api != response.data.api_keys.cend());
    BOOST_TEST(api->api_key == kafka::api_versions_api::key);
    BOOST_TEST(api->min_version == kafka::api_versions_handler::min_supported);
    BOOST_TEST(api->max_version == kafka::api_versions_handler::max_supported);
}
