// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "features/feature_table.h"
#include "kafka/protocol/consumer_group_describe.h"
#include "kafka/protocol/consumer_group_heartbeat.h"
#include "kafka/protocol/describe_redpanda_roles.h"
#include "kafka/protocol/types.h"
#include "kafka/server/handlers/api_versions.h"
#include "redpanda/tests/fixture.h"
#include "test_utils/boost_fixture.h"

#include <algorithm>

namespace {

/// The APIs a broker advertises. get_supported_apis() is built from the handler
/// list alone, so it also carries the gated-off KIP-848 keys.
///
/// TODO(kip-848): delete once the protocol starts advertising; the tests below
/// should then compare against get_supported_apis() directly.
chunked_vector<kafka::api_versions_response_key> expected_advertised_apis() {
    auto apis = kafka::get_supported_apis();
    auto to_remove = std::ranges::remove_if(
      apis,
      [](kafka::api_key::type key) {
          return key == kafka::consumer_group_heartbeat_api::key
                 || key == kafka::consumer_group_describe_api::key;
      },
      &kafka::api_versions_response_key::api_key);
    apis.erase_to_end(to_remove.begin());
    return apis;
}

} // namespace

// https://github.com/apache/kafka/blob/eaccb92/core/src/test/scala/unit/kafka/server/ApiVersionsRequestTest.scala

FIXTURE_TEST(validate_latest_version, redpanda_thread_fixture) {
    auto client = make_kafka_client().get();
    client.connect().get();

    kafka::api_versions_request request;
    request.data.client_software_name = "redpanda";
    request.data.client_software_version = "x.x.x";
    auto response = client.dispatch(request, kafka::api_version(3)).get();
    BOOST_TEST(response.data.error_code == kafka::error_code::none);
    client.stop().then([&client] { client.shutdown(); }).get();

    auto expected = expected_advertised_apis();
    BOOST_TEST(response.data.api_keys == expected);
}

FIXTURE_TEST(validate_v0, redpanda_thread_fixture) {
    auto client = make_kafka_client().get();
    client.connect().get();

    kafka::api_versions_request request;
    auto response = client.dispatch(request, kafka::api_version(0)).get();
    BOOST_TEST(response.data.error_code == kafka::error_code::none);
    client.stop().then([&client] { client.shutdown(); }).get();

    auto expected = expected_advertised_apis();
    BOOST_TEST(response.data.api_keys == expected);
}

FIXTURE_TEST(unsupported_version, redpanda_thread_fixture) {
    auto client = make_kafka_client().get();
    client.connect().get();

    kafka::api_versions_request request;
    auto max_version = kafka::api_version(3);
    auto response = client.dispatch(request, max_version).get();
    client.stop().then([&client] { client.shutdown(); }).get();

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

// Tests for bug that broke flex request parsing for null or empty client ids
FIXTURE_TEST(flex_with_empty_client_id, redpanda_thread_fixture) {
    auto client = make_kafka_client("").get();
    client.connect().get();

    kafka::api_versions_request request;
    auto response = client.dispatch(request, kafka::api_version(3)).get();
    BOOST_TEST(response.data.error_code == kafka::error_code::none);
    client.stop().then([&client] { client.shutdown(); }).get();
}

FIXTURE_TEST(flex_with_null_client_id, redpanda_thread_fixture) {
    auto client = make_kafka_client(std::nullopt).get();
    client.connect().get();

    kafka::api_versions_request request;
    auto response = client.dispatch(request, kafka::api_version(3)).get();
    BOOST_TEST(response.data.error_code == kafka::error_code::none);
    client.stop().then([&client] { client.shutdown(); }).get();
}

FIXTURE_TEST(reserved_range_apis_advertised, redpanda_thread_fixture) {
    auto apis = kafka::get_supported_apis();
    BOOST_CHECK(std::ranges::any_of(apis, [](const auto& a) {
        return a.api_key == kafka::redpanda_api_key_base;
    }));
}

SEASTAR_THREAD_TEST_CASE(reserved_api_gated_by_feature) {
    auto make_resp = [] {
        kafka::api_versions_response r;
        r.data.api_keys.push_back(
          kafka::api_versions_response_key{
            kafka::describe_redpanda_roles_api::key,
            kafka::api_version{0},
            kafka::api_version{0}});
        return r;
    };
    const auto has_key = [](const kafka::api_versions_response& r) {
        return std::ranges::any_of(r.data.api_keys, [](const auto& a) {
            return a.api_key == kafka::describe_redpanda_roles_api::key;
        });
    };

    features::feature_table inactive;
    auto r1 = make_resp();
    kafka::remove_unavailable_reserved_apis(r1, inactive);
    BOOST_CHECK(!has_key(r1));

    features::feature_table active;
    active.testing_activate_all();
    auto r2 = make_resp();
    kafka::remove_unavailable_reserved_apis(r2, active);
    BOOST_CHECK(has_key(r2));
}

SEASTAR_THREAD_TEST_CASE(consumer_group_apis_gated) {
    // Keys 68/69 must stay hidden even on a cluster with every feature active,
    // since nothing can serve them yet.
    auto make_resp = [] {
        kafka::api_versions_response r;
        for (auto key :
             {kafka::consumer_group_heartbeat_api::key,
              kafka::consumer_group_describe_api::key}) {
            r.data.api_keys.push_back(
              kafka::api_versions_response_key{
                key, kafka::api_version{1}, kafka::api_version{1}});
        }
        return r;
    };

    features::feature_table inactive;
    auto r1 = make_resp();
    kafka::remove_unavailable_consumer_group_apis(r1, inactive);
    BOOST_CHECK(r1.data.api_keys.empty());

    features::feature_table active;
    active.testing_activate_all();
    auto r2 = make_resp();
    kafka::remove_unavailable_consumer_group_apis(r2, active);
    BOOST_CHECK(r2.data.api_keys.empty());
}
