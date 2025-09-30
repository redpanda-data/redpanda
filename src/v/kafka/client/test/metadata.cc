// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/protocol/metadata.h"

#include "cluster/security_frontend.h"
#include "kafka/client/client.h"
#include "kafka/client/test/fixture.h"
#include "kafka/server/handlers/details/security.h"
#include "security/acl.h"
#include "test_utils/boost_fixture.h"

#include <algorithm>

class metadata_fixture : public kafka_client_fixture {
public:
    void create_user(
      std::string_view username, security::scram_credential credentials) {
        app.controller->get_security_frontend()
          .local()
          .create_user(
            security::credential_user(username),
            std::move(credentials),
            model::timeout_clock::now() + 5s)
          .get();
    }
};

FIXTURE_TEST(test_empty_metadata, metadata_fixture) {
    auto client = make_connected_client();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });

    auto resp
      = client.fetch_metadata(kafka::metadata_request{.list_all_topics = true})
          .get();
    BOOST_REQUIRE_GT(resp.data.brokers.size(), 0);
    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 0);
}

FIXTURE_TEST(test_nonexistent_topic, metadata_fixture) {
    auto client = make_connected_client();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });

    auto resp
      = client
          .fetch_metadata(
            kafka::metadata_request{
              .data
              = {.topics = {{kafka::metadata_request_topic{.name = model::topic("non-existent")}}}, .allow_auto_topic_creation = false}})
          .get();
    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp.data.topics[0].error_code,
      kafka::error_code::unknown_topic_or_partition);
}

FIXTURE_TEST(test_authz_response, metadata_fixture) {
    auto username = ss::sstring("test_user");
    auto password = ss::sstring("test_password");
    auto test_topic = create_topic();
    auto creds = security::scram_sha256::make_credentials(
      password, security::scram_sha256::min_iterations);
    create_user(username, creds);
    enable_sasl();
    auto disable_sasl_defer = ss::defer([this] { disable_sasl(); });

    std::vector<security::acl_binding> cluster_bindings{
      security::acl_binding(
        security::resource_pattern(
          security::resource_type::cluster,
          security::default_cluster_name,
          security::pattern_type::literal),
        security::acl_entry(
          kafka::details::to_acl_principal(ssx::sformat("User:{}", username)),
          security::acl_host::wildcard_host(),
          security::acl_operation::alter,
          security::acl_permission::allow)),
      security::acl_binding(
        security::resource_pattern(
          security::resource_type::topic,
          test_topic.tp,
          security::pattern_type::literal),
        security::acl_entry(
          kafka::details::to_acl_principal(ssx::sformat("User:{}", username)),
          security::acl_host::wildcard_host(),
          security::acl_operation::describe,
          security::acl_permission::allow))};

    auto acl_result = app.controller->get_security_frontend()
                        .local()
                        .create_acls(std::move(cluster_bindings), 1s)
                        .get();
    const auto errors_in_acl_results =
      [](const std::vector<cluster::errc>& errs) {
          return std::ranges::any_of(errs, [](const cluster::errc& e) {
              return e != cluster::errc::success;
          });
      };

    BOOST_REQUIRE(!errors_in_acl_results(acl_result));

    auto client = make_client();
    client.set_credentials(
      kc::sasl_configuration{
        .mechanism = ss::sstring{"SCRAM-SHA-256"},
        .username = username,
        .password = password});
    client.connect().get();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });

    auto resp
      = client
          .fetch_metadata(
            kafka::metadata_request{
              .data
              = {.topics = {{{.name = test_topic.tp}}}, .allow_auto_topic_creation = false, .include_cluster_authorized_operations = true, .include_topic_authorized_operations = true}})
          .get();

    constexpr auto expected_cluster_authorized_operations
      = 0x180; // Describe | Alter
    constexpr auto expected_topic_authorized_operations = 0x100; // Describe

    BOOST_REQUIRE_GT(resp.data.brokers.size(), 0);
    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
    BOOST_REQUIRE_EQUAL(
      resp.data.cluster_authorized_operations,
      expected_cluster_authorized_operations);
    BOOST_REQUIRE_EQUAL(resp.data.topics[0].name, test_topic.tp);
    BOOST_REQUIRE_EQUAL(
      resp.data.topics[0].error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(
      resp.data.topics[0].topic_authorized_operations,
      expected_topic_authorized_operations);
}
