// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/security_frontend.h"
#include "kafka/client/transport.h"
#include "kafka/protocol/create_topics.h"
#include "kafka/protocol/metadata.h"
#include "kafka/protocol/types.h"
#include "kafka/server/handlers/details/security.h"
#include "kafka/server/handlers/metadata.h"
#include "model/timeout_clock.h"
#include "redpanda/tests/fixture.h"
#include "security/acl.h"
#include "security/scram_algorithm.h"
#include "security/types.h"
#include "test_utils/random_bytes.h"

#include <boost/test/tools/old/interface.hpp>

#include <algorithm>

static const int32_t not_provided_authz_return = -2147483648;
static const std::vector<security::acl_operation> default_cluster_auths = {
  security::acl_operation::create,
  security::acl_operation::alter,
  security::acl_operation::describe,
  security::acl_operation::cluster_action,
  security::acl_operation::describe_configs,
  security::acl_operation::alter_configs,
  security::acl_operation::idempotent_write};

static const std::vector<security::acl_operation> default_topics_auths = {
  security::acl_operation::read,
  security::acl_operation::write,
  security::acl_operation::create,
  security::acl_operation::describe,
  security::acl_operation::remove,
  security::acl_operation::alter,
  security::acl_operation::describe_configs,
  security::acl_operation::alter_configs};

static const ss::sstring test_username = "test";
static const ss::sstring test_acl_principal = "User:test";
static const ss::sstring test_password = "password";

class metadata_fixture : public redpanda_thread_fixture {
protected:
    void create_topic(ss::sstring tp, int32_t partitions, int16_t rf) {
        kafka::creatable_topic topic;
        topic.name = model::topic(tp);
        topic.num_partitions = partitions;
        topic.replication_factor = rf;

        auto req = kafka::create_topics_request{.data{
          .topics = {topic},
          .timeout_ms = 10s,
          .validate_only = false,
        }};

        auto client = make_kafka_client().get();
        auto deferred_close = ss::defer([&client] { client.stop().get(); });
        client.connect().get();
        auto resp
          = client.dispatch(std::move(req), kafka::api_version(2)).get();
    }

    void create_user(const ss::sstring& username, const ss::sstring& password) {
        auto credential = security::scram_sha256::make_credentials(
          password, security::scram_sha256::min_iterations);
        auto user = security::credential_user(username);
        auto err = app.controller->get_security_frontend()
                     .local()
                     .create_user(
                       user, credential, model::timeout_clock::now() + 5s)
                     .get();
        BOOST_REQUIRE_EQUAL(err, cluster::errc::success);
    }
};

FIXTURE_TEST(metadata_v9_no_topics, metadata_fixture) {
    kafka::metadata_request req_no_cluster{.data{
      .topics = {},
      .allow_auto_topic_creation = false,
      .include_cluster_authorized_operations = false,
      .include_topic_authorized_operations = false}};
    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();
    auto resp
      = client.dispatch(std::move(req_no_cluster), kafka::api_version(8)).get();
    BOOST_REQUIRE(!resp.data.errored());
    BOOST_CHECK_EQUAL(
      resp.data.cluster_authorized_operations, not_provided_authz_return);
    BOOST_CHECK(resp.data.topics.empty());

    kafka::metadata_request req_cluster_authz{
      .data{
        .topics = {},
        .allow_auto_topic_creation = false,
        .include_cluster_authorized_operations = true,
        .include_topic_authorized_operations = false},
    };
    resp = client.dispatch(std::move(req_cluster_authz), kafka::api_version(8))
             .get();
    BOOST_REQUIRE(!resp.data.errored());
    BOOST_CHECK_EQUAL(
      resp.data.cluster_authorized_operations,
      kafka::details::to_bit_field(default_cluster_auths));
}

FIXTURE_TEST(metadata_v9_topics, metadata_fixture) {
    ss::sstring test_topic_name = "metadata_v9_topics";

    create_topic(test_topic_name, 1, 1);

    kafka::metadata_request req{.data{
      .topics = {},
      .allow_auto_topic_creation = false,
      .include_cluster_authorized_operations = false,
      .include_topic_authorized_operations = false}};
    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();
    auto resp = client.dispatch(std::move(req), kafka::api_version(8)).get();
    BOOST_REQUIRE(!resp.data.errored());
    BOOST_CHECK_EQUAL(
      resp.data.cluster_authorized_operations, not_provided_authz_return);
    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
    BOOST_CHECK_EQUAL(resp.data.topics[0].name, model::topic{test_topic_name});
    BOOST_CHECK_EQUAL(
      resp.data.topics[0].topic_authorized_operations,
      not_provided_authz_return);

    kafka::metadata_request req_topic_authz{
      .data{
        .topics = {},
        .allow_auto_topic_creation = false,
        .include_cluster_authorized_operations = false,
        .include_topic_authorized_operations = true},
    };
    resp = client.dispatch(std::move(req_topic_authz), kafka::api_version(8))
             .get();
    BOOST_REQUIRE(!resp.data.errored());
    BOOST_CHECK_EQUAL(
      resp.data.cluster_authorized_operations, not_provided_authz_return);
    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);
    BOOST_CHECK_EQUAL(resp.data.topics[0].name, model::topic{test_topic_name});
    BOOST_CHECK_EQUAL(
      resp.data.topics[0].topic_authorized_operations,
      kafka::details::to_bit_field(default_topics_auths));
}

FIXTURE_TEST(metadata_v9_authz_acl, metadata_fixture) {
    wait_for_controller_leadership().get();
    ss::sstring test_topic_name = "metadata_v9_authz_acl";

    create_topic(test_topic_name, 1, 1);
    create_user(test_username, test_password);

    // Enable SASL to enable authentication
    enable_sasl();
    auto disable_sasl_defer = ss::defer([this] { disable_sasl(); });

    // Start by creating just describe ACLs for the cluster for the user
    std::vector<security::acl_binding> cluster_bindings{security::acl_binding(
      security::resource_pattern(
        security::resource_type::cluster,
        security::default_cluster_name,
        security::pattern_type::literal),
      security::acl_entry(
        kafka::details::to_acl_principal(test_acl_principal),
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

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();
    authn_kafka_client(client, test_username, test_password);

    kafka::metadata_request cluster_req{.data{
      .topics = {},
      .allow_auto_topic_creation = false,
      .include_cluster_authorized_operations = true,
      .include_topic_authorized_operations = false}};
    auto resp
      = client.dispatch(std::move(cluster_req), kafka::api_version(8)).get();

    // Here we expect to only see describe and to not see any topics
    BOOST_REQUIRE(!resp.data.errored());
    const std::vector<security::acl_operation> expected_cluster_ops = {
      security::acl_operation::describe};
    BOOST_CHECK_EQUAL(
      resp.data.cluster_authorized_operations,
      kafka::details::to_bit_field(expected_cluster_ops));
    BOOST_CHECK(resp.data.topics.empty());

    // Now allow the user to see the test topic
    std::vector<security::acl_binding> topic_bindings{security::acl_binding(
      security::resource_pattern(
        security::resource_type::topic,
        test_topic_name,
        security::pattern_type::literal),
      security::acl_entry(
        kafka::details::to_acl_principal(test_acl_principal),
        security::acl_host::wildcard_host(),
        security::acl_operation::describe,
        security::acl_permission::allow))};

    acl_result = app.controller->get_security_frontend()
                   .local()
                   .create_acls(std::move(topic_bindings), 1s)
                   .get();

    BOOST_REQUIRE(!errors_in_acl_results(acl_result));

    kafka::metadata_request topic_req{.data{
      .topics = {},
      .allow_auto_topic_creation = false,
      .include_cluster_authorized_operations = true,
      .include_topic_authorized_operations = true}};

    resp = client.dispatch(std::move(topic_req), kafka::api_version(8)).get();

    BOOST_REQUIRE(!resp.data.errored());
    BOOST_CHECK_EQUAL(
      resp.data.cluster_authorized_operations,
      kafka::details::to_bit_field(expected_cluster_ops));
    BOOST_REQUIRE_EQUAL(resp.data.topics.size(), 1);

    BOOST_CHECK_EQUAL(
      resp.data.topics[0].topic_authorized_operations,
      kafka::details::to_bit_field(expected_cluster_ops));
}

FIXTURE_TEST(metadata_empty_topic_name, metadata_fixture) {
    using kafka::api_version;
    if (kafka::metadata_handler::max_supported < api_version{12}) {
        return;
    }

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    constexpr auto make_request = []() {
        return kafka::metadata_request{.data{
          .topics = {{{.name{}}}},
          .allow_auto_topic_creation = false,
          .include_cluster_authorized_operations = false,
          .include_topic_authorized_operations = false}};
    };
    const kafka::metadata_response_data default_response;
    for (api_version ver{8}; ver < api_version{12}; ++ver) {
        auto resp = client.dispatch(make_request(), ver).get();
        BOOST_REQUIRE(resp.data.errored());
        BOOST_REQUIRE(!resp.data.topics.empty());
        if (ver <= api_version{9}) {
            BOOST_REQUIRE_EQUAL(
              resp.data.topics.front().error_code,
              kafka::error_code::invalid_topic_exception);
        } else {
            BOOST_REQUIRE_EQUAL(
              resp.data.topics.front().error_code,
              kafka::error_code::invalid_request);
            BOOST_REQUIRE(resp.data.brokers.empty());
            BOOST_REQUIRE_EQUAL(
              resp.data.cluster_id, default_response.cluster_id);
            BOOST_REQUIRE_EQUAL(
              resp.data.controller_id, default_response.controller_id);
        }
    }
}

FIXTURE_TEST(metadata_non_empty_topic_id, metadata_fixture) {
    using kafka::api_version;
    if (kafka::metadata_handler::max_supported < api_version{12}) {
        return;
    }
    ss::sstring test_topic_name = "metadata_non_empty_topic_id";

    create_topic(test_topic_name, 1, 1);

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    const auto make_request = [&]() {
        auto uuid = kafka::uuid::from_string(
          bytes_to_base64(tests::random_bytes(16)));
        return kafka::metadata_request{.data{
          .topics = {{{
            .topic_id{uuid},
            .name{test_topic_name},
          }}},
          .allow_auto_topic_creation = false,
          .include_cluster_authorized_operations = false,
          .include_topic_authorized_operations = false}};
    };
    const kafka::metadata_response_data default_response;
    for (api_version ver{10}; ver < api_version{12}; ++ver) {
        auto resp = client.dispatch(make_request(), ver).get();
        BOOST_REQUIRE(resp.data.errored());
        BOOST_REQUIRE(!resp.data.topics.empty());
        const auto expected = kafka::error_code::invalid_request;
        BOOST_REQUIRE_EQUAL(resp.data.topics.front().error_code, expected);
        BOOST_REQUIRE(resp.data.brokers.empty());
        BOOST_REQUIRE_EQUAL(resp.data.cluster_id, default_response.cluster_id);
        BOOST_REQUIRE_EQUAL(
          resp.data.controller_id, default_response.controller_id);
    }
}

FIXTURE_TEST(metadata_cluster_auth, metadata_fixture) {
    // Cluster authorized operations are returned only from v8 to v10
    using namespace kafka;
    constexpr auto max_supported = kafka::metadata_handler::max_supported;

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    const auto make_request = [&]() {
        return metadata_request{.data{
          .topics = {},
          .allow_auto_topic_creation = false,
          .include_cluster_authorized_operations = true,
          .include_topic_authorized_operations = false}};
    };

    const metadata_response_data default_response;
    const auto cluster_ops = kafka::details::to_bit_field(
      security::get_allowed_operations<security::acl_cluster_name>());

    for (api_version ver{1}; ver < max_supported; ++ver) {
        auto resp = client.dispatch(make_request(), ver).get();
        BOOST_REQUIRE(!resp.data.errored());
        const auto expected
          = (ver >= api_version{8} && ver <= api_version{10})
              ? cluster_ops
              : default_response.cluster_authorized_operations;
        BOOST_REQUIRE_EQUAL(resp.data.cluster_authorized_operations, expected);
    }
}
