// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/config_frontend.h"
#include "cluster/security_frontend.h"
#include "kafka/client/transport.h"
#include "kafka/protocol/create_topics.h"
#include "kafka/protocol/metadata.h"
#include "kafka/protocol/sasl_authenticate.h"
#include "kafka/protocol/sasl_handshake.h"
#include "kafka/protocol/types.h"
#include "kafka/server/handlers/details/security.h"
#include "model/ktp.h"
#include "model/timeout_clock.h"
#include "random/generators.h"
#include "redpanda/tests/fixture.h"
#include "security/acl.h"
#include "security/scram_algorithm.h"
#include "security/scram_authenticator.h"
#include "security/types.h"
#include "types.h"

#include <absl/algorithm/container.h>
#include <boost/test/tools/old/interface.hpp>

#include <chrono>

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

    void enable_sasl() {
        cluster::config_update_request r{.upsert = {{"enable_sasl", "true"}}};
        auto res = app.controller->get_config_frontend()
                     .local()
                     .patch(r, model::timeout_clock::now() + 1s)
                     .get();
        BOOST_REQUIRE(!res.errc);
    }

    security::server_first_message send_scram_client_first(
      kafka::client::transport& client,
      const security::client_first_message& client_first) {
        kafka::sasl_authenticate_request client_first_req;
        {
            auto msg = client_first.message();
            client_first_req.data.auth_bytes = bytes(msg.cbegin(), msg.cend());
        }
        auto client_first_resp = client.dispatch(client_first_req).get();
        BOOST_REQUIRE_EQUAL(
          client_first_resp.data.error_code, kafka::error_code::none);
        return security::server_first_message(
          client_first_resp.data.auth_bytes);
    }

    security::server_final_message send_scram_client_final(
      kafka::client::transport& client,
      const security::client_final_message& client_final) {
        kafka::sasl_authenticate_request client_last_req;
        {
            auto msg = client_final.message();
            client_last_req.data.auth_bytes = bytes(msg.cbegin(), msg.cend());
        }
        auto client_last_resp = client.dispatch(client_last_req).get();

        BOOST_REQUIRE_EQUAL(
          client_last_resp.data.error_code, kafka::error_code::none);
        return security::server_final_message(
          std::move(client_last_resp.data.auth_bytes));
    }

    void do_sasl_handshake(kafka::client::transport& client) {
        kafka::sasl_handshake_request req;
        req.data.mechanism = security::scram_sha256_authenticator::name;

        auto resp = client.dispatch(req).get();
        BOOST_REQUIRE_EQUAL(resp.data.error_code, kafka::error_code::none);
    }

    void authn_kafka_client(
      kafka::client::transport& client,
      const ss::sstring& username,
      const ss::sstring& password) {
        do_sasl_handshake(client);
        const auto nonce = random_generators::gen_alphanum_string(130);
        const security::client_first_message client_first(username, nonce);
        const auto server_first = send_scram_client_first(client, client_first);

        BOOST_REQUIRE(
          std::string_view(server_first.nonce()).starts_with(nonce));
        BOOST_REQUIRE_GE(
          server_first.iterations(), security::scram_sha256::min_iterations);
        security::client_final_message client_final(
          bytes::from_string("n,,"), server_first.nonce());
        auto salted_password = security::scram_sha256::hi(
          bytes(password.cbegin(), password.cend()),
          server_first.salt(),
          server_first.iterations());
        client_final.set_proof(security::scram_sha256::client_proof(
          salted_password, client_first, server_first, client_final));

        auto server_final = send_scram_client_final(client, client_final);
        BOOST_REQUIRE(!server_final.error());

        auto server_key = security::scram_sha256::server_key(salted_password);
        auto server_sig = security::scram_sha256::server_signature(
          server_key, client_first, server_first, client_final);

        BOOST_REQUIRE_EQUAL(server_final.signature(), server_sig);
    }
};

FIXTURE_TEST(metadata_v9_no_topics, metadata_fixture) {
    kafka::metadata_request req_no_cluster{.data{
      .topics = {},
      .allow_auto_topic_creation = false,
      .include_cluster_authorized_operations = false,
      .include_topic_authorized_operations = false}};
    auto client = make_kafka_client().get();
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
    ss::sstring test_topic_name = "test";

    create_topic(test_topic_name, 1, 1);

    kafka::metadata_request req{.data{
      .topics = {},
      .allow_auto_topic_creation = false,
      .include_cluster_authorized_operations = false,
      .include_topic_authorized_operations = false}};
    auto client = make_kafka_client().get();
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
    ss::sstring test_topic_name = "test";

    create_topic(test_topic_name, 1, 1);
    create_user(test_username, test_password);

    // Enable SASL to enable authentication
    enable_sasl();

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
          return absl::c_any_of(errs, [](const cluster::errc& e) {
              return e != cluster::errc::success;
          });
      };

    BOOST_REQUIRE(!errors_in_acl_results(acl_result));

    auto client = make_kafka_client().get();
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
