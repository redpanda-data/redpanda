// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "cluster/controller.h"
#include "cluster/security_frontend.h"
#include "kafka/protocol/describe_user_scram_credentials.h"
#include "kafka/protocol/types.h"
#include "kafka/server/handlers/details/security.h"
#include "model/timeout_clock.h"
#include "redpanda/tests/fixture.h"
#include "security/acl.h"
#include "security/scram_algorithm.h"
#include "security/scram_credential.h"
#include "security/types.h"
#include "test_utils/boost_fixture.h"

#include <seastar/core/sstring.hh>

#include <algorithm>

class describe_user_scram_credentials_fixture : public redpanda_thread_fixture {
protected:
    static constexpr auto user_name_256 = "test_user_256";
    static constexpr auto user_name_512 = "test_user_512";
    static constexpr auto password_256 = "password256";
    static constexpr auto password_512 = "password512";
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

    void validate_user_response_two_users(
      kafka::describe_user_scram_credentials_response resp) {
        BOOST_REQUIRE(!resp.data.errored());
        BOOST_REQUIRE_EQUAL(resp.data.results.size(), 2);

        chunked_vector<kafka::describe_user_scram_credentials_result> check;
        check.reserve(2);
        check.emplace_back(
          kafka::describe_user_scram_credentials_result{
            .user = kafka::scram_user_name{user_name_256},
            .error_code = kafka::error_code::none,
            .credential_infos = {
              kafka::credential_info{
                .mechanism = kafka::scram_mechanism::scram_sha_256,
                .iterations = security::scram_sha256::min_iterations},
            }});
        check.emplace_back(
          kafka::describe_user_scram_credentials_result{
            .user = kafka::scram_user_name{user_name_512},
            .error_code = kafka::error_code::none,
            .credential_infos = {
              kafka::credential_info{
                .mechanism = kafka::scram_mechanism::scram_sha_512,
                .iterations = security::scram_sha512::min_iterations},
            }});

        std::ranges::sort(
          resp.data.results,
          {},
          &kafka::describe_user_scram_credentials_result::user);

        std::ranges::sort(
          check, {}, &kafka::describe_user_scram_credentials_result::user);

        BOOST_REQUIRE_EQUAL(resp.data.results, check);
    }
};

FIXTURE_TEST(
  describe_user_scram_credentials_no_auth,
  describe_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();
    auto creds_256 = security::scram_sha256::make_credentials(
      password_256, security::scram_sha256::min_iterations);
    create_user(user_name_256, creds_256);

    auto creds_512 = security::scram_sha512::make_credentials(
      password_512, security::scram_sha512::min_iterations);
    create_user(user_name_512, creds_512);

    kafka::describe_user_scram_credentials_request req;

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    validate_user_response_two_users(std::move(resp));
}

FIXTURE_TEST(
  describe_user_scram_credentials_auth,
  describe_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();
    auto creds_256 = security::scram_sha256::make_credentials(
      password_256, security::scram_sha256::min_iterations);
    create_user(user_name_256, creds_256);

    auto creds_512 = security::scram_sha512::make_credentials(
      password_512, security::scram_sha512::min_iterations);
    create_user(user_name_512, creds_512);

    enable_sasl();

    auto disable_sasl_defer = ss::defer([this] { disable_sasl(); });

    std::vector<security::acl_binding> cluster_bindings{security::acl_binding(
      security::resource_pattern(
        security::resource_type::cluster,
        security::default_cluster_name,
        security::pattern_type::literal),

      security::acl_entry(
        kafka::details::to_acl_principal(
          ssx::sformat("User:{}", user_name_256)),
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
    authn_kafka_client(client, user_name_256, password_256);
    kafka::describe_user_scram_credentials_request req;

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    validate_user_response_two_users(std::move(resp));
}

FIXTURE_TEST(
  describe_user_scram_credentials_not_authz,
  describe_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();
    auto creds_256 = security::scram_sha256::make_credentials(
      password_256, security::scram_sha256::min_iterations);
    create_user(user_name_256, creds_256);

    auto creds_512 = security::scram_sha512::make_credentials(
      password_512, security::scram_sha512::min_iterations);
    create_user(user_name_512, creds_512);

    enable_sasl();

    auto disable_sasl_defer = ss::defer([this] { disable_sasl(); });

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();
    authn_kafka_client(client, user_name_256, password_256);

    kafka::describe_user_scram_credentials_request req;
    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    BOOST_REQUIRE(resp.data.errored());
    BOOST_REQUIRE_EQUAL(
      resp.data.error_code, kafka::error_code::cluster_authorization_failed);
    BOOST_REQUIRE(resp.data.results.empty());
}

FIXTURE_TEST(
  describe_user_scram_credentials_user_does_not_exist,
  describe_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();
    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();
    kafka::describe_user_scram_credentials_request req;
    req.data.users.emplace(
      chunked_vector<kafka::user_name>{
        kafka::user_name{.name = kafka::scram_user_name{"non_existent_user"}}});

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    BOOST_CHECK(resp.data.errored());
    BOOST_CHECK_EQUAL(resp.data.error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
    BOOST_CHECK_EQUAL(resp.data.results[0].user, "non_existent_user");
    BOOST_CHECK_EQUAL(
      resp.data.results[0].error_code, kafka::error_code::resource_not_found);
}

FIXTURE_TEST(
  describer_user_scram_credentials_empty_user,
  describe_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();
    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();
    kafka::describe_user_scram_credentials_request req;
    req.data.users.emplace(
      chunked_vector<kafka::user_name>{
        kafka::user_name{.name = kafka::scram_user_name{""}}});
    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    BOOST_CHECK(resp.data.errored());
    BOOST_CHECK_EQUAL(resp.data.error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
    BOOST_CHECK_EQUAL(resp.data.results[0].user, "");
    BOOST_CHECK_EQUAL(
      resp.data.results[0].error_code, kafka::error_code::resource_not_found);
}

FIXTURE_TEST(
  describe_user_scram_credentials_duplicate_user,
  describe_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();
    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();
    kafka::describe_user_scram_credentials_request req;
    req.data.users.emplace(
      chunked_vector<kafka::user_name>{
        kafka::user_name{.name = kafka::scram_user_name{"non_existent_user"}},
        kafka::user_name{.name = kafka::scram_user_name{"non_existent_user"}}});

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    BOOST_CHECK(resp.data.errored());
    BOOST_CHECK_EQUAL(resp.data.error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
    BOOST_CHECK_EQUAL(resp.data.results[0].user, "non_existent_user");
    BOOST_CHECK_EQUAL(
      resp.data.results[0].error_code, kafka::error_code::duplicate_resource);
}

FIXTURE_TEST(
  describe_user_scram_credentials_duplicate_user_exists,
  describe_user_scram_credentials_fixture) {
    create_user(
      "exists_256",
      security::scram_sha256::make_credentials(
        "password_256", security::scram_sha256::min_iterations));
    wait_for_controller_leadership().get();
    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();
    kafka::describe_user_scram_credentials_request req;
    req.data.users.emplace(
      chunked_vector<kafka::user_name>{
        kafka::user_name{.name = kafka::scram_user_name{"exists_256"}},
        kafka::user_name{.name = kafka::scram_user_name{"exists_256"}}});

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    BOOST_CHECK(resp.data.errored());
    BOOST_CHECK_EQUAL(resp.data.error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
    BOOST_CHECK_EQUAL(resp.data.results[0].user, "exists_256");
    BOOST_CHECK_EQUAL(
      resp.data.results[0].error_code, kafka::error_code::duplicate_resource);
}

FIXTURE_TEST(
  describe_user_scram_credentials_mix,
  describe_user_scram_credentials_fixture) {
    kafka::scram_user_name exists_1{"exists_1"};
    kafka::scram_user_name exists_2{"exists_2"};
    kafka::scram_user_name does_not_exist{"does_not_exist"};
    kafka::scram_user_name does_not_exist_duplicate{"does_not_exist_duplicate"};

    create_user(
      exists_1(),
      security::scram_sha256::make_credentials(
        "password_256", security::scram_sha256::min_iterations));
    create_user(
      exists_2(),
      security::scram_sha256::make_credentials(
        "password_512", security::scram_sha256::min_iterations));

    kafka::describe_user_scram_credentials_request req;
    req.data.users.emplace(
      chunked_vector<kafka::user_name>{
        kafka::user_name{.name = exists_1},
        kafka::user_name{.name = exists_2},
        kafka::user_name{.name = does_not_exist},
        kafka::user_name{.name = does_not_exist_duplicate},
        kafka::user_name{.name = does_not_exist_duplicate}});

    wait_for_controller_leadership().get();
    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();
    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    BOOST_CHECK(resp.data.errored());
    BOOST_CHECK_EQUAL(resp.data.error_code, kafka::error_code::none);
    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 4);

    chunked_vector<kafka::describe_user_scram_credentials_result> check;
    check.reserve(4);
    check.emplace_back(
      kafka::describe_user_scram_credentials_result{
        .user = kafka::scram_user_name{exists_1()},
        .error_code = kafka::error_code::none,
        .credential_infos = {
          kafka::credential_info{
            .mechanism = kafka::scram_mechanism::scram_sha_256,
            .iterations = security::scram_sha256::min_iterations},
        }});
    check.emplace_back(
      kafka::describe_user_scram_credentials_result{
        .user = kafka::scram_user_name{exists_2()},
        .error_code = kafka::error_code::none,
        .credential_infos = {
          kafka::credential_info{
            .mechanism = kafka::scram_mechanism::scram_sha_256,
            .iterations = security::scram_sha256::min_iterations},
        }});
    check.emplace_back(
      kafka::describe_user_scram_credentials_result{
        .user = kafka::scram_user_name{does_not_exist()},
        .error_code = kafka::error_code::resource_not_found,
        .error_message = ssx::sformat(
          "Cannot describe SCRAM credentials for non-existent user: {}",
          does_not_exist())});
    check.emplace_back(
      kafka::describe_user_scram_credentials_result{
        .user = kafka::scram_user_name{does_not_exist_duplicate()},
        .error_code = kafka::error_code::duplicate_resource,
        .error_message = ssx::sformat(
          "Cannot describe SCRAM credentials for the same user twice in a "
          "single "
          "request: {}",
          does_not_exist_duplicate())});

    std::ranges::sort(
      resp.data.results,
      {},
      &kafka::describe_user_scram_credentials_result::user);

    std::ranges::sort(
      check, {}, &kafka::describe_user_scram_credentials_result::user);

    BOOST_REQUIRE_EQUAL(resp.data.results, check);
}
