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
#include "kafka/protocol/alter_user_scram_credentials.h"
#include "kafka/protocol/types.h"
#include "kafka/server/handlers/details/security.h"
#include "redpanda/tests/fixture.h"
#include "security/acl.h"
#include "security/credential_store.h"
#include "security/scram_algorithm.h"
#include "security/scram_authenticator.h"
#include "security/scram_credential.h"
#include "security/types.h"
#include "test_utils/boost_fixture.h"

#include <algorithm>

class alter_user_scram_credentials_fixture : public redpanda_thread_fixture {
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
};

FIXTURE_TEST(
  alter_user_scram_credentials_not_authz,
  alter_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();

    auto creds_256 = security::scram_sha256::make_credentials(
      password_256, security::scram_sha256::min_iterations);
    create_user(user_name_256, creds_256);

    auto [creds_512, salted_password_512]
      = security::scram_sha512::make_credentials_and_return_password(
        password_512, security::scram_sha512::min_iterations);

    enable_sasl();

    auto disable_sasl_defer = ss::defer([this] { disable_sasl(); });

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();
    authn_kafka_client(client, user_name_256, password_256);

    kafka::alter_user_scram_credentials_request req;
    req.data.upsertions.emplace_back(
      kafka::scram_credential_upsertion{
        .name = kafka::scram_user_name{user_name_512},
        .mechanism = kafka::scram_mechanism::scram_sha_512,
        .iterations = security::scram_sha512::min_iterations,
        .salt = creds_512.salt(),
        .salted_password = salted_password_512,
      });

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    BOOST_REQUIRE(resp.data.errored());
    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
    BOOST_CHECK_EQUAL(resp.data.results[0].user, user_name_512);
    BOOST_CHECK_EQUAL(
      resp.data.results[0].error_code,
      kafka::error_code::cluster_authorization_failed);
}

FIXTURE_TEST(
  alter_user_scram_credentials_add_user, alter_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();

    auto [creds_256, salted_password_256]
      = security::scram_sha256::make_credentials_and_return_password(
        password_256, security::scram_sha256::min_iterations);

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    kafka::alter_user_scram_credentials_request req;
    req.data.upsertions.emplace_back(
      kafka::scram_credential_upsertion{
        .name = kafka::scram_user_name{user_name_256},
        .mechanism = kafka::scram_mechanism::scram_sha_256,
        .iterations = security::scram_sha256::min_iterations,
        .salt = creds_256.salt(),
        .salted_password = salted_password_256,
      });

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    BOOST_REQUIRE(!resp.data.errored());
    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
    BOOST_CHECK_EQUAL(resp.data.results[0].user, user_name_256);
    BOOST_CHECK_EQUAL(resp.data.results[0].error_code, kafka::error_code::none);

    auto& sec = app.controller->get_credential_store().local();
    BOOST_REQUIRE(sec.contains(security::credential_user(user_name_256)));
    auto cred = sec.get<security::scram_credential>(
      security::credential_user(user_name_256));
    BOOST_REQUIRE(cred.has_value());
    BOOST_REQUIRE_MESSAGE(
      !cred->password_set_at().is_missing(),
      "Credential should have password_set_at");
    BOOST_CHECK_EQUAL(cred, creds_256);
}

FIXTURE_TEST(
  alter_user_scram_credentials_add_user_with_authz,
  alter_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();

    auto creds_256 = security::scram_sha256::make_credentials(
      password_256, security::scram_sha256::min_iterations);
    create_user(user_name_256, creds_256);

    auto [creds_512, salted_password_512]
      = security::scram_sha512::make_credentials_and_return_password(
        password_512, security::scram_sha512::min_iterations);

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
        security::acl_operation::alter,
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

    authn_kafka_client<security::scram_sha256_authenticator>(
      client, user_name_256, password_256);

    kafka::alter_user_scram_credentials_request req;
    req.data.upsertions.emplace_back(
      kafka::scram_credential_upsertion{
        .name = kafka::scram_user_name{user_name_512},
        .mechanism = kafka::scram_mechanism::scram_sha_512,
        .iterations = security::scram_sha512::min_iterations,
        .salt = creds_512.salt(),
        .salted_password = salted_password_512,
      });

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    BOOST_REQUIRE(!resp.data.errored());
    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
    BOOST_CHECK_EQUAL(resp.data.results[0].user, user_name_512);
    BOOST_CHECK_EQUAL(resp.data.results[0].error_code, kafka::error_code::none);

    auto& sec = app.controller->get_credential_store().local();
    BOOST_REQUIRE(sec.contains(security::credential_user(user_name_512)));
    auto cred = sec.get<security::scram_credential>(
      security::credential_user(user_name_512));
    BOOST_REQUIRE(cred.has_value());
    BOOST_REQUIRE_MESSAGE(
      !cred->password_set_at().is_missing(),
      "Credential should have password_set_at");
    BOOST_CHECK_EQUAL(cred, creds_512);

    // now create a new client and authenticate with the created user
    auto client2 = make_kafka_client().get();
    auto deferred_close_2 = ss::defer([&client2] { client2.stop().get(); });
    client2.connect().get();
    authn_kafka_client<security::scram_sha512_authenticator>(
      client2, user_name_512, password_512);
}

FIXTURE_TEST(
  alter_user_scram_credentials_delete_user,
  alter_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();

    auto creds_256 = security::scram_sha256::make_credentials(
      password_256, security::scram_sha256::min_iterations);
    create_user(user_name_256, creds_256);

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    kafka::alter_user_scram_credentials_request req;
    req.data.deletions.emplace_back(
      kafka::scram_credential_deletion{
        .name = kafka::scram_user_name{user_name_256},
        .mechanism = kafka::scram_mechanism::scram_sha_256});

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    BOOST_REQUIRE(!resp.data.errored());
    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
    BOOST_CHECK_EQUAL(resp.data.results[0].user, user_name_256);
    BOOST_CHECK_EQUAL(resp.data.results[0].error_code, kafka::error_code::none);

    auto& sec = app.controller->get_credential_store().local();
    BOOST_REQUIRE(!sec.contains(security::credential_user(user_name_256)));
}

FIXTURE_TEST(
  alter_user_scram_credentials_delete_nonexistant_user,
  alter_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();
    kafka::alter_user_scram_credentials_request req;
    req.data.deletions.emplace_back(
      kafka::scram_credential_deletion{
        .name = kafka::scram_user_name{"nonexistant_user"},
        .mechanism = kafka::scram_mechanism::scram_sha_256});

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    BOOST_REQUIRE(resp.data.errored());
    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
    BOOST_CHECK_EQUAL(resp.data.results[0].user, "nonexistant_user");
    BOOST_CHECK_EQUAL(
      resp.data.results[0].error_code, kafka::error_code::resource_not_found);
}

FIXTURE_TEST(
  alter_user_scram_credentials_delete_user_wrong_mech,
  alter_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();

    auto creds_256 = security::scram_sha256::make_credentials(
      password_256, security::scram_sha256::min_iterations);
    create_user(user_name_256, creds_256);

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    kafka::alter_user_scram_credentials_request req;
    req.data.deletions.emplace_back(
      kafka::scram_credential_deletion{
        .name = kafka::scram_user_name{user_name_256},
        .mechanism = kafka::scram_mechanism::scram_sha_512});

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    BOOST_REQUIRE(resp.data.errored());
    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
    BOOST_CHECK_EQUAL(resp.data.results[0].user, user_name_256);
    BOOST_CHECK_EQUAL(
      resp.data.results[0].error_code, kafka::error_code::resource_not_found);
}

FIXTURE_TEST(
  alter_user_scram_credentials_update_user,
  alter_user_scram_credentials_fixture) {
    auto creds_256 = security::scram_sha256::make_credentials(
      password_256, security::scram_sha256::min_iterations);
    create_user(user_name_256, creds_256);

    auto [creds_512, salted_password_512]
      = security::scram_sha512::make_credentials_and_return_password(
        password_512, security::scram_sha512::min_iterations);

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    kafka::alter_user_scram_credentials_request req;
    req.data.upsertions.emplace_back(
      kafka::scram_credential_upsertion{
        .name = kafka::scram_user_name{user_name_256},
        .mechanism = kafka::scram_mechanism::scram_sha_512,
        .iterations = security::scram_sha512::min_iterations,
        .salt = creds_512.salt(),
        .salted_password = salted_password_512,
      });

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    BOOST_REQUIRE(!resp.data.errored());
    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
    BOOST_CHECK_EQUAL(resp.data.results[0].user, user_name_256);
    BOOST_CHECK_EQUAL(resp.data.results[0].error_code, kafka::error_code::none);

    auto& sec = app.controller->get_credential_store().local();
    BOOST_REQUIRE(sec.contains(security::credential_user(user_name_256)));
    auto cred = sec.get<security::scram_credential>(
      security::credential_user(user_name_256));
    BOOST_REQUIRE(cred.has_value());
    BOOST_REQUIRE_MESSAGE(
      !cred->password_set_at().is_missing(),
      "Credential should have password_set_at");
    BOOST_CHECK_EQUAL(cred, creds_512);
}

FIXTURE_TEST(
  alter_user_scram_credentials_empty_upsert_user,
  alter_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    kafka::alter_user_scram_credentials_request req;
    req.data.upsertions.emplace_back(
      kafka::scram_credential_upsertion{
        .name = kafka::scram_user_name{""},
        .mechanism = kafka::scram_mechanism::scram_sha_256,
      });

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    BOOST_REQUIRE(resp.data.errored());
    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
    BOOST_CHECK_EQUAL(resp.data.results[0].user, "");
    BOOST_CHECK_EQUAL(
      resp.data.results[0].error_code,
      kafka::error_code::unacceptable_credential);
    BOOST_CHECK_EQUAL(
      resp.data.results[0].error_message, "Username must not be empty");
}

FIXTURE_TEST(
  alter_user_scram_credentials_invalid_scram_name,
  alter_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    kafka::alter_user_scram_credentials_request req;
    req.data.upsertions.emplace_back(
      kafka::scram_credential_upsertion{
        .name = kafka::scram_user_name{"=="},
        .mechanism = kafka::scram_mechanism::scram_sha_256,
      });

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    BOOST_REQUIRE(resp.data.errored());
    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
    BOOST_CHECK_EQUAL(resp.data.results[0].user, "==");
    BOOST_CHECK_EQUAL(
      resp.data.results[0].error_code,
      kafka::error_code::unacceptable_credential);
    BOOST_CHECK_EQUAL(
      resp.data.results[0].error_message, "Invalid SCRAM username");
}

FIXTURE_TEST(
  alter_user_scram_credentials_empty_delete_user,
  alter_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    kafka::alter_user_scram_credentials_request req;
    req.data.deletions.emplace_back(
      kafka::scram_credential_deletion{
        .name = kafka::scram_user_name{""},
        .mechanism = kafka::scram_mechanism::scram_sha_256,
      });

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    BOOST_REQUIRE(resp.data.errored());
    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
    BOOST_CHECK_EQUAL(resp.data.results[0].user, "");
    BOOST_CHECK_EQUAL(
      resp.data.results[0].error_code,
      kafka::error_code::unacceptable_credential);
    BOOST_CHECK_EQUAL(
      resp.data.results[0].error_message, "Username must not be empty");
}

FIXTURE_TEST(
  alter_user_scram_credentials_invalid_scram_mech_upsertion,
  alter_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    kafka::alter_user_scram_credentials_request req;
    req.data.upsertions.emplace_back(
      kafka::scram_credential_upsertion{
        .name = kafka::scram_user_name{"test1"},
        .mechanism = kafka::scram_mechanism::unknown,
      });
    req.data.upsertions.emplace_back(
      kafka::scram_credential_upsertion{
        .name = kafka::scram_user_name{"test2"},
        .mechanism
        = (kafka::scram_mechanism)((int8_t)kafka::scram_mechanism::scram_sha_512
                                   + 1),
      });

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    BOOST_REQUIRE(resp.data.errored());
    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 2);
    chunked_vector<kafka::alter_user_scram_credentials_result> check;
    check.reserve(2);
    check.emplace_back(
      kafka::alter_user_scram_credentials_result{
        .user = kafka::scram_user_name{"test1"},
        .error_code = kafka::error_code::unsupported_sasl_mechanism,
        .error_message = "Unknown SCRAM mechanism",
      });
    check.emplace_back(
      kafka::alter_user_scram_credentials_result{
        .user = kafka::scram_user_name{"test2"},
        .error_code = kafka::error_code::unsupported_sasl_mechanism,
        .error_message = "Unknown SCRAM mechanism",
      });

    std::ranges::sort(
      check, {}, &kafka::alter_user_scram_credentials_result::user);
    std::ranges::sort(
      resp.data.results, {}, &kafka::alter_user_scram_credentials_result::user);
    BOOST_REQUIRE_EQUAL(resp.data.results, check);
}

FIXTURE_TEST(
  alter_user_scram_credentials_invalid_scram_mech_deletion,
  alter_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    kafka::alter_user_scram_credentials_request req;
    req.data.deletions.emplace_back(
      kafka::scram_credential_deletion{
        .name = kafka::scram_user_name{"test1"},
        .mechanism = kafka::scram_mechanism::unknown,
      });
    req.data.deletions.emplace_back(
      kafka::scram_credential_deletion{
        .name = kafka::scram_user_name{"test2"},
        .mechanism
        = (kafka::scram_mechanism)((int8_t)kafka::scram_mechanism::scram_sha_512
                                   + 1),
      });

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    BOOST_REQUIRE(resp.data.errored());
    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 2);
    chunked_vector<kafka::alter_user_scram_credentials_result> check;
    check.reserve(2);
    check.emplace_back(
      kafka::alter_user_scram_credentials_result{
        .user = kafka::scram_user_name{"test1"},
        .error_code = kafka::error_code::unsupported_sasl_mechanism,
        .error_message = "Unknown SCRAM mechanism",
      });
    check.emplace_back(
      kafka::alter_user_scram_credentials_result{
        .user = kafka::scram_user_name{"test2"},
        .error_code = kafka::error_code::unsupported_sasl_mechanism,
        .error_message = "Unknown SCRAM mechanism",
      });

    std::ranges::sort(
      check, {}, &kafka::alter_user_scram_credentials_result::user);
    std::ranges::sort(
      resp.data.results, {}, &kafka::alter_user_scram_credentials_result::user);
    BOOST_REQUIRE_EQUAL(resp.data.results, check);
}

FIXTURE_TEST(
  alter_user_scram_credentials_invalid_iterations,
  alter_user_scram_credentials_fixture) {
    static constexpr auto max_iterations = 16384;
    wait_for_controller_leadership().get();

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    kafka::alter_user_scram_credentials_request req;
    req.data.upsertions.emplace_back(
      kafka::scram_credential_upsertion{
        .name = kafka::scram_user_name{"test1"},
        .mechanism = kafka::scram_mechanism::scram_sha_256,
        .iterations = security::scram_sha256::min_iterations - 1,
      });
    req.data.upsertions.emplace_back(
      kafka::scram_credential_upsertion{
        .name = kafka::scram_user_name{"test2"},
        .mechanism = kafka::scram_mechanism::scram_sha_256,
        .iterations = max_iterations + 1,
      });
    req.data.upsertions.emplace_back(
      kafka::scram_credential_upsertion{
        .name = kafka::scram_user_name{"test3"},
        .mechanism = kafka::scram_mechanism::scram_sha_512,
        .iterations = security::scram_sha512::min_iterations - 1,
      });
    req.data.upsertions.emplace_back(
      kafka::scram_credential_upsertion{
        .name = kafka::scram_user_name{"test4"},
        .mechanism = kafka::scram_mechanism::scram_sha_512,
        .iterations = max_iterations + 1,
      });

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    BOOST_REQUIRE(resp.data.errored());
    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 4);

    chunked_vector<kafka::alter_user_scram_credentials_result> check;
    check.reserve(4);
    check.emplace_back(
      kafka::alter_user_scram_credentials_result{
        .user = kafka::scram_user_name{"test1"},
        .error_code = kafka::error_code::unacceptable_credential,
        .error_message = "Too few iterations",
      });
    check.emplace_back(
      kafka::alter_user_scram_credentials_result{
        .user = kafka::scram_user_name{"test2"},
        .error_code = kafka::error_code::unacceptable_credential,
        .error_message = "Too many iterations",
      });
    check.emplace_back(
      kafka::alter_user_scram_credentials_result{
        .user = kafka::scram_user_name{"test3"},
        .error_code = kafka::error_code::unacceptable_credential,
        .error_message = "Too few iterations",
      });
    check.emplace_back(
      kafka::alter_user_scram_credentials_result{
        .user = kafka::scram_user_name{"test4"},
        .error_code = kafka::error_code::unacceptable_credential,
        .error_message = "Too many iterations",
      });

    std::ranges::sort(
      check, {}, &kafka::alter_user_scram_credentials_result::user);
    std::ranges::sort(
      resp.data.results, {}, &kafka::alter_user_scram_credentials_result::user);
    BOOST_REQUIRE_EQUAL(resp.data.results, check);
}

FIXTURE_TEST(
  alter_user_scram_credentials_duplicates,
  alter_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    kafka::alter_user_scram_credentials_request req;
    req.data.upsertions.emplace_back(
      kafka::scram_credential_upsertion{
        .name = kafka::scram_user_name{"test1"},
        .mechanism = kafka::scram_mechanism::scram_sha_256,
        .iterations = security::scram_sha256::min_iterations,
      });
    req.data.upsertions.emplace_back(
      kafka::scram_credential_upsertion{
        .name = kafka::scram_user_name{"test1"},
        .mechanism = kafka::scram_mechanism::scram_sha_256,
        .iterations = security::scram_sha256::min_iterations,
      });
    req.data.deletions.emplace_back(
      kafka::scram_credential_deletion{
        .name = kafka::scram_user_name{"test1"},
        .mechanism = kafka::scram_mechanism::scram_sha_256,
      });
    req.data.deletions.emplace_back(
      kafka::scram_credential_deletion{
        .name = kafka::scram_user_name{"test1"},
        .mechanism = kafka::scram_mechanism::scram_sha_256,
      });

    auto resp = client.dispatch(std::move(req), kafka::api_version(0)).get();
    BOOST_REQUIRE(resp.data.errored());
    BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
    BOOST_CHECK_EQUAL(resp.data.results[0].user, "test1");
    BOOST_CHECK_EQUAL(
      resp.data.results[0].error_code, kafka::error_code::duplicate_resource);
}

FIXTURE_TEST(
  alter_user_scram_credentials_superuser_to_superuser,
  alter_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();

    ss::smp::invoke_on_all([]() {
        config::shard_local_cfg()
          .get("superusers")
          .set_value(std::vector<ss::sstring>{user_name_256, user_name_512});
    }).get();

    auto deferred_config = ss::defer([] {
        ss::smp::invoke_on_all([]() {
            config::shard_local_cfg()
              .get("superusers")
              .set_value(std::vector<ss::sstring>{});
        }).get();
    });

    auto creds_256 = security::scram_sha256::make_credentials(
      password_256, security::scram_sha256::min_iterations);
    create_user(user_name_256, creds_256);

    auto [creds_512, salted_password_512]
      = security::scram_sha512::make_credentials_and_return_password(
        password_512, security::scram_sha512::min_iterations);
    create_user(user_name_512, creds_512);

    enable_sasl();
    auto disable_sasl_defer = ss::defer([this] { disable_sasl(); });

    const auto make_acl_binding = [](const std::string_view name) {
        return security::acl_binding(
          security::resource_pattern(
            security::resource_type::cluster,
            security::default_cluster_name,
            security::pattern_type::literal),
          security::acl_entry(
            kafka::details::to_acl_principal(ssx::sformat("User:{}", name)),
            security::acl_host::wildcard_host(),
            security::acl_operation::alter,
            security::acl_permission::allow));
    };
    std::vector<security::acl_binding> cluster_bindings{
      make_acl_binding(user_name_256),
      make_acl_binding(user_name_512),
    };

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
    authn_kafka_client<security::scram_sha512_authenticator>(
      client, user_name_512, password_512);

    // a superuser can update another superuser
    {
        kafka::alter_user_scram_credentials_request req;
        req.data.upsertions.emplace_back(
          kafka::scram_credential_upsertion{
            .name = kafka::scram_user_name{user_name_256},
            .mechanism = kafka::scram_mechanism::scram_sha_512,
            .iterations = security::scram_sha512::min_iterations,
            .salt = creds_512.salt(),
            .salted_password = salted_password_512,
          });

        auto resp
          = client.dispatch(std::move(req), kafka::api_version(0)).get();
        BOOST_REQUIRE(!resp.data.errored());
        BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
        BOOST_CHECK_EQUAL(resp.data.results[0].user, user_name_256);
        BOOST_CHECK_EQUAL(
          resp.data.results[0].error_code, kafka::error_code::none);

        auto& sec = app.controller->get_credential_store().local();
        BOOST_REQUIRE(sec.contains(security::credential_user(user_name_256)));
        auto cred = sec.get<security::scram_credential>(
          security::credential_user(user_name_256));
        BOOST_REQUIRE(cred.has_value());
        BOOST_REQUIRE_MESSAGE(
          !cred->password_set_at().is_missing(),
          "Credential should have password_set_at");
        BOOST_CHECK_EQUAL(cred, creds_512);
    }

    // a superuser can delete another superuser
    {
        kafka::alter_user_scram_credentials_request req;
        req.data.deletions.emplace_back(
          kafka::scram_credential_deletion{
            .name = kafka::scram_user_name{user_name_256},
            .mechanism = kafka::scram_mechanism::scram_sha_512});

        auto resp
          = client.dispatch(std::move(req), kafka::api_version(0)).get();
        BOOST_REQUIRE(!resp.data.errored());
        BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
        BOOST_CHECK_EQUAL(resp.data.results[0].user, user_name_256);
        BOOST_CHECK_EQUAL(
          resp.data.results[0].error_code, kafka::error_code::none);

        auto& sec = app.controller->get_credential_store().local();
        BOOST_REQUIRE(!sec.contains(security::credential_user(user_name_256)));
    }
}

FIXTURE_TEST(
  alter_user_scram_credentials_user_to_superuser,
  alter_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();

    ss::smp::invoke_on_all([]() {
        config::shard_local_cfg()
          .get("superusers")
          .set_value(std::vector<ss::sstring>{user_name_256});
    }).get();
    auto deferred_config = ss::defer([] {
        ss::smp::invoke_on_all([]() {
            config::shard_local_cfg()
              .get("superusers")
              .set_value(std::vector<ss::sstring>{});
        }).get();
    });

    auto creds_256 = security::scram_sha256::make_credentials(
      password_256, security::scram_sha256::min_iterations);
    create_user(user_name_256, creds_256);

    auto [creds_512, salted_password_512]
      = security::scram_sha512::make_credentials_and_return_password(
        password_512, security::scram_sha512::min_iterations);
    create_user(user_name_512, creds_512);

    enable_sasl();
    auto disable_sasl_defer = ss::defer([this] { disable_sasl(); });

    const auto make_acl_binding = [](const std::string_view name) {
        return security::acl_binding(
          security::resource_pattern(
            security::resource_type::cluster,
            security::default_cluster_name,
            security::pattern_type::literal),
          security::acl_entry(
            kafka::details::to_acl_principal(ssx::sformat("User:{}", name)),
            security::acl_host::wildcard_host(),
            security::acl_operation::alter,
            security::acl_permission::allow));
    };
    std::vector<security::acl_binding> cluster_bindings{
      make_acl_binding(user_name_256),
      make_acl_binding(user_name_512),
    };

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
    authn_kafka_client<security::scram_sha512_authenticator>(
      client, user_name_512, password_512);

    // a user cannot update a superuser
    {
        auto [creds_512, salted_password_512]
          = security::scram_sha512::make_credentials_and_return_password(
            password_512, security::scram_sha512::min_iterations);

        kafka::alter_user_scram_credentials_request req;
        req.data.upsertions.emplace_back(
          kafka::scram_credential_upsertion{
            .name = kafka::scram_user_name{user_name_256},
            .mechanism = kafka::scram_mechanism::scram_sha_512,
            .iterations = security::scram_sha512::min_iterations,
            .salt = creds_512.salt(),
            .salted_password = salted_password_512,
          });

        auto resp
          = client.dispatch(std::move(req), kafka::api_version(0)).get();
        BOOST_REQUIRE(resp.data.errored());
        BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
        BOOST_CHECK_EQUAL(resp.data.results[0].user, user_name_256);
        BOOST_CHECK_EQUAL(
          resp.data.results[0].error_code,
          kafka::error_code::cluster_authorization_failed);

        auto& sec = app.controller->get_credential_store().local();
        BOOST_REQUIRE(sec.contains(security::credential_user(user_name_256)));
        auto cred = sec.get<security::scram_credential>(
          security::credential_user(user_name_256));
        BOOST_REQUIRE(cred.has_value());
        BOOST_REQUIRE_MESSAGE(
          !cred->password_set_at().is_missing(),
          "Credential should have password_set_at");
        BOOST_CHECK_EQUAL(cred, creds_256);
    }

    // a user cannot delete a superuser
    {
        kafka::alter_user_scram_credentials_request req;
        req.data.deletions.emplace_back(
          kafka::scram_credential_deletion{
            .name = kafka::scram_user_name{user_name_256},
            .mechanism = kafka::scram_mechanism::scram_sha_256});

        auto resp
          = client.dispatch(std::move(req), kafka::api_version(0)).get();
        BOOST_REQUIRE(resp.data.errored());
        BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
        BOOST_CHECK_EQUAL(resp.data.results[0].user, user_name_256);
        BOOST_CHECK_EQUAL(
          resp.data.results[0].error_code,
          kafka::error_code::cluster_authorization_failed);

        auto& sec = app.controller->get_credential_store().local();
        BOOST_REQUIRE(sec.contains(security::credential_user(user_name_256)));
    }
}

FIXTURE_TEST(
  alter_user_scram_credentials_user_to_superuser_noauthz,
  alter_user_scram_credentials_fixture) {
    wait_for_controller_leadership().get();

    ss::smp::invoke_on_all([]() {
        config::shard_local_cfg()
          .get("superusers")
          .set_value(std::vector<ss::sstring>{user_name_256});
    }).get();
    auto deferred_config = ss::defer([] {
        ss::smp::invoke_on_all([]() {
            config::shard_local_cfg()
              .get("superusers")
              .set_value(std::vector<ss::sstring>{});
        }).get();
    });

    auto creds_256 = security::scram_sha256::make_credentials(
      password_256, security::scram_sha256::min_iterations);
    create_user(user_name_256, creds_256);

    auto client = make_kafka_client().get();
    auto deferred_close = ss::defer([&client] { client.stop().get(); });
    client.connect().get();

    // any user can update a superuser when authz checks are disabled
    {
        auto [creds_512, salted_password_512]
          = security::scram_sha512::make_credentials_and_return_password(
            password_512, security::scram_sha512::min_iterations);

        kafka::alter_user_scram_credentials_request req;
        req.data.upsertions.emplace_back(
          kafka::scram_credential_upsertion{
            .name = kafka::scram_user_name{user_name_256},
            .mechanism = kafka::scram_mechanism::scram_sha_512,
            .iterations = security::scram_sha512::min_iterations,
            .salt = creds_512.salt(),
            .salted_password = salted_password_512,
          });

        auto resp
          = client.dispatch(std::move(req), kafka::api_version(0)).get();
        BOOST_REQUIRE(!resp.data.errored());
        BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
        BOOST_CHECK_EQUAL(resp.data.results[0].user, user_name_256);
        BOOST_CHECK_EQUAL(
          resp.data.results[0].error_code, kafka::error_code::none);

        auto& sec = app.controller->get_credential_store().local();
        BOOST_REQUIRE(sec.contains(security::credential_user(user_name_256)));
        auto cred = sec.get<security::scram_credential>(
          security::credential_user(user_name_256));
        BOOST_REQUIRE(cred.has_value());
        BOOST_REQUIRE_MESSAGE(
          !cred->password_set_at().is_missing(),
          "Credential should have password_set_at");
        BOOST_CHECK_EQUAL(cred, creds_512);
    }

    // any user can delete a superuser when authz checks are disabled
    {
        kafka::alter_user_scram_credentials_request req;
        req.data.deletions.emplace_back(
          kafka::scram_credential_deletion{
            .name = kafka::scram_user_name{user_name_256},
            .mechanism = kafka::scram_mechanism::scram_sha_512});

        auto resp
          = client.dispatch(std::move(req), kafka::api_version(0)).get();
        BOOST_REQUIRE(!resp.data.errored());
        BOOST_REQUIRE_EQUAL(resp.data.results.size(), 1);
        BOOST_CHECK_EQUAL(resp.data.results[0].user, user_name_256);
        BOOST_CHECK_EQUAL(
          resp.data.results[0].error_code, kafka::error_code::none);

        auto& sec = app.controller->get_credential_store().local();
        BOOST_REQUIRE(!sec.contains(security::credential_user(user_name_256)));
    }
}
