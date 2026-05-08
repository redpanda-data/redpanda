// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "http/client.h"
#include "kafka/client/client.h"
#include "kafka/client/configuration.h"
#include "kafka/client/test/fixture.h"
#include "kafka/client/test/utils.h"
#include "kafka/protocol/metadata.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "pandaproxy/test/utils.h"
#include "test_utils/boost_fixture.h"

#include <seastar/util/defer.hh>

namespace kc = kafka::client;

FIXTURE_TEST(reconnect, kafka_client_fixture) {
    using namespace std::chrono_literals;

    info("Waiting for leadership");
    wait_for_controller_leadership().get();

    auto tp = model::topic_partition(model::topic("t"), model::partition_id(0));
    auto client = make_connected_client();
    client.set_retry_base_backoff(10ms);
    client.set_max_retries(size_t(0));

    {
        info("Checking no topics");
        auto res = client.dispatch(make_list_topics_req()).get();
        BOOST_REQUIRE_EQUAL(res.data.topics.size(), 0);
    }

    {
        info("Adding known topic");
        auto ntp = make_default_ntp(tp.topic, tp.partition);
        add_topic(model::topic_namespace_view(ntp)).get();
    }

    {
        info("Checking for known topic");
        auto res = client.dispatch(make_list_topics_req()).get();
        BOOST_REQUIRE_EQUAL(res.data.topics.size(), 1);
        static_assert(
          kc::api_version_for(kafka::metadata_request::api_type::key)
            < kafka::api_version(12),
          "topic::name is nullable in v12+");
        BOOST_REQUIRE_EQUAL((*res.data.topics[0].name)(), "t");
    }

    {
        info("Restarting broker");
        restart();
    }

    {
        client.set_max_retries(size_t(5));
        info("Checking for known topic - controller ready");
        auto res = client.dispatch(make_list_topics_req()).get();
        BOOST_REQUIRE_EQUAL(res.data.topics.size(), 1);
        static_assert(
          kc::api_version_for(kafka::metadata_request::api_type::key)
            < kafka::api_version(12),
          "topic::name is nullable in v12+");
        BOOST_REQUIRE_EQUAL((*res.data.topics[0].name)(), "t");
    }

    info("Stopping client");
    client.stop().get();
}
inline http::client make_admin_client() {
    net::base_transport::configuration transport_cfg;
    transport_cfg.server_addr = net::unresolved_address{"127.0.0.1", 9644};
    return http::client(transport_cfg);
}

FIXTURE_TEST(password_change_live_client, kafka_client_fixture) {
    ss::sstring username{"admin"};
    ss::sstring userpass{"foopar"};

    info("Enable SASL and restart");
    enable_sasl_and_restart(username);
    auto disable_sasl = ss::defer([this] {
        // This is necessary or else subsequent fixture tests will also have
        // SASL enabled
        info("Disable SASL and restart");
        disable_sasl_and_restart();
    });
    info("Waiting for leadership");
    wait_for_controller_leadership().get();
    ss::sstring user_body = fmt::format(
      R"({{"username": "{}", "password": "{}","algorithm": "SCRAM-SHA-256"}})",
      username,
      userpass);
    auto body = iobuf();
    body.append(user_body.data(), user_body.size());

    info("Create superuser");
    auto admin_client = make_admin_client();
    auto res = http_request(
      admin_client,
      "/v1/security/users",
      std::move(body),
      boost::beast::http::verb::post);
    BOOST_REQUIRE_EQUAL(res.headers.result(), boost::beast::http::status::ok);

    auto tp = model::topic_partition(model::topic("t"), model::partition_id(0));
    auto kafka_client = make_client();
    kafka_client.set_credentials(
      kc::sasl_configuration{
        .mechanism = "SCRAM-SHA-256",
        .username = username,
        .password = userpass,
      });

    kafka_client.connect().get();

    {
        info("Adding known topic");
        auto ntp = make_default_ntp(tp.topic, tp.partition);
        add_topic(model::topic_namespace_view(ntp)).get();
    }

    {
        info("Checking for known topic");
        auto res = kafka_client.dispatch(make_list_topics_req()).get();
        BOOST_REQUIRE_EQUAL(res.data.topics.size(), 1);
        static_assert(
          kc::api_version_for(kafka::metadata_request::api_type::key)
            < kafka::api_version(12),
          "topic::name is nullable in v12+");
        BOOST_REQUIRE_EQUAL((*res.data.topics[0].name)(), "t");
    }

    {
        // Setting the password has no effect until the client disconnects
        info("Changing password");
        userpass = "foobar";
        kafka_client.set_credentials(
          kc::sasl_configuration{
            .mechanism = "SCRAM-SHA-256",
            .username = username,
            .password = userpass,
          });
    }

    {
        info("Recheck for known topic");
        auto res = kafka_client.dispatch(make_list_topics_req()).get();
        BOOST_REQUIRE_EQUAL(res.data.topics.size(), 1);
        static_assert(
          kc::api_version_for(kafka::metadata_request::api_type::key)
            < kafka::api_version(12),
          "topic::name is nullable in v12+");
        BOOST_REQUIRE_EQUAL((*res.data.topics[0].name)(), "t");
    }

    info("Stopping kafka client");
    kafka_client.stop().get();
}

FIXTURE_TEST(auth_failure_does_not_get_stuck_in_progress, kafka_client_fixture) {
    using namespace std::chrono_literals;
    ss::sstring username{"admin"};
    ss::sstring correct_password{"correct_pass"};
    ss::sstring wrong_password{"wrong_pass"};

    info("Enable SASL and restart");
    enable_sasl_and_restart(username);
    auto disable_sasl = ss::defer([this] {
        info("Disable SASL and restart");
        disable_sasl_and_restart();
    });
    info("Waiting for leadership");
    wait_for_controller_leadership().get();

    info("Create superuser");
    ss::sstring user_body = fmt::format(
      R"({{"username": "{}", "password": "{}","algorithm": "SCRAM-SHA-256"}})",
      username,
      correct_password);
    auto body = iobuf();
    body.append(user_body.data(), user_body.size());
    auto admin_client = make_admin_client();
    auto res = http_request(
      admin_client,
      "/v1/security/users",
      std::move(body),
      boost::beast::http::verb::post);
    BOOST_REQUIRE_EQUAL(res.headers.result(), boost::beast::http::status::ok);

    // Connect successfully with correct credentials first so the cluster is
    // started and brokers are discovered.
    auto kafka_client = make_client();
    kafka_client.set_retry_base_backoff(10ms);
    kafka_client.set_max_retries(size_t(0));
    kafka_client.set_credentials(
      kc::sasl_configuration{
        .mechanism = "SCRAM-SHA-256",
        .username = username,
        .password = correct_password,
      });
    kafka_client.connect().get();

    // Now swap in wrong credentials and force a reconnect by dispatching a
    // request. The broker will reconnect and attempt authentication with the
    // wrong password, leaving _authentication_state in a failed state.
    // Before the fix, the state was left as in_progress, which caused
    // needs_authentication() to return false on the next attempt, silently
    // skipping re-authentication and making all subsequent requests fail.
    info("Swapping to wrong credentials");
    kafka_client.set_credentials(
      kc::sasl_configuration{
        .mechanism = "SCRAM-SHA-256",
        .username = username,
        .password = wrong_password,
      });

    // Force the broker to reconnect and hit the bad auth path. Allow 1 retry
    // so transient disconnect/broker_not_available after restart does not make
    // the test flaky before it reaches the SASL auth path.
    restart(true);
    wait_for_controller_leadership().get();
    kafka_client.set_max_retries(size_t(1));
    BOOST_REQUIRE_EXCEPTION(
      kafka_client.dispatch(make_list_topics_req()).get(),
      kc::broker_error,
      [](const kc::broker_error& e) {
          return e.error == kafka::error_code::sasl_authentication_failed;
      });

    // Restore correct credentials. With the fix, _authentication_state was
    // reset to none on failure, so the next dispatch re-attempts auth and
    // succeeds. Without the fix it stays in_progress and the request fails.
    info("Restoring correct credentials");
    kafka_client.set_credentials(
      kc::sasl_configuration{
        .mechanism = "SCRAM-SHA-256",
        .username = username,
        .password = correct_password,
      });
    kafka_client.set_max_retries(size_t(3));

    {
        info("Adding a known topic to verify authenticated request succeeds");
        auto tp = model::topic_partition(
          model::topic("t"), model::partition_id(0));
        auto ntp = make_default_ntp(tp.topic, tp.partition);
        add_topic(model::topic_namespace_view(ntp)).get();

        info("Verifying client recovers after auth failure");
        auto result = kafka_client.dispatch(make_list_topics_req()).get();
        BOOST_REQUIRE_EQUAL(result.data.topics.size(), 1);
        static_assert(
          kc::api_version_for(kafka::metadata_request::api_type::key)
            < kafka::api_version(12),
          "topic::name is nullable in v12+");
        BOOST_REQUIRE_EQUAL((*result.data.topics[0].name)(), "t");
    }

    info("Stopping kafka client");
    kafka_client.stop().get();
}
