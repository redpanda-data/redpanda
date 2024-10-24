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
#include "kafka/protocol/errors.h"
#include "kafka/protocol/metadata.h"
#include "kafka/protocol/produce.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "pandaproxy/test/utils.h"
#include "utils/unresolved_address.h"

#include <seastar/util/defer.hh>

#include <chrono>

namespace kc = kafka::client;

FIXTURE_TEST(reconnect, kafka_client_fixture) {
    using namespace std::chrono_literals;

    info("Waiting for leadership");
    wait_for_controller_leadership().get();

    auto tp = model::topic_partition(model::topic("t"), model::partition_id(0));
    auto client = make_connected_client();
    client.config().retry_base_backoff.set_value(10ms);
    client.config().retries.set_value(size_t(0));

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
        BOOST_REQUIRE_EQUAL(res.data.topics[0].name(), "t");
    }

    {
        info("Restarting broker");
        restart();
    }

    {
        info("Checking for known topic - expect controller not ready");
        auto res = client.dispatch(make_list_topics_req());
        BOOST_REQUIRE_THROW(res.get(), kc::broker_error);
    }

    {
        client.config().retries.set_value(size_t(5));
        info("Checking for known topic - controller ready");
        auto res = client.dispatch(make_list_topics_req()).get();
        BOOST_REQUIRE_EQUAL(res.data.topics.size(), 1);
        BOOST_REQUIRE_EQUAL(res.data.topics[0].name(), "t");
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
    kafka_client.config().sasl_mechanism.set_value(
      ss::sstring{"SCRAM-SHA-256"});
    kafka_client.config().scram_username.set_value(username);
    kafka_client.config().scram_password.set_value(userpass);
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
        BOOST_REQUIRE_EQUAL(res.data.topics[0].name(), "t");
    }

    {
        // Setting the password has no effect until the client disconnects
        info("Changing password");
        userpass = "foobar";
        kafka_client.config().scram_password.set_value(userpass);
    }

    {
        info("Recheck for known topic");
        auto res = kafka_client.dispatch(make_list_topics_req()).get();
        BOOST_REQUIRE_EQUAL(res.data.topics.size(), 1);
        BOOST_REQUIRE_EQUAL(res.data.topics[0].name(), "t");
    }

    info("Stopping kafka client");
    kafka_client.stop().get();
}
