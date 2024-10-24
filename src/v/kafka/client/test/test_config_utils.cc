// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/base_property.h"
#include "config/configuration.h"
#include "config/node_config.h"
#include "config/tls_config.h"
#include "json/ostreamwrapper.h"
#include "json/writer.h"
#include "kafka/client/client.h"
#include "kafka/client/config_utils.h"
#include "kafka/client/configuration.h"
#include "model/namespace.h"
#include "redpanda/tests/fixture.h"
#include "security/acl.h"
#include "security/ephemeral_credential_store.h"
#include "security/sasl_authentication.h"
#include "security/scram_authenticator.h"
#include "security/types.h"

#include <seastar/util/defer.hh>

#include <boost/test/tools/old/interface.hpp>
#include <yaml-cpp/emitter.h>

#include <chrono>

namespace kafka::client {
// BOOST_REQURE_EQUAL fails to find this if it's in the global namespace
bool operator==(const configuration& lhs, const configuration& rhs) {
    return fmt::format("{}", config::to_yaml(lhs, config::redact_secrets::no))
           == fmt::format(
             "{}", config::to_yaml(rhs, config::redact_secrets::no));
}

std::ostream& operator<<(std::ostream& os, const configuration& c) {
    YAML::Emitter e{os};
    auto n = config::to_yaml(c, config::redact_secrets::no);
    n.SetStyle(YAML::EmitterStyle::value::Block);
    n["brokers"].SetStyle(YAML::EmitterStyle::value::Block);
    n["broker_tls"].SetStyle(YAML::EmitterStyle::value::Block);
    e << n;
    return os;
}
} // namespace kafka::client

namespace {

template<typename T>
std::unique_ptr<T> clone_config(const T& t) {
    return T{config::to_yaml(t), config::redact_secrets::no};
}

} // namespace

FIXTURE_TEST(test_config_utils, redpanda_thread_fixture) {
    using namespace std::chrono_literals;

    info("Waiting for leadership");
    wait_for_controller_leadership().get();

    const auto& ec_store
      = app.controller->get_ephemeral_credential_store().local();

    // Default configuration doesn't have a SASL listener
    config::node_config node_cfg;
    config::configuration& cluster_cfg{lconf()};

    // Default configuration doesn't override anything
    kafka::client::configuration client_cfg;

    security::acl_principal principal{
      security::principal_type::ephemeral_user, "ephemeral_user"};

    const auto create_credentials = [&, this]() {
        return kafka::client::create_client_credentials(
          *app.controller, cluster_cfg, client_cfg, principal);
    };

    // Default configuration, no authz - expect no changes
    {
        auto config = create_credentials().get();
        BOOST_REQUIRE_EQUAL(*config, client_cfg);
        BOOST_REQUIRE(!ec_store.has(ec_store.find(principal)));
    }

    const auto sasl_ep = config::broker_authn_endpoint{
      .name = "sasl",
      .address = net::unresolved_address{"127.0.0.2", 9092},
      .authn_method = config::broker_authn_method::sasl};

    // Configure a sasl listener:
    {
        std::vector<config::broker_authn_endpoint> kafka_api;
        kafka_api.push_back(sasl_ep);
        node_cfg.kafka_api.property::set_value(kafka_api);
    }

    // Expect no changes as authz isn't enabled:
    {
        auto config = create_credentials().get();
        BOOST_REQUIRE_EQUAL(*config, client_cfg);
        BOOST_REQUIRE(!ec_store.has(ec_store.find(principal)));
    }

    // Expect no changes when credentials overriden:
    {
        client_cfg.scram_username.set_value(ss::sstring{"user"});
        client_cfg.scram_password.set_value(ss::sstring{"pass"});
        client_cfg.sasl_mechanism.set_value(
          ss::sstring{security::scram_sha512_authenticator::name});

        auto config = create_credentials().get();
        BOOST_REQUIRE_EQUAL(*config, client_cfg);
        BOOST_REQUIRE(!ec_store.has(ec_store.find(principal)));

        // reset the credentials
        client_cfg.scram_username.reset();
        client_cfg.scram_password.reset();
        client_cfg.sasl_mechanism.reset();
    }

    // Enable authz
    cluster_cfg.kafka_enable_authorization.set_value(std::optional<bool>(true));

    // Expect credentials are created.
    {
        auto config = create_credentials().get();
        BOOST_REQUIRE_EQUAL(
          config->sasl_mechanism(), security::scram_sha512_authenticator::name);
        BOOST_REQUIRE(config->scram_username.is_overriden());
        BOOST_REQUIRE(config->scram_password.is_overriden());
        BOOST_REQUIRE(ec_store.has(ec_store.find(principal)));
    }
}
