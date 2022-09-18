// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/sharded_client_cache.h"

#include "config/node_config.h"
#include "config/rest_authn_endpoint.h"
#include "kafka/client/configuration.h"
#include "pandaproxy/types.h"
#include "pandaproxy/kafka_client_cache.h"

#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

#include <chrono>

using namespace std::chrono_literals;

namespace pp = pandaproxy;

SEASTAR_THREAD_TEST_CASE(test_sharded_client_cache) {
    pp::credential_t user;
    size_t cache_max_size = 10;
    std::chrono::milliseconds keep_alive{1000ms};

    kafka::client::configuration cfg;
    cfg.sasl_mechanism.set_value(ss::sstring{"SCRAM-SHA-256"});
    cfg.scram_username.set_value(user.name);
    cfg.scram_password.set_value(user.pass);
    BOOST_REQUIRE_EQUAL(cfg.scram_username.value(), user.name);
    BOOST_REQUIRE_EQUAL(cfg.scram_password.value(), user.pass);

    pp::sharded_client_cache client_cache;
    client_cache
      .start(
        ss::default_smp_service_group(),
        to_yaml(cfg, config::redact_secrets::no),
        cache_max_size,
        config::node().kafka_api.value(),
        keep_alive.count())
      .get();
    auto stop_client_cache = ss::defer(
      [&client_cache]() { client_cache.stop().get(); });

    pp::client_ptr client1 = client_cache.fetch_client(user, config::rest_authn_type::none).get();
    pp::client_ptr client2 = client_cache.fetch_client(user, config::rest_authn_type::none).get();
    BOOST_CHECK(&client1->real == &client2->real);
    BOOST_TEST(client1->last_used() == client2->last_used());

    ss::sleep(ss::lowres_clock::duration(keep_alive + pp::kafka_client_cache::clean_timer())).get();
    pp::client_ptr client3 = client_cache.fetch_client(user, config::rest_authn_type::none).get();
    BOOST_CHECK(&client1->real != &client3->real);
    BOOST_TEST(client1->last_used() != client3->last_used());
}

SEASTAR_THREAD_TEST_CASE(test_keep_alive) {
    pp::credential_t user;
    size_t cache_max_size = 10;

    kafka::client::configuration cfg;
    cfg.sasl_mechanism.set_value(ss::sstring{"SCRAM-SHA-256"});
    cfg.scram_username.set_value(user.name);
    cfg.scram_password.set_value(user.pass);
    BOOST_REQUIRE_EQUAL(cfg.scram_username.value(), user.name);
    BOOST_REQUIRE_EQUAL(cfg.scram_password.value(), user.pass);

    {
      // A negative keep alive means that idle clients should
      // be immediately removed when garbage collection
      // kicks in. Note that garbage collection occurs
      // on a 10s interval.
      std::chrono::milliseconds keep_alive{-1000ms};

      pp::sharded_client_cache client_cache;
      client_cache
        .start(
          ss::default_smp_service_group(),
          to_yaml(cfg, config::redact_secrets::no),
          cache_max_size,
          config::node().kafka_api.value(),
          keep_alive.count())
        .get();
      auto stop_client_cache = ss::defer(
        [&client_cache]() { client_cache.stop().get(); });

      pp::client_ptr client1 = client_cache.fetch_client(user, config::rest_authn_type::none).get();
      ss::sleep(ss::lowres_clock::duration(pp::kafka_client_cache::clean_timer())).get();
      pp::client_ptr client2 = client_cache.fetch_client(user, config::rest_authn_type::none).get();
      BOOST_CHECK(&client1->real != &client2->real);
      BOOST_TEST(client1->last_used() != client2->last_used());
    }

    {
      // A zero keep alive means the same thing as the
      // negative version. Regardless we should explictly
      // test that scenario.
      std::chrono::milliseconds keep_alive{0ms};
    
      pp::sharded_client_cache client_cache;
      client_cache
        .start(
          ss::default_smp_service_group(),
          to_yaml(cfg, config::redact_secrets::no),
          cache_max_size,
          config::node().kafka_api.value(),
          keep_alive.count())
        .get();
      auto stop_client_cache = ss::defer(
        [&client_cache]() { client_cache.stop().get(); });

      pp::client_ptr client1 = client_cache.fetch_client(user, config::rest_authn_type::none).get();
      ss::sleep(ss::lowres_clock::duration(pp::kafka_client_cache::clean_timer())).get();
      pp::client_ptr client2 = client_cache.fetch_client(user, config::rest_authn_type::none).get();
      BOOST_CHECK(&client1->real != &client2->real);
      BOOST_TEST(client1->last_used() != client2->last_used());
    }
}
