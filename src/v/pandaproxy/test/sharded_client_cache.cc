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

#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/defer.hh>

#include <boost/test/unit_test.hpp>

#include <chrono>

namespace pp = pandaproxy;

SEASTAR_THREAD_TEST_CASE(test_sharded_client_cache) {
    pp::credential_t user;
    size_t cache_size = 10;
    model::timestamp::type keep_alive = 5000;

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
        cache_size,
        config::node().kafka_api.value(),
        keep_alive)
      .get();
    auto stop_client_cache = ss::defer(
      [&client_cache]() { client_cache.stop().get(); });

    // First client fetch will test the creation path
    client_cache.fetch_client(user, config::rest_authn_type::http_basic)
      .then([keep_alive, &client_cache, &user](pp::client_ptr client1) {
          // Second client fetch will test the reuse path
          return client_cache
            .fetch_client(user, config::rest_authn_type::http_basic)
            .then([keep_alive, client1, &client_cache, &user](
                    pp::client_ptr client2) {
                BOOST_TEST(
                  client1->creation_time() == client2->creation_time());

                // Wait a bit for the client to become stale
                // The third client fetch will test client clean and
                // re-make. So the creation times should be different.
                auto ms_keep_alive = std::chrono::milliseconds(keep_alive);
                return ss::sleep(ss::lowres_clock::duration(ms_keep_alive))
                  .then([client1, &client_cache, &user] {
                      return client_cache
                        .fetch_client(user, config::rest_authn_type::http_basic)
                        .then([client1](pp::client_ptr client3) {
                            BOOST_TEST(
                              client1->creation_time()
                              != client3->creation_time());
                        });
                  });
            });
      })
      .get();
}
