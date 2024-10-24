// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "pandaproxy/kafka_client_cache.h"

#include "config/node_config.h"
#include "config/rest_authn_endpoint.h"
#include "kafka/client/configuration.h"
#include "pandaproxy/types.h"
#include "security/scram_authenticator.h"
#include "ssx/future-util.h"

#include <seastar/core/gate.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/timer.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <chrono>
#include <vector>

using namespace std::chrono_literals;

static constexpr auto clean_timer_period = 10s;

namespace pandaproxy {
using cache_item = std::pair<client_ptr, client_mu_ptr>;

struct test_client_cache : public kafka_client_cache {
    explicit test_client_cache(size_t max_size)
      : kafka_client_cache(
          to_yaml(kafka::client::configuration{}, config::redact_secrets::no),
          max_size,
          1000ms) {
        start().get();
    }

    test_client_cache(size_t max_size, std::chrono::milliseconds keep_alive)
      : kafka_client_cache(
          to_yaml(kafka::client::configuration{}, config::redact_secrets::no),
          max_size,
          keep_alive) {
        start().get();
    }

    ~test_client_cache() { stop().get(); }

    cache_item
    get_client(credential_t user, config::rest_authn_method authn_method) {
        return fetch_or_insert(user, authn_method);
    }
};
} // namespace pandaproxy

namespace pp = pandaproxy;

SEASTAR_THREAD_TEST_CASE(cache_make_client_scram_sha_256) {
    pp::test_client_cache client_cache{10};
    pp::credential_t user{
      "red", "panda", security::scram_sha256_authenticator::name};

    {
        // Creating a client with no authn methods results in a kafka
        // client without a principal
        pp::client_ptr client = client_cache.make_client(
          user, config::rest_authn_method::none);
        BOOST_TEST(client->config().sasl_mechanism.value() == "");
        BOOST_TEST(client->config().scram_username.value() == "");
        BOOST_TEST(client->config().scram_password.value() == "");
    }

    {
        // Creating a client with http_basic authn type results
        // in a kafka client with a principal
        pp::client_ptr client = client_cache.make_client(
          user, config::rest_authn_method::http_basic);
        BOOST_TEST(
          client->config().sasl_mechanism.value()
          == ss::sstring{"SCRAM-SHA-256"});
        BOOST_TEST(client->config().scram_username.value() == user.name);
        BOOST_TEST(client->config().scram_password.value() == user.pass);
    }
}

SEASTAR_THREAD_TEST_CASE(cache_make_client_scram_sha_512) {
    pp::test_client_cache client_cache{10};
    pp::credential_t user{
      "red", "panda", security::scram_sha512_authenticator::name};

    {
        // Creating a client with no authn methods results in a kafka
        // client without a principal
        pp::client_ptr client = client_cache.make_client(
          user, config::rest_authn_method::none);
        BOOST_TEST(client->config().sasl_mechanism.value() == "");
        BOOST_TEST(client->config().scram_username.value() == "");
        BOOST_TEST(client->config().scram_password.value() == "");
    }

    {
        // Creating a client with http_basic authn type results
        // in a kafka client with a principal
        pp::client_ptr client = client_cache.make_client(
          user, config::rest_authn_method::http_basic);
        BOOST_TEST(
          client->config().sasl_mechanism.value()
          == ss::sstring{"SCRAM-SHA-512"});
        BOOST_TEST(client->config().scram_username.value() == user.name);
        BOOST_TEST(client->config().scram_password.value() == user.pass);
    }
}

SEASTAR_THREAD_TEST_CASE(cache_fetch_or_insert) {
    size_t s = 1, max_s = 1;
    pp::credential_t user{
      "red", "panda", security::scram_sha256_authenticator::name};
    pp::test_client_cache client_cache{s};
    BOOST_TEST(client_cache.size() == 0);
    BOOST_TEST(client_cache.max_size() == max_s);

    // First fetch tests not-found path: cache.size > cache.max_size and cache
    // is empty
    pp::cache_item item = client_cache.get_client(
      user, config::rest_authn_method::http_basic);
    pp::client_ptr client = item.first;
    BOOST_TEST(
      client->config().sasl_mechanism.value() == ss::sstring{"SCRAM-SHA-256"});
    BOOST_TEST(client->config().scram_username.value() == user.name);
    BOOST_TEST(client->config().scram_password.value() == user.pass);

    // Second fetch tests found path: user password did not change
    item = client_cache.get_client(user, config::rest_authn_method::http_basic);
    client = item.first;
    BOOST_TEST(
      client->config().sasl_mechanism.value() == ss::sstring{"SCRAM-SHA-256"});
    BOOST_TEST(client->config().scram_username.value() == user.name);
    BOOST_TEST(client->config().scram_password.value() == user.pass);

    pp::credential_t user2{user};
    user2.pass = "parrot";
    // Third fetch tests found path: user password did change
    // so any refs will have the updated password.
    item = client_cache.get_client(
      user2, config::rest_authn_method::http_basic);
    pp::client_ptr client2 = item.first;
    BOOST_TEST(
      client2->config().sasl_mechanism.value() == ss::sstring{"SCRAM-SHA-256"});
    BOOST_TEST(client2->config().scram_username.value() == user.name);
    BOOST_TEST(client2->config().scram_password.value() == user2.pass);
    BOOST_TEST(
      client->config().sasl_mechanism.value() == ss::sstring{"SCRAM-SHA-256"});
    BOOST_TEST(client->config().scram_username.value() == user.name);
    BOOST_TEST(client->config().scram_password.value() == user2.pass);

    user2.name = "party";
    // Fourth fetch tests not-found path: cache.size == cache.max_size and cache
    // is not empty. The LRU replacement policy takes affect and an element is
    // evicted
    item = client_cache.get_client(
      user2, config::rest_authn_method::http_basic);
    client2 = item.first;
    BOOST_TEST(
      client2->config().sasl_mechanism.value() == ss::sstring{"SCRAM-SHA-256"});
    BOOST_TEST(client2->config().scram_username.value() == user2.name);
    BOOST_TEST(client2->config().scram_password.value() == user2.pass);
    BOOST_TEST(client_cache.size() == s);
    BOOST_TEST(client_cache.max_size() == max_s);
}

SEASTAR_THREAD_TEST_CASE(test_keep_alive) {
    pp::credential_t user{
      "red", "panda", security::scram_sha256_authenticator::name};
    size_t cache_max_size = 5;
    std::chrono::milliseconds keep_alive{50ms};

    pp::test_client_cache client_cache{cache_max_size, keep_alive};

    // This will insert a client into the cache. The client is keyed by the user
    // name.
    client_cache.get_client(user, config::rest_authn_method::none);

    size_t s = client_cache.size();
    BOOST_CHECK(s == 1);
    ss::sleep(ss::lowres_clock::duration(clean_timer_period + keep_alive))
      .get();
    s = client_cache.size();
    BOOST_CHECK(s == 0);
}
