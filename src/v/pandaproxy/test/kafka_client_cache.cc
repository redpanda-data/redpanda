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
#include "pandaproxy/client_cache_error.h"
#include "pandaproxy/types.h"

#include <seastar/core/sleep.hh>
#include <seastar/testing/thread_test_case.hh>

#include <boost/test/unit_test.hpp>

#include <chrono>
#include <vector>

using namespace std::chrono_literals;

namespace pandaproxy {
class test_client_cache : public kafka_client_cache {
public:
    test_client_cache(size_t max_size, std::vector<config::broker_authn_endpoint> kafka_api)
      : kafka_client_cache(to_yaml(kafka::client::configuration{}, config::redact_secrets::no), max_size, std::move(kafka_api), (1000ms).count()) {}

    test_client_cache(size_t max_size)
      : test_client_cache(max_size, std::move(config::node().kafka_api.value())) {}

    client_ptr fetch(credential_t user) {
      return kafka_client_cache::fetch(user);
    }

    void insert(credential_t user, client_ptr client) {
      kafka_client_cache::insert(user, client);
    }

    client_ptr
    make_client(credential_t user, config::rest_authn_type authn_type) {
      return kafka_client_cache::make_client(user, authn_type);
    }
};
} // namespace pandaproxy

namespace pp = pandaproxy;

SEASTAR_THREAD_TEST_CASE(test_cache_make_client) {
  size_t max_size{10};
  auto kafka_api = std::move(config::node().kafka_api.value());
  kafka_api.clear();
  kafka_api.push_back(config::broker_authn_endpoint{.address = net::unresolved_address("127.0.0.1", 9092), .authn_method = config::broker_authn_method::sasl});
  pp::test_client_cache client_cache{max_size, std::move(kafka_api)};

  {
    // Creating a client with no authn type results in a kafka
    // client without a principal
    pp::client_ptr client = client_cache.make_client(pp::credential_t{}, config::rest_authn_type::none);
    BOOST_TEST(client->real.config().sasl_mechanism.value() == "");
    BOOST_TEST(client->real.config().scram_username.value() == "");
    BOOST_TEST(client->real.config().scram_password.value() == "");
  }

  {
    // Creating a client with http_basic authn type results
    // in a kafka client with a principal
    pp::credential_t user{"red", "panda"};
    pp::client_ptr client = client_cache.make_client(user, config::rest_authn_type::http_basic);
    BOOST_TEST(client->real.config().sasl_mechanism.value() == ss::sstring{"SCRAM-SHA-256"});
    BOOST_TEST(client->real.config().scram_username.value() == user.name);
    BOOST_TEST(client->real.config().scram_password.value() == user.pass);
  }
}

SEASTAR_THREAD_TEST_CASE(test_cache_insert_and_fetch_client) {
  {
    pp::credential_t user1;
    size_t max_size{1};
    pp::test_client_cache client_cache{max_size};
    BOOST_TEST(client_cache.size() == 0);

    pp::client_ptr client1 = client_cache.make_client(user1, config::rest_authn_type::none);

    // First insert should pass without problems
    client_cache.insert(user1, client1);

    // Create a new client with a different user
    // and insert.
    pp::credential_t user2{"red", "panda"};
    pp::client_ptr client2 = client_cache.make_client(user2, config::rest_authn_type::none);
    client_cache.insert(user2, client2);

    // The LRU replacement should have triggered so
    // the cache size should still be 1 and a fetch on the
    // first user should fail with nullptr.
    BOOST_TEST(client_cache.size() == 1);
    BOOST_TEST(client_cache.max_size() == max_size);
    pp::client_ptr temp1 = client_cache.fetch(user1);
    BOOST_CHECK(temp1 == pp::client_ptr(nullptr));

    // A fetch on the second user should succeed
    // and it should have the latest timestamp.
    // Also do a short sleep so the timestamps are different.
    ss::sleep(ss::lowres_clock::duration(1ms)).get();
    model::timestamp original_t = client2->last_used;
    pp::client_ptr temp2 = client_cache.fetch(user2);
    BOOST_CHECK(&temp2->real == &client2->real);
    BOOST_TEST(temp2->last_used() > original_t());
  }

  {
    size_t max_size{0};
    pp::test_client_cache client_cache{max_size};

    // Insert with max size 0 should fail
    try {
      client_cache.insert(pp::credential_t{}, nullptr);
    } catch (const pp::client_cache_error &ex) {
      BOOST_TEST(ex.what() == "Failed to insert client. Max cache size is 0.");
    }
  }
}
