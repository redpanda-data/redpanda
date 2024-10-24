// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/node_config.h"

#include <seastar/testing/thread_test_case.hh>

static const auto no_advertised_kafka_api_conf
  = "redpanda:\n"
    "  data_directory: /var/lib/redpanda/data\n"
    "  node_id: 1\n"
    "  rpc_server:\n"
    "    address: 127.0.0.1\n"
    "    port: 33145\n"
    "  kafka_api:\n"
    "    address: 192.168.1.1\n"
    "    port: 9999\n"
    "  seed_servers:\n"
    "    - host: \n"
    "        address: 127.0.0.1\n"
    "        port: 33145\n"
    "      node_id: 1\n"
    "  admin:\n"
    "    address: 127.0.0.1\n"
    "    port: 9644\n";

static const auto advertised_kafka_api_conf = "  advertised_kafka_api:\n"
                                              "    address: 10.48.0.2\n"
                                              "    port: 1234\n";

static const auto kafka_endpoints_conf_v2
  = "redpanda:\n"
    "  data_directory: /var/lib/redpanda/data\n"
    "  node_id: 1\n"
    "  rpc_server:\n"
    "    address: 127.0.0.1\n"
    "    port: 33145\n"
    "  kafka_api:\n"
    "    - address: 192.168.1.1\n"
    "      port: 9999\n"
    "    - address: 2.2.2.2\n"
    "      name: lala\n"
    "      port: 8888\n"
    "  seed_servers:\n"
    "    - host: \n"
    "        address: 127.0.0.1\n"
    "        port: 33145\n"
    "      node_id: 1\n"
    "  admin:\n"
    "    address: 127.0.0.1\n"
    "    port: 9644\n"
    "  advertised_kafka_api:\n"
    "    - address: 10.48.0.2\n"
    "      port: 1234\n"
    "    - address: 1.1.1.1\n"
    "      name: foobar\n"
    "      port: 9999\n";

YAML::Node no_advertised_kafka_api() {
    return YAML::Load(no_advertised_kafka_api_conf);
}

YAML::Node with_advertised_kafka_api() {
    ss::sstring conf = no_advertised_kafka_api_conf;
    conf += advertised_kafka_api_conf;
    return YAML::Load(conf);
}

SEASTAR_THREAD_TEST_CASE(shall_return_kafka_api_as_advertised_api_was_not_set) {
    config::node_config cfg;
    auto errors = cfg.load(no_advertised_kafka_api());
    BOOST_TEST(errors.size() == 0);
    auto adv_list = cfg.advertised_kafka_api();
    BOOST_REQUIRE_EQUAL(
      adv_list[0].address.host(), cfg.kafka_api()[0].address.host());
    BOOST_REQUIRE_EQUAL(
      adv_list[0].address.port(), cfg.kafka_api()[0].address.port());
};

SEASTAR_THREAD_TEST_CASE(shall_return_advertised_kafka_api) {
    config::node_config cfg;
    auto errors = cfg.load(with_advertised_kafka_api());
    BOOST_TEST(errors.size() == 0);
    auto adv_list = cfg.advertised_kafka_api();
    BOOST_REQUIRE_EQUAL(adv_list[0].address.host(), "10.48.0.2");
    BOOST_REQUIRE_EQUAL(adv_list[0].address.port(), 1234);
};

SEASTAR_THREAD_TEST_CASE(handles_v2) {
    config::node_config cfg_v1;
    auto errors = cfg_v1.load(with_advertised_kafka_api());
    BOOST_TEST(errors.size() == 0);

    config::node_config cfg_v2;
    auto node = YAML::Load(kafka_endpoints_conf_v2);
    errors = cfg_v2.load(node);
    BOOST_TEST(errors.size() == 0);

    BOOST_REQUIRE_EQUAL(cfg_v2.kafka_api().size(), 2);
    BOOST_REQUIRE_EQUAL(cfg_v2.advertised_kafka_api().size(), 2);

    // v1 parses to match first in v2 list
    BOOST_REQUIRE_EQUAL(cfg_v1.kafka_api()[0], cfg_v2.kafka_api()[0]);
    BOOST_REQUIRE_EQUAL(
      cfg_v1.advertised_kafka_api()[0], cfg_v2.advertised_kafka_api()[0]);
    BOOST_REQUIRE(cfg_v1.kafka_api()[0].name.empty());
    BOOST_REQUIRE(cfg_v1.advertised_kafka_api()[0].name.empty());
    BOOST_REQUIRE(cfg_v2.kafka_api()[0].name.empty());
    BOOST_REQUIRE(cfg_v2.advertised_kafka_api()[0].name.empty());

    // v2 parses out more than one item
    BOOST_REQUIRE_EQUAL(cfg_v2.kafka_api()[1].name, "lala");
    BOOST_REQUIRE_EQUAL(cfg_v2.kafka_api()[1].address.host(), "2.2.2.2");
    BOOST_REQUIRE_EQUAL(cfg_v2.kafka_api()[1].address.port(), 8888);
    BOOST_REQUIRE_EQUAL(cfg_v2.advertised_kafka_api()[1].name, "foobar");
    BOOST_REQUIRE_EQUAL(
      cfg_v2.advertised_kafka_api()[1].address.host(), "1.1.1.1");
    BOOST_REQUIRE_EQUAL(cfg_v2.advertised_kafka_api()[1].address.port(), 9999);
}
