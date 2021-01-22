// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"

#include <seastar/testing/thread_test_case.hh>

static auto const no_advertised_kafka_api_conf
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

static auto const advertised_kafka_api_conf = "  advertised_kafka_api:\n"
                                              "    address: 10.48.0.2\n"
                                              "    port: 1234\n";

YAML::Node no_advertised_kafka_api() {
    return YAML::Load(no_advertised_kafka_api_conf);
}

YAML::Node with_advertised_kafka_api() {
    ss::sstring conf = no_advertised_kafka_api_conf;
    conf += advertised_kafka_api_conf;
    return YAML::Load(conf);
}

SEASTAR_THREAD_TEST_CASE(shall_return_kafka_api_as_advertised_api_was_not_set) {
    config::configuration cfg;
    cfg.read_yaml(no_advertised_kafka_api());
    auto adv_list = cfg.advertised_kafka_api();
    BOOST_REQUIRE_EQUAL(
      adv_list[0].address.host(), cfg.kafka_api()[0].address.host());
    BOOST_REQUIRE_EQUAL(
      adv_list[0].address.port(), cfg.kafka_api()[0].address.port());
};

SEASTAR_THREAD_TEST_CASE(shall_return_advertised_kafka_api) {
    config::configuration cfg;
    cfg.read_yaml(with_advertised_kafka_api());
    auto adv_list = cfg.advertised_kafka_api();
    BOOST_REQUIRE_EQUAL(adv_list[0].address.host(), "10.48.0.2");
    BOOST_REQUIRE_EQUAL(adv_list[0].address.port(), 1234);
};
