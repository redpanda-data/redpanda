// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/node_config.h"
#include "config/seed_server.h"
#include "utils/unresolved_address.h"

#include <seastar/testing/thread_test_case.hh>

static const auto old_seed_server_format = R"(
redpanda:
  data_directory: /var/lib/redpanda/data
  node_id: 1
  rpc_server:
    address: 127.0.0.1
    port: 33145
  kafka_api:
    address: 192.168.1.1
    port: 9999
  seed_servers:
    - host: 
        address: 1.2.3.4
        port: 1234
      node_id: 1
    - host: 
        address: 4.3.2.1
        port: 4321
      node_id: 2
  admin:
    address: 127.0.0.1
    port: 9644)";

static const auto old_seed_server_format_no_id = R"(
redpanda:
  data_directory: /var/lib/redpanda/data
  node_id: 1
  rpc_server:
    address: 127.0.0.1
    port: 33145
  kafka_api:
    address: 192.168.1.1
    port: 9999
  seed_servers:
     - host: 
        address: 1.2.3.4
        port: 1234
     - host: 
        address: 4.3.2.1
        port: 4321
  admin:
    address: 127.0.0.1
    port: 9644)";

static const auto new_seed_server_format = R"(
redpanda:
  data_directory: /var/lib/redpanda/data"
  node_id: 1
  rpc_server:
    address: 127.0.0.1
    port: 33145
  kafka_api:
    address: 192.168.1.1
    port: 9999
  seed_servers:
     - address: 1.2.3.4
       port: 1234
     - address: 4.3.2.1
       port: 4321
  admin:
    address: 127.0.0.1
    port: 9644)";

YAML::Node read_old_seed_server_format() {
    return YAML::Load(old_seed_server_format);
}

YAML::Node read_old_seed_server_format_no_id() {
    return YAML::Load(old_seed_server_format_no_id);
}

YAML::Node read_new_seed_server_format() {
    return YAML::Load(new_seed_server_format);
}

SEASTAR_THREAD_TEST_CASE(test_seed_servers_yaml_parsing) {
    config::node_config old_seed;
    config::node_config old_seed_no_id;
    config::node_config new_seed;

    old_seed.load(read_old_seed_server_format());
    old_seed_no_id.load(read_old_seed_server_format_no_id());
    new_seed.load(read_new_seed_server_format());

    BOOST_REQUIRE_EQUAL(
      new_seed.seed_servers()[0],
      config::seed_server{net::unresolved_address("1.2.3.4", 1234)});
    BOOST_REQUIRE_EQUAL(
      new_seed.seed_servers()[1],
      config::seed_server{net::unresolved_address("4.3.2.1", 4321)});
    BOOST_REQUIRE_EQUAL(old_seed.seed_servers(), new_seed.seed_servers());
    BOOST_REQUIRE_EQUAL(old_seed_no_id.seed_servers(), new_seed.seed_servers());
};
