// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <yaml-cpp/yaml.h>

auto ipv4_address_str = "test_addr:\n"
                        "  address: 192.168.0.1\n"
                        "  port: 6547\n";

auto ipv6_address_str = "test_addr:\n"
                        "  address: 2001:0db8:85a3:0000:0000:8a2e:0370:7334\n"
                        "  port: 7777\n";

auto all_interfaces_address_str = "test_addr:\n"
                                  "  address: 0.0.0.0\n"
                                  "  port: 3214\n";

auto hostname_address_str = "test_addr:\n"
                            "  address: localhost\n"
                            "  port: 1234\n";

ss::socket_address ip4_addr = ss::socket_address(
  ss::ipv4_addr("192.168.0.1", 6547));

ss::socket_address read_socket_from_yaml(ss::sstring yaml_string) {
    auto node = YAML::Load(yaml_string);
    return node["test_addr"].as<ss::socket_address>();
}

SEASTAR_THREAD_TEST_CASE(write_as_yaml) {}

SEASTAR_THREAD_TEST_CASE(test_decode_ipv4) {
    auto ip4 = read_socket_from_yaml(ipv4_address_str);

    BOOST_TEST(ip4.port() == 6547);
    BOOST_TEST(ip4.addr().is_ipv4());
    BOOST_TEST(ip4.addr().as_ipv4_address().ip == 0xC0A80001);

    auto ip6 = read_socket_from_yaml(ipv6_address_str);

    BOOST_TEST(ip6.port() == 7777);
    BOOST_TEST(ip6.addr().is_ipv6());
    auto expected_ip6 = std::array<uint8_t, 16>{
      0x20,
      0x01,
      0x0d,
      0xb8,
      0x85,
      0xa3,
      0x00,
      0x00,
      0x00,
      0x00,
      0x8a,
      0x2e,
      0x03,
      0x70,
      0x73,
      0x34};
    BOOST_TEST(ip6.addr().as_ipv6_address().ip == expected_ip6);

    auto all_interfaces = read_socket_from_yaml(all_interfaces_address_str);

    BOOST_TEST(all_interfaces.port() == 3214);
    BOOST_TEST(all_interfaces.addr().is_ipv4());
    BOOST_TEST(all_interfaces.addr().as_ipv4_address().ip == 0x00000000);

    auto host_name = read_socket_from_yaml(hostname_address_str);

    BOOST_TEST(host_name.port() == 1234);
    BOOST_TEST(host_name.addr().is_ipv4());
    // 0x7F000001 == 127.0.0.1
    BOOST_TEST(host_name.addr().as_ipv4_address().ip == 0x7F000001);
}
