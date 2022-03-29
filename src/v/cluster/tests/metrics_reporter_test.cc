/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include <boost/test/tools/old/interface.hpp>

#include <cstdint>
#include <stdexcept>
#include <vector>
#define BOOST_TEST_MODULE leader_balancer

#include "cluster/metrics_reporter.h"

#include <boost/test/unit_test.hpp>

cluster::details::address make_address(
  ss::sstring proto, ss::sstring host, uint16_t port, ss::sstring path) {
    return cluster::details::address{
      .protocol = std::move(proto),
      .host = std::move(host),
      .port = port,
      .path = std::move(path)};
}

BOOST_AUTO_TEST_CASE(test_parsing_url) {
    std::vector<std::pair<ss::sstring, cluster::details::address>> params;

    params.emplace_back(
      "http://localhost", make_address("http", "localhost", 80, ""));
    params.emplace_back(
      "https://localhost", make_address("https", "localhost", 443, ""));
    params.emplace_back(
      "https://192.168.1.1:1234",
      make_address("https", "192.168.1.1", 1234, ""));
    params.emplace_back(
      "https://192.168.1.1:1234/some/path/to/resource",
      make_address("https", "192.168.1.1", 1234, "/some/path/to/resource"));
    params.emplace_back(
      "https://192.168.1.1/some/path/to/resource",
      make_address("https", "192.168.1.1", 443, "/some/path/to/resource"));

    for (auto [url, expected] : params) {
        auto address = cluster::details::parse_url(url);

        BOOST_REQUIRE_EQUAL(expected.protocol, address.protocol);
        BOOST_REQUIRE_EQUAL(expected.host, address.host);
        BOOST_REQUIRE_EQUAL(expected.port, address.port);
        BOOST_REQUIRE_EQUAL(expected.path, address.path);
    }
}

BOOST_AUTO_TEST_CASE(test_invalid_url) {
    BOOST_REQUIRE_THROW(
      cluster::details::parse_url("tcp://localhost"), std::invalid_argument);
    BOOST_REQUIRE_THROW(
      cluster::details::parse_url("tcp:/"), std::invalid_argument);
    BOOST_REQUIRE_THROW(
      cluster::details::parse_url("vectorized.io"), std::invalid_argument);
}
