// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "net/transport.h"

#include <gtest/gtest.h>

TEST(format_connect_authority, ipv4) {
    EXPECT_EQ(
      net::detail::format_connect_authority("127.0.0.1", 8080),
      "127.0.0.1:8080");
}

TEST(format_connect_authority, dns_hostname) {
    EXPECT_EQ(
      net::detail::format_connect_authority("example.com", 443),
      "example.com:443");
}

TEST(format_connect_authority, ipv6_literal_gets_bracketed) {
    EXPECT_EQ(net::detail::format_connect_authority("::1", 8443), "[::1]:8443");
    EXPECT_EQ(
      net::detail::format_connect_authority("2001:db8::1", 443),
      "[2001:db8::1]:443");
}

TEST(format_connect_authority, already_bracketed_ipv6_left_alone) {
    // Defensive: if an upstream parser hands us a pre-bracketed host,
    // we must not double-bracket it.
    EXPECT_EQ(
      net::detail::format_connect_authority("[::1]", 8443), "[::1]:8443");
}
