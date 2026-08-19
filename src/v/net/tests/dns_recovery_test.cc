// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "net/dns.h"
#include "net/tests/dns_test_utils.h"
#include "test_utils/test.h"
#include "utils/unresolved_address.h"

#include <seastar/core/lowres_clock.hh>
#include <seastar/core/sleep.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>

#include <gtest/gtest.h>

#include <chrono>
#include <vector>

using namespace std::chrono_literals;

// Covers the resolve_dns liveness bound: a lookup whose resolver never
// invokes the completion callback (simulated by a DNS server that receives
// queries and never replies, combined with a c-ares per-query timeout far
// above the liveness timeout) must fail with a timeout instead of hanging,
// and must not poison the shard: the per-shard resolver is replaced, so
// both concurrent waiters and subsequent lookups complete normally.
//
// Without the bound, the first lookup would hold the per-shard mutex
// forever and every later resolve_dns call on the shard -- from any
// subsystem -- would wedge behind it (CORE-17096).

namespace {

constexpr auto liveness_timeout = 1s;
// How long after the wedged lookup the concurrent one is issued: long
// enough that the wedged query has reached the (blackholing) server, short
// enough that the waiter's own timeout leaves margin after the holder's.
constexpr auto waiter_stagger = 500ms;
// Keep c-ares' own per-query timeout far above the liveness timeout so the
// blackholed lookup is still pending -- not failed by c-ares itself -- when
// the liveness bound fires.
constexpr auto resolver_query_timeout = 30s;

ss::net::dns_resolver::options make_options(uint16_t port) {
    auto opts = ss::net::dns_resolver::options{};
    opts.servers = std::vector<ss::net::inet_address>{
      ss::net::inet_address("127.0.0.1")};
    opts.udp_port = port;
    opts.timeout = resolver_query_timeout;
    return opts;
}

} // namespace

TEST_CORO(dns_recovery, lost_lookup_fails_and_resolver_is_replaced) {
    auto server = net::dns_test::mock_dns_server{
      net::dns_test::server_mode::blackhole};
    co_await server.start();
    net::configure_dns_resolution_for_testing(
      make_options(server.port()), liveness_timeout);

    // A lookup the server never answers: must fail within the liveness
    // bound rather than hang, and must trigger a resolver replacement.
    auto wedged = net::unresolved_address("wedged.test", 19092);
    // A second lookup issued while the first still holds the resolver
    // mutex: must not be poisoned by the first one's fate.
    auto waiting = net::unresolved_address("waiting.test", 19093);

    auto wedged_fut = net::resolve_dns(wedged);
    co_await ss::sleep(waiter_stagger);
    auto waiting_fut = net::resolve_dns(waiting);
    // Answer further queries so that after the replacement the waiter (and
    // anything else on this shard) resolves normally.
    server.set_mode(net::dns_test::server_mode::answer_a);

    EXPECT_THROW(co_await std::move(wedged_fut), ss::timed_out_error);
    EXPECT_EQ(net::dns_resolver_replacements_for_testing(), 1u);
    EXPECT_GT(server.queries_received(), 0u);

    auto waiting_addr = co_await std::move(waiting_fut);
    EXPECT_EQ(
      waiting_addr,
      ss::socket_address(ss::net::inet_address("127.0.0.1"), 19093));

    // The shard is fully recovered: fresh lookups resolve promptly against
    // the replaced channel and no further replacement happens.
    auto recovered_addr = co_await net::resolve_dns(
      net::unresolved_address("recovered.test", 19094));
    EXPECT_EQ(
      recovered_addr,
      ss::socket_address(ss::net::inet_address("127.0.0.1"), 19094));
    EXPECT_EQ(net::dns_resolver_replacements_for_testing(), 1u);

    co_await server.stop();
}
