// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "base/seastarx.h"
#include "net/tests/dns_test_utils.h"
#include "test_utils/test.h"

#include <seastar/core/sstring.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/net/dns.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/util/log.hh>

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstddef>

using namespace std::chrono_literals;

// Liveness test for the DNS resolver stack (seastar + vendored c-ares): every
// resolution attempt must complete, successfully or with an error. A lost
// resolver callback leaves callers waiting forever; CORE-17096 observed this
// as an internal RPC transport wedge that required a node restart.
//
// The trigger is production-shaped. Kubernetes pods normally get a resolv.conf
// with search domains such as <namespace>.svc.cluster.local, svc.cluster.local,
// cluster.local, plus options like ndots:5. For names that c-ares treats as
// relative, an NXDOMAIN response for one search-suffixed query makes c-ares
// issue the next query from that query's callback. This test configures the
// search domains directly instead of depending on the host's resolv.conf, and
// uses arbitrary suffix names because only the chained-callback shape matters.
// This caught the QID-reuse lost-callback bug tracked in
// https://github.com/c-ares/c-ares/issues/1256.
//
// Detection is probabilistic against the broken implementation. `qid_draws`
// below is sized for roughly 1 - e^-8 detection probability when the
// 1/65536 QID-reuse bug is present.

namespace {

constexpr auto resolver_query_timeout = 5s;
constexpr auto lookup_timeout = 30s;
constexpr auto resolver_close_timeout = 5s;

// Seven search domains plus the literal name gives eight chained queries per
// lookup. Real Kubernetes pods may have fewer or more search domains depending
// on cluster and node DNS config; this count is test volume, not a semantic
// dependency.
constexpr size_t search_domain_count = 7;
constexpr auto queries_per_lookup = search_domain_count + 1;
constexpr auto chains = 64;
constexpr auto lookups_per_chain = 1024;
constexpr auto qid_draws = chains * lookups_per_chain * queries_per_lookup;

struct chain_result {
    uint64_t completed = 0;
    uint64_t timed_out = 0;
};

// One chain performs `lookups` sequential resolutions. Every name is unique
// and every resolution is expected to complete (here: fail with NXDOMAIN
// after exhausting the search list). A per-lookup timeout far above the
// c-ares per-query timeout distinguishes "completed with an error" (fine)
// from "no completion callback arrived" (the bug).
ss::future<chain_result>
run_chain(ss::net::dns_resolver* resolver, int chain_id, int lookups) {
    auto result = chain_result{};
    for (int i = 0; i < lookups; ++i) {
        auto name = ss::format("q-{}-{}", chain_id, i);
        try {
            co_await ss::with_timeout(
              ss::lowres_clock::now() + lookup_timeout,
              resolver->get_host_by_name(
                name, ss::net::inet_address::family::INET));
            ++result.completed;
        } catch (const ss::timed_out_error&) {
            ++result.timed_out;
            co_return result;
        } catch (...) {
            ++result.completed;
        }
    }
    co_return result;
}

} // namespace

TEST_CORO(dns_liveness, every_resolution_completes) {
    // The per-query trace logging is hundreds of MB at this volume.
    ss::global_logger_registry().set_logger_level(
      "dns_resolver", ss::log_level::error);

    auto server = net::dns_test::mock_dns_server{};
    co_await server.start();

    auto opts = ss::net::dns_resolver::options{};
    opts.servers = std::vector<ss::net::inet_address>{
      ss::net::inet_address("127.0.0.1")};
    opts.udp_port = server.port();
    opts.timeout = resolver_query_timeout;

    const std::array<ss::sstring, search_domain_count> search_domains{
      "s0.test",
      "s1.test",
      "s2.test",
      "s3.test",
      "s4.test",
      "s5.test",
      "s6.test"};
    opts.domains = std::vector<ss::sstring>(
      search_domains.begin(), search_domains.end());

    auto resolver = ss::net::dns_resolver(opts);

    auto futures = std::vector<ss::future<chain_result>>{};
    futures.reserve(chains);
    for (int c = 0; c < chains; ++c) {
        futures.push_back(run_chain(&resolver, c, lookups_per_chain));
    }
    auto results = co_await ss::when_all_succeed(
      futures.begin(), futures.end());

    auto total = chain_result{};
    for (const auto& r : results) {
        total.completed += r.completed;
        total.timed_out += r.timed_out;
    }

    EXPECT_EQ(total.timed_out, 0u)
      << total.timed_out << " DNS resolution(s) timed out (" << total.completed
      << " completed, " << server.queries_received()
      << " DNS queries received, test configured for " << qid_draws
      << " QID draws). One known cause is a lost c-ares callback; see "
         "CORE-17096 and "
         "https://github.com/c-ares/c-ares/issues/1256";

    if (total.timed_out == 0) {
        co_await resolver.close();
    } else {
        // Closing a resolver with an orphaned in-flight query may itself
        // hang; bound it so the test reports the assertion above instead
        // of timing out silently.
        try {
            co_await ss::with_timeout(
              ss::lowres_clock::now() + resolver_close_timeout,
              resolver.close());
        } catch (const ss::timed_out_error&) {
        }
    }
    co_await server.stop();
}
