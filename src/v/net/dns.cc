/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "net/dns.h"

#include "base/vlog.h"
#include "ssx/future-util.h"
#include "ssx/mutex.h"
#include "utils/unresolved_address.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/with_timeout.hh>
#include <seastar/net/dns.hh>
#include <seastar/util/log.hh>

#include <chrono>
#include <ranges>
#include <utility>

namespace net {

namespace {

ss::logger dnslog{"dns"};

using namespace std::chrono_literals;

// After this long without completion a lookup is considered lost, not slow:
// c-ares bounds every query with its own per-query timeout and retry budget
// (including each search-domain candidate), so a healthy channel always
// completes -- with an answer or an error -- well within this bound.
constexpr std::chrono::milliseconds default_resolver_liveness_timeout = 60s;

struct resolver_state {
    ss::net::dns_resolver::options options{};
    std::chrono::milliseconds liveness_timeout{
      default_resolver_liveness_timeout};
    ss::net::dns_resolver resolver{options};
    ssx::mutex lock{"resolve_dns"};
    size_t replacements{0};
};

resolver_state& state() {
    static thread_local resolver_state st{};
    return st;
}

// Replaces the shard's resolver with a fresh channel. The old channel is
// closed in the background; if the close itself never completes (it may
// depend on the same lost callback that condemned the channel), it is
// deliberately leaked -- bounded to one channel per replacement.
void replace_resolver(resolver_state& st) {
    auto old = std::exchange(st.resolver, ss::net::dns_resolver(st.options));
    ssx::background = old.close()
                        .handle_exception([](const std::exception_ptr&) {})
                        .finally([old = std::move(old)] {});
}

ss::future<ss::net::hostent> lookup_host(const unresolved_address& address) {
    auto& st = state();
    const auto timeout = st.liveness_timeout;
    // A holder releases the lock within one liveness timeout, so waiting
    // longer than that means starvation, not a slow lookup.
    auto units = co_await st.lock.get_units(
      ssx::mutex::time_point::clock::now() + timeout);
    ss::net::hostent host;
    try {
        host = co_await ss::with_timeout(
          ss::lowres_clock::now() + timeout,
          st.resolver.get_host_by_name(address.host(), address.family()));
    } catch (const ss::timed_out_error&) {
        // The channel lost this lookup's completion callback (e.g.
        // CORE-17096) and will never make progress again; replace it so the
        // shard's DNS recovers instead of wedging every subsequent lookup
        // behind this one.
        ++st.replacements;
        vlog(
          dnslog.error,
          "DNS lookup of {} did not complete within {}; replacing the "
          "shard's resolver (replacement #{})",
          address.host(),
          timeout,
          st.replacements);
        replace_resolver(st);
        throw;
    }
    if (host.addr_entries.empty()) {
        throw std::runtime_error(
          fmt::format(
            "dns resolution of {} returned no addresses", address.host()));
    }
    co_return host;
}

} // namespace

ss::future<ss::socket_address> resolve_dns(const unresolved_address& address) {
    auto host = co_await lookup_host(address);
    co_return ss::socket_address(
      host.addr_entries.front().addr, address.port());
}

ss::future<std::vector<ss::socket_address>>
resolve_dns_all(const unresolved_address& address) {
    auto host = co_await lookup_host(address);

    co_return host.addr_entries
      | std::views::transform([port = address.port()](const auto& entry) {
            return ss::socket_address(entry.addr, port);
        })
      | std::ranges::to<std::vector>();
}

void configure_dns_resolution_for_testing(
  ss::net::dns_resolver::options options,
  std::chrono::milliseconds liveness_timeout) {
    auto& st = state();
    st.options = std::move(options);
    st.liveness_timeout = liveness_timeout;
    st.replacements = 0;
    replace_resolver(st);
}

size_t dns_resolver_replacements_for_testing() { return state().replacements; }

} // namespace net
