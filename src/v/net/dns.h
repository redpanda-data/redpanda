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
#pragma once
#include "utils/unresolved_address.h"

#include <seastar/net/dns.hh>

#include <chrono>
#include <cstddef>
#include <vector>

namespace net {

/**
 * Resolves addresses using seastar DNS resolver. It uses mutex to workaround
 * seastar bug causing segmentation fault when udp channel is being accessed by
 * different fibers.
 *
 * Lookups are additionally bounded by a liveness timeout: a lookup that
 * neither completes nor fails within the bound (a healthy c-ares channel
 * always does, its own per-query timeouts permitting) indicates a resolver
 * whose completion callback was lost (see CORE-17096). Such a lookup fails
 * with `ss::timed_out_error` and the shard's resolver is replaced with a
 * fresh one, so a single lost callback cannot permanently wedge every
 * subsequent lookup on the shard behind the serializing mutex.
 *
 * Returns the first address from resolve_dns_all.
 */
ss::future<ss::socket_address> resolve_dns(const unresolved_address&);

/**
 * Resolves a host name to all of its addresses, each combined with the
 * port of the input address. Never returns an empty list; resolution
 * failures and empty results surface as exceptional futures.
 *
 * The order of the returned addresses carries no preference: for
 * dual-stack names the resolver reports IPv4/IPv6 entries in DNS
 * response arrival order (RFC 6724 sorting is not in effect).
 */
ss::future<std::vector<ss::socket_address>>
resolve_dns_all(const unresolved_address&);

/**
 * Test-only: replaces the calling shard's resolver with one built from
 * `options` (e.g. pointing at an in-test DNS server) and overrides the
 * liveness timeout after which a non-completing lookup causes the shard's
 * resolver to be replaced. Also resets the replacement counter.
 */
void configure_dns_resolution_for_testing(
  ss::net::dns_resolver::options options,
  std::chrono::milliseconds liveness_timeout);

/**
 * Test-only: number of times the calling shard's resolver was replaced
 * after a lookup failed to complete within the liveness timeout.
 */
size_t dns_resolver_replacements_for_testing();

} // namespace net
