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

#include "utils/mutex.h"
#include "utils/unresolved_address.h"

#include <seastar/core/coroutine.hh>
#include <seastar/net/dns.hh>

namespace net {

ss::future<ss::socket_address> resolve_dns(unresolved_address address) {
    static thread_local ss::net::dns_resolver resolver;
    static thread_local mutex m{"resolve_dns"};
    // lock
    auto units = co_await m.get_units();
    // resolve
    auto i_a = co_await resolver.resolve_name(address.host(), address.family());

    co_return ss::socket_address(i_a, address.port());
};

} // namespace net
