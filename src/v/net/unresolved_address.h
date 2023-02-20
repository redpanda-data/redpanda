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

#include "seastarx.h"
#include "serde/serde.h"

#include <seastar/core/sstring.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>

#include <boost/functional/hash.hpp>
#include <fmt/ostream.h>

namespace net {

/// Class representing unresolved network address in form of <host name, port>
/// tuple
class unresolved_address
  : public serde::envelope<
      unresolved_address,
      serde::version<0>,
      serde::compat_version<0>> {
public:
    using inet_family = std::optional<ss::net::inet_address::family>;

    unresolved_address() = default;
    unresolved_address(
      ss::sstring host,
      uint16_t port,
      inet_family family = ss::net::inet_address::family::INET)
      : _host(std::move(host))
      , _port(port)
      , _family(family) {}

    const ss::sstring& host() const { return _host; }
    uint16_t port() const { return _port; }
    inet_family family() const { return _family; }

    bool operator==(const unresolved_address& other) const = default;
    friend bool operator<(const unresolved_address&, const unresolved_address&)
      = default;

    auto serde_fields() { return std::tie(_host, _port, _family); }

private:
    friend std::ostream&
    operator<<(std::ostream& o, const unresolved_address& s) {
        fmt::print(o, "{{host: {}, port: {}}}", s.host(), s.port());
        return o;
    }

    ss::sstring _host;
    uint16_t _port{0};
    inet_family _family;
};

} // namespace net

namespace std {
template<>
struct hash<net::unresolved_address> {
    size_t operator()(const net::unresolved_address& a) const {
        size_t h = 0;
        boost::hash_combine(h, hash<ss::sstring>()(a.host()));
        boost::hash_combine(h, hash<uint16_t>()(a.port()));
        return h;
    }
};
} // namespace std
