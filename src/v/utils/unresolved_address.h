/*
 * Copyright 2020 Vectorized, Inc.
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

#include <seastar/core/sstring.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>

#include <boost/functional/hash.hpp>
#include <fmt/ostream.h>

/// Class representing unresolved network address in form of <host name, port>
/// tuple
class unresolved_address {
public:
    unresolved_address() = default;
    unresolved_address(ss::sstring host, uint16_t port)
      : _host(std::move(host))
      , _port(port) {}

    const ss::sstring& host() const { return _host; }
    uint16_t port() const { return _port; }

    bool operator==(const unresolved_address& other) const = default;

private:
    friend std::ostream& operator<<(std::ostream&, const unresolved_address&);

    ss::sstring _host;
    uint16_t _port{0};
};

inline std::ostream& operator<<(std::ostream& o, const unresolved_address& s) {
    fmt::print(o, "{{host: {}, port: {}}}", s.host(), s.port());
    return o;
}

namespace std {
template<>
struct hash<unresolved_address> {
    size_t operator()(const unresolved_address& a) const {
        size_t h = 0;
        boost::hash_combine(h, hash<ss::sstring>()(a.host()));
        boost::hash_combine(h, hash<uint16_t>()(a.port()));
        return h;
    }
};
} // namespace std
