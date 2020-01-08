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
    unresolved_address(sstring host, uint16_t port)
      : _host(std::move(host))
      , _port(port) {}

    const sstring& host() const { return _host; }
    uint16_t port() const { return _port; }

    future<socket_address> resolve() const {
        return net::inet_address::find(_host).then(
          [port = _port](net::inet_address i_a) {
              return socket_address(i_a, port);
          });
    }

    bool operator==(const unresolved_address& other) const {
        return _host == other._host && _port == other._port;
    }

private:
    seastar::sstring _host;
    uint16_t _port{0};
};

namespace std {
static inline ostream& operator<<(ostream& o, const unresolved_address& s) {
    fmt::print(o, "host: {}, port: {}", s.host(), s.port());
    return o;
}

template<>
struct hash<unresolved_address> {
    size_t operator()(const unresolved_address& a) const {
        size_t h = 0;
        boost::hash_combine(h, hash<sstring>()(a.host()));
        boost::hash_combine(h, hash<uint16_t>()(a.port()));
        return h;
    }
};
} // namespace std
