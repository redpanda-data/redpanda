#pragma once

#include "seastarx.h"

#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>

#include <cstdint>

namespace config {
struct seed_server {
    int64_t id;
    socket_address addr;
};
} // namespace config

namespace std {
static inline ostream& operator<<(ostream& o, const config::seed_server& s) {
    return o << "raft_seed_server{id=" << s.id << ", addr=" << s.addr.addr()
             << ":" << s.addr.port() << " }";
}
} // namespace std
