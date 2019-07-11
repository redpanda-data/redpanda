#pragma once

#include "seastarx.h"

#include <seastar/net/ip.hh>

#include <cstdint>

struct raft_seed_server {
    int64_t id;
    ipv4_addr addr;
};

namespace std {
static inline ostream& operator<<(ostream& o, const raft_seed_server& s) {
    return o << "raft_seed_server{id=" << s.id << ", addr=" << s.addr << " }";
}
} // namespace std
