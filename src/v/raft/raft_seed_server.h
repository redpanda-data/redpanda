#pragma once

#include <cstdint>

#include <seastar/net/ip.hh>

namespace v {
struct raft_seed_server {
  int64_t id;
  seastar::ipv4_addr addr;
};

}  // namespace v
namespace std {
static inline ostream &
operator<<(ostream &o, const ::v::raft_seed_server &s) {
  return o << "v::raft_seed_server{id=" << s.id << ", addr=" << s.addr << " }";
}
}  // namespace std
