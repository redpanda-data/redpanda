#pragma once

#include <cstdint>

#include <smf/human_bytes.h>

namespace rp {
namespace api {
struct client_stats {
  uint64_t bytes_sent{0};
  uint64_t bytes_read{0};
  uint64_t read_rpc{0};
  uint64_t write_rpc{0};

  client_stats &
  operator+=(const client_stats &o) {
    bytes_sent += o.bytes_sent;
    bytes_read += o.bytes_read;
    read_rpc += o.read_rpc;
    write_rpc += o.write_rpc;
    return *this;
  }
};
}  // namespace api
}  // namespace rp

namespace std {
inline ostream &
operator<<(ostream &o, const rp::api::client_stats &s) {
  o << "rp::api::client_stats{bytes_sent:" << smf::human_bytes(s.bytes_sent)
    << " (" << s.bytes_sent
    << "), bytes_read:" << smf::human_bytes(s.bytes_read) << " ("
    << s.bytes_read << "), read_rpc:" << s.read_rpc
    << ", write_rpc:" << s.write_rpc << "}";
  return o;
}
}  // namespace std
