#pragma once

#include <cstring>

namespace v {
struct seed_server {
  int64_t id;
  seastar::ipv4_addr addr;
};
struct raft_cfg {
  raft_cfg(int64_t server_id, int16_t min, int16_t max)
    : id(server_id), min_version(min), max_version(max) {}
  ~raft_cfg() = default;
  int64_t id;
  int16_t min_version;
  int16_t max_version;
  /// \brief must be odd number
  int32_t seed_server_topic_partitions = 7;
  std::vector<seed_server> seeds;
};

}  // namespace v
