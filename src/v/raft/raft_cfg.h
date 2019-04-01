#pragma once

#include <cstring>

#include "raft_seed_server.h"

namespace v {
struct raft_cfg {
  int64_t id;
  int16_t min_version;
  int16_t max_version;
  /// \brief must be odd number
  int32_t seed_server_meta_topic_partitions = 7;
  std::vector<raft_seed_server> seeds;
};
}  // namespace v

namespace std {
static inline ostream &
operator<<(ostream &o, const ::v::raft_cfg &raft) {
  o << "v::raft_cfg{id=" << raft.id << ", min_version=" << raft.min_version
    << ", max_version=" << raft.max_version << ", seed_server_topic_partitions="
    << raft.seed_server_meta_topic_partitions << ", seed_servers: ";
  for (const auto &s : raft.seeds) {
    o << s;
  }
  return o << " }";
}

}  // namespace std
