#pragma once

#include <bytell_hash_map.hpp>
#include <smf/macros.h>

#include "filesystem/wal_nstpidx.h"
#include "filesystem/wal_requests.h"
#include "raft_generated.h"

struct raft_follower_index_metadata {
  int64_t next_index = -1;
  int64_t match_index = -1;
};
class raft_nstpidx_metadata {
 public:
  SMF_DISALLOW_COPY_AND_ASSIGN(raft_nstpidx_metadata);
  explicit raft_nstpidx_metadata(wal_nstpidx i, int64_t n, int64_t t, int32_t p,
                                 int64_t term_id, int64_t start, int64_t end)
    : idx(i), ns(n), topic(t), partition(p), term(term_id), start_offset(start),
      end_offset(end) {}
  raft_nstpidx_metadata(raft_nstpidx_metadata &&o) noexcept
    : idx(o.idx), ns(o.ns), topic(o.topic), partition(o.partition),
      term(o.term), start_offset(o.start_offset), end_offset(o.end_offset),
      leader_id(o.leader_id), voted_for(o.voted_for),
      config(std::move(o.config)) {}

  const wal_nstpidx idx;
  const int64_t ns;
  const int64_t topic;
  const int32_t partition;

  int64_t term;
  int64_t start_offset;
  int64_t end_offset;

  // when leader_id == server.id it means we are the leader
  int64_t leader_id = -1;
  int64_t voted_for = -1;

  raft::configuration_requestT config;

  void
  initialize_leader() {
    initialize_leader_look_aside_table();
  }

 private:
  void
  initialize_leader_look_aside_table() {
    leader_look_aside_table_.clear();
    leader_look_aside_table_.reserve(config.nodes.size() +
                                     config.learners.size());
  }

 private:
  // must be re-initialized, cleared every time we become leader
  // key is the server id in the config
  ska::bytell_hash_map<int64_t, raft_nstpidx_metadata> leader_look_aside_table_;
};
