#pragma once

#include <bytell_hash_map.hpp>

#include "filesystem/wal_nstpidx.h"
#include "filesystem/write_ahead_log.h"
#include "raft.smf.fb.h"
#include "raft_nstpidx_stats.h"

namespace v {

class raft_service : public raft::raft_api {
 public:
  explicit raft_service(raft::serverT opts,
                        seastar::distributed<v::write_ahead_log> *w);

  ~raft_service() = default;

 private:
  raft::serverT cfg_;
  seastar::distributed<v::write_ahead_log> *wal_;

  ska::bytell_hash_map<wal_nstpidx, raft_nstpidx_stats> largest_offset_{};
};

}  // namespace v
