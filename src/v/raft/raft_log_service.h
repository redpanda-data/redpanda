#pragma once

#include <smf/macros.h>

#include "filesystem/wal_nstpidx.h"
#include "filesystem/write_ahead_log.h"

#include "raft.smf.fb.h"
#include "raft_cfg.h"
#include "raft_nstpidx_metadata.h"

namespace v {

// https://github.com/xingyif/raft
class raft_log_service final : public raft::raft_api {
 public:
  SMF_DISALLOW_COPY_AND_ASSIGN(raft_log_service);
  explicit raft_log_service(raft_cfg opts,
                            seastar::distributed<write_ahead_log> *data);

  ~raft_log_service() = default;

  /// \brief should be the first method that is called on this object.
  /// it queries the state of the world before starting the log.
  ///
  seastar::future<> initialize();

  const raft_cfg cfg;

 private:
  /// \brief in case of fresh new install (no data). Ensure that seed
  /// servers start at *least* their core topic for creating topics
  seastar::future<> initialize_seed_servers();
  seastar::future<> initialize_cfg_lookup();
  seastar::future<> recover_existing_logs();
  seastar::future<> raft_cfg_log_process_one(std::unique_ptr<wal_read_reply> r);

  // request helpers
  bool
  is_leader(const raft_nstpidx_metadata &n) const {
    return cfg.id == n.leader_id;
  }

 private:
  seastar::distributed<write_ahead_log> *data_;
  std::unordered_map<wal_nstpidx, raft_nstpidx_metadata> omap_;
};

}  // namespace v
