#pragma once

#include "filesystem/wal_nstpidx.h"
#include "filesystem/write_ahead_log.h"
#include "seastarx.h"

#include <smf/macros.h>

// raft
#include "raft.smf.fb.h"
#include "raft_cfg.h"
#include "raft_client_cache.h"
#include "raft_nstpidx_metadata.h"

// https://github.com/xingyif/raft
class raft_log_service final : public raft::raft_api {
public:
    SMF_DISALLOW_COPY_AND_ASSIGN(raft_log_service);
    explicit raft_log_service(
      raft_cfg opts, distributed<write_ahead_log>* data);

    ~raft_log_service() = default;

    /// \brief should be the first method that is called on this object.
    /// it queries the state of the world before starting the log.
    future<> initialize();

    /// \brief stop raft_log_service
    future<> stop();

    const raft_cfg cfg;

private:
    /// \brief in case of fresh new install (no data). Ensure that seed
    /// servers start at *least* their core topic for creating topics
    future<> initialize_seed_servers();
    future<> initialize_cfg_lookup();
    future<> recover_existing_logs();
    future<>
    raft_cfg_log_process_one(std::unique_ptr<wal_read_reply> r);

    // request helpers
    bool is_leader(const raft_nstpidx_metadata& n) const {
        return cfg.id == n.leader_id;
    }

private:
    distributed<write_ahead_log>* _data;
    std::unordered_map<wal_nstpidx, raft_nstpidx_metadata> _omap;
    raft_client_cache _cache;
};
