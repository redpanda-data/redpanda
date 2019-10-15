#pragma once

#include "cluster/partition.h"
#include "cluster/shard_table.h"
#include "model/metadata.h"
#include "raft/heartbeat_manager.h"
#include "storage/log_manager.h"

#include <unordered_map>

namespace cluster {
class partition_manager {
public:
    partition_manager(
      model::node_id::type nid,
      std::chrono::milliseconds raft_timeout,
      sstring base_dir,
      size_t max_segment_size,
      storage::log_append_config::fsync should_fsync,
      model::timeout_clock::duration disk_timeout,
      sharded<cluster::shard_table>& nlc,
      sharded<raft::client_cache>& clients);

    inline lw_shared_ptr<partition> get(model::ntp ntp) {
        return _ntp_table.find(ntp)->second;
    }
    /// \brief raw api for raft/service.h
    inline raft::consensus& consensus_for(raft::group_id group) {
        return *(_raft_table.find(group)->second->raft());
    }

    future<> stop();
    future<> manage(model::ntp, raft::group_id);

private:
    model::node_id _self;
    storage::log_append_config::fsync _should_fsync;
    model::timeout_clock::duration _disk_timeout;

    storage::log_manager _mngr;
    raft::heartbeat_manager _hbeats;

    sharded<cluster::shard_table>& _shard_table;
    sharded<raft::client_cache>& _clients;

    // XXX use intrusive containers here
    std::unordered_map<model::ntp, lw_shared_ptr<partition>> _ntp_table;
    std::unordered_map<raft::group_id, lw_shared_ptr<partition>> _raft_table;
};
} // namespace cluster
