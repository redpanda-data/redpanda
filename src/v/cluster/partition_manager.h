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
      storage::log_append_config::fsync should_fsync,
      model::timeout_clock::duration disk_timeout,
      sharded<cluster::shard_table>& nlc,
      sharded<rpc::connection_cache>& clients);

    using leader_cb_t = noncopyable_function<void(lw_shared_ptr<partition>)>;

    inline lw_shared_ptr<partition> get(const model::ntp& ntp) const {
        return _ntp_table.find(ntp)->second;
    }

    inline bool contains(const model::ntp& ntp) const {
        return _ntp_table.find(ntp) != _ntp_table.end();
    }

    /// \brief raw api for raft/service.h
    inline raft::consensus& consensus_for(raft::group_id group) const {
        return *(_raft_table.find(group)->second->raft());
    }

    future<> start();
    future<> stop();
    future<consensus_ptr>
      manage(model::ntp, raft::group_id, std::vector<model::broker>);

    void register_leadership_notification(leader_cb_t cb) {
        _notifications.push_back(std::move(cb));
    }

    storage::log_manager::logs_type& logs() { return _mngr.logs(); }

private:
    void trigger_leadership_notification(raft::group_id);

    model::node_id _self;
    storage::log_append_config::fsync _should_fsync;
    model::timeout_clock::duration _disk_timeout;

    storage::log_manager _mngr;
    raft::heartbeat_manager _hbeats;
    /// used to wait for concurrent recoveries
    gate _bg;

    sharded<cluster::shard_table>& _shard_table;
    sharded<rpc::connection_cache>& _clients;

    std::vector<leader_cb_t> _notifications;
    // XXX use intrusive containers here
    std::unordered_map<model::ntp, lw_shared_ptr<partition>> _ntp_table;
    std::unordered_map<raft::group_id, lw_shared_ptr<partition>> _raft_table;
};
} // namespace cluster
