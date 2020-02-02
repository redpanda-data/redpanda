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
      ss::sharded<cluster::shard_table>& nlc,
      ss::sharded<rpc::connection_cache>& clients);

    using leader_cb_t
      = ss::noncopyable_function<void(ss::lw_shared_ptr<partition>)>;

    inline ss::lw_shared_ptr<partition> get(const model::ntp& ntp) const {
        return _ntp_table.find(ntp)->second;
    }

    inline bool contains(const model::ntp& ntp) const {
        return _ntp_table.find(ntp) != _ntp_table.end();
    }

    /// \brief raw api for raft/service.h
    inline raft::consensus& consensus_for(raft::group_id group) const {
        return *(_raft_table.find(group)->second->raft());
    }

    ss::future<> start();
    ss::future<> stop();
    ss::future<consensus_ptr> manage(
      model::ntp,
      raft::group_id,
      std::vector<model::broker>,
      std::optional<raft::consensus::append_entries_cb_t>);

    void register_leadership_notification(leader_cb_t cb) {
        _notifications.push_back(std::move(cb));
    }

    std::optional<storage::log> log(const model::ntp& ntp) {
        return _mngr.get(ntp);
    }

private:
    void trigger_leadership_notification(raft::group_id);
    ss::lw_shared_ptr<raft::consensus> make_consensus(
      raft::group_id,
      std::vector<model::broker>,
      storage::log,
      std::optional<raft::consensus::append_entries_cb_t>);
    model::node_id _self;
    storage::log_append_config::fsync _should_fsync;
    model::timeout_clock::duration _disk_timeout;

    storage::log_manager _mngr;
    raft::heartbeat_manager _hbeats;
    /// used to wait for concurrent recoveries
    ss::gate _bg;

    ss::sharded<cluster::shard_table>& _shard_table;
    ss::sharded<rpc::connection_cache>& _clients;

    std::vector<leader_cb_t> _notifications;
    // XXX use intrusive containers here
    std::unordered_map<model::ntp, ss::lw_shared_ptr<partition>> _ntp_table;
    std::unordered_map<raft::group_id, ss::lw_shared_ptr<partition>>
      _raft_table;
};
} // namespace cluster
