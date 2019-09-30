#pragma once

#include "cluster/partition.h"
#include "cluster/shard_table.h"
#include "model/metadata.h"

#include <unordered_map>

namespace cluster {
class partition_manager {
public:
    partition_manager(
      model::node_id::type nid,
      sharded<cluster::shard_table>& nlc,
      sharded<raft::client_cache>& clients)
      : _nid(std::move(nid))
      , _shard_table(nlc)
      , _clients(clients) {
    }
    partition_manager(partition_manager&&) noexcept = default;
    lw_shared_ptr<partition> get(model::ntp ntp) {
        return _parts.find(ntp)->second;
    }
    /// \brief raw api for raft/service.h
    raft::consensus& consensus_for(raft::group_id group) {
        return _groups.find(group)->second->raft();
    }
    future<> stop() {
        return make_ready_future<>();
    }
    future<> manage(model::ntp, raft::group_id) {
        return make_ready_future<>();
    }

private:
    model::node_id _nid;
    sharded<cluster::shard_table>& _shard_table;
    sharded<raft::client_cache>& _clients;

    // XXX use intrusive containers here
    std::unordered_map<model::ntp, lw_shared_ptr<partition>> _parts;
    std::unordered_map<raft::group_id, lw_shared_ptr<partition>> _groups;
};
} // namespace cluster
