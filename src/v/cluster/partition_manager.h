#pragma once

#include "cluster/partition.h"
#include "model/metadata.h"
#include "raft/group_shard_table.h"

#include <unordered_map>

namespace cluster {
class partition_manager {
public:
    using m_ntp_t = model::namespaced_topic_partition;
    partition_manager(
      model::node_id::type nid,
      sharded<raft::group_shard_table>& nlc,
      sharded<raft::client_cache>& clients)
      : _nid(std::move(nid))
      , _shard_table(nlc)
      , _clients(clients) {
    }
    partition_manager(partition_manager&&) noexcept = default;
    lw_shared_ptr<partition> get(model::namespaced_topic_partition ntp) {
        return _parts.find(ntp)->second;
    }
    raft::consensus& consensus_for(raft::group_id group) {
        return _groups.find(group)->second->raft_group();
    }
    future<> stop() {
        return make_ready_future<>();
    }
    future<> recover() {
        return make_ready_future<>();
    }

private:
    model::node_id _nid;
    sharded<raft::group_shard_table>& _shard_table;
    sharded<raft::client_cache>& _clients;

    // XXX use intrusive containers here
    std::unordered_map<m_ntp_t, lw_shared_ptr<partition>> _parts;
    std::unordered_map<raft::group_id, lw_shared_ptr<partition>> _groups;
};
} // namespace cluster
