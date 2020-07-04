#pragma once

#include "cluster/members_table.h"
#include "cluster/partition_allocator.h"
#include "cluster/types.h"
#include "config/seed_server.h"
#include "raft/consensus.h"
#include "raft/types.h"
#include "rpc/connection_cache.h"

namespace cluster {

// Members manager class is responsible for updating information about cluster
// members, joining the cluster and creating inter cluster connections.
// This class receives updates from controller STM. It reacts only on raft
// configuration batch types. All the updates are propagated to core local
// cluster::members instances. There is only one instance of members manager
// running on core-0
class members_manager {
public:
    static constexpr ss::shard_id shard = 0;
    members_manager(
      consensus_ptr,
      ss::sharded<members_table>&,
      ss::sharded<rpc::connection_cache>&,
      ss::sharded<partition_allocator>&,
      ss::sharded<ss::abort_source>&);

    ss::future<> start();
    ss::future<> stop();

    ss::future<result<join_reply>> handle_join_request(model::broker);
    ss::future<std::error_code> apply_update(model::record_batch);

    bool is_batch_applicable(const model::record_batch& b) {
        return b.header().type == raft::configuration_batch_type;
    }

private:
    using seed_iterator = std::vector<config::seed_server>::const_iterator;
    // Cluster join
    void join_raft0();
    bool is_already_member() const {
        return _raft0->config().contains_broker(_self.id());
    }

    ss::future<result<join_reply>>
    dispatch_join_to_seed_server(seed_iterator it);
    ss::future<result<join_reply>>
    dispatch_join_to_remote(const config::seed_server&, model::broker);

    ss::future<join_reply> dispatch_join_request();
    template<typename Func>
    auto dispatch_rpc_to_leader(Func&& f);

    // Raft 0 config updates
    ss::future<> handle_raft0_cfg_update(raft::group_configuration);
    ss::future<> update_connections(patch<broker_ptr>);

    std::vector<config::seed_server> _seed_servers;
    model::broker _self;
    std::chrono::milliseconds _join_timeout;
    consensus_ptr _raft0;
    ss::sharded<members_table>& _members_table;
    ss::sharded<rpc::connection_cache>& _connection_cache;
    ss::sharded<partition_allocator>& _allocator;
    ss::sharded<ss::abort_source>& _as;
    ss::gate _gate;
};
} // namespace cluster