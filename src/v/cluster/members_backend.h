#pragma once

#include "cluster/fwd.h"
#include "cluster/members_manager.h"
#include "cluster/members_table.h"
#include "cluster/partition_allocator.h"
#include "cluster/types.h"
#include "model/metadata.h"
#include "raft/consensus.h"

#include <seastar/core/condition-variable.hh>

#include <absl/container/node_hash_set.h>

#include <chrono>
namespace cluster {

class members_backend {
public:
    enum class reallocation_state { initial, reassigned, requested, finished };
    /**
     * struct describing partition reallocation
     */
    struct reallocation_meta {
        reallocation_meta(members_manager::node_update update, model::ntp ntp)
          : update(update)
          , ntp(std::move(ntp)) {}

        members_manager::node_update update;
        model::ntp ntp;
        std::optional<partition_allocator::allocation_units> new_assignment;
        reallocation_state state = reallocation_state::initial;
    };

    members_backend(
      ss::sharded<cluster::topics_frontend>&,
      ss::sharded<cluster::topic_table>&,
      ss::sharded<partition_allocator>&,
      ss::sharded<members_table>&,
      ss::sharded<controller_api>&,
      ss::sharded<members_manager>&,
      consensus_ptr,
      ss::sharded<ss::abort_source>&);

    void start();
    ss::future<> stop();

private:
    void start_reconciliation_loop();
    ss::future<> reconcile();
    ss::future<> reallocate_replica_set(reallocation_meta&);
    void mark_done_as_finished(reallocation_meta&);

    ss::future<> handle_updates();
    void handle_decommissioned(const members_manager::node_update&);
    void handle_recommissioned(const members_manager::node_update&);

    ss::sharded<topics_frontend>& _topics_frontend;
    ss::sharded<topic_table>& _topics;
    ss::sharded<partition_allocator>& _allocator;
    ss::sharded<members_table>& _members;
    ss::sharded<controller_api>& _api;
    ss::sharded<members_manager>& _members_manager;
    consensus_ptr _raft0;
    ss::sharded<ss::abort_source>& _as;
    ss::gate _bg;
    mutex _lock;

    // replicas reallocations in progress
    std::vector<reallocation_meta> _reallocations;
    std::chrono::milliseconds _retry_timeout;
    ss::timer<> _retry_timer;
    ss::condition_variable _new_updates;
};

} // namespace cluster
