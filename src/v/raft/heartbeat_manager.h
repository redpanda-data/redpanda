#pragma once

#include "raft/client_cache.h"
#include "raft/consensus.h"
#include "raft/timeout_jitter.h"
#include "raft/types.h"

#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>

namespace raft::details {
struct consensus_ptr_by_group_id {
    bool operator()(
      const lw_shared_ptr<consensus>& l,
      const lw_shared_ptr<consensus>& r) const {
        return l->meta().group < r->meta().group;
    }
};

} // namespace raft::details

namespace raft {

class heartbeat_manager {
public:
    using consensus_ptr = lw_shared_ptr<consensus>;
    using consensus_set_t
      = std::set<consensus_ptr, details::consensus_ptr_by_group_id>;

    explicit heartbeat_manager(timeout_jitter, sharded<client_cache>&);

    void register_group(lw_shared_ptr<consensus>);
    void deregister_group(raft::group_id);
    future<> start();
    future<> stop();

private:
    void dispatch_heartbeats();
    /// \brief unprotected, must be used inside the gate & semaphore
    future<> do_dispatch_heartbeats(
      clock_type::time_point last_timeout, clock_type::time_point next_timeout);

    /// \brief sends a batch to one node
    future<> do_heartbeat(heartbeat_request&&);
    /// \brief notifies the consensus groups about append_entries log offsets
    void process_reply(heartbeat_reply&&);

    // private members

    clock_type::time_point _hbeat = clock_type::now();
    timeout_jitter _timeout;
    timer_type _election_timeout;
    /// \brief used to wait for background ops before shutting down
    gate _bghbeats;

    /// \brief this index is used for post-processing requests
    consensus_set_t _sorted_groups;
    /// \brief this index is used for iteration order
    std::vector<consensus_ptr> _iteration_order;
    sharded<client_cache>& _clients;
};
} // namespace raft
