#pragma once

#include "outcome.h"
#include "raft/consensus.h"
#include "raft/types.h"
#include "rpc/connection_cache.h"

#include <seastar/core/semaphore.hh>
#include <seastar/core/sharded.hh>
#include <seastar/util/log.hh>

#include <boost/container/flat_set.hpp>

namespace raft::details {
struct consensus_ptr_by_group_id {
    bool operator()(
      const ss::lw_shared_ptr<consensus>& l,
      const ss::lw_shared_ptr<consensus>& r) const {
        return l->meta().group < r->meta().group;
    }
    bool operator()(
      const ss::lw_shared_ptr<consensus>& ptr, raft::group_id value) const {
        return ptr->meta().group < value;
    }
};

} // namespace raft::details

namespace raft {
extern ss::logger hbeatlog;
class heartbeat_manager {
public:
    using consensus_ptr = ss::lw_shared_ptr<consensus>;
    using consensus_set = boost::container::
      flat_set<consensus_ptr, details::consensus_ptr_by_group_id>;
    struct node_heartbeat {
        node_heartbeat(model::node_id t, heartbeat_request req)
          : target(t)
          , request(std::move(req)) {}

        model::node_id target;
        heartbeat_request request;
    };
    heartbeat_manager(
      duration_type interval, ss::sharded<rpc::connection_cache>&);

    void register_group(ss::lw_shared_ptr<consensus>);
    void deregister_group(raft::group_id);
    duration_type election_duration() const { return _heartbeat_interval * 2; }

    ss::future<> start();
    ss::future<> stop();

private:
    void dispatch_heartbeats();
    /// \brief unprotected, must be used inside the gate & semaphore
    ss::future<> do_dispatch_heartbeats(
      clock_type::time_point last_timeout, clock_type::time_point next_timeout);

    /// \brief sends a batch to one node
    ss::future<> do_heartbeat(node_heartbeat&&, clock_type::time_point);

    /// \brief notifies the consensus groups about append_entries log offsets
    /// \param n the physical node that owns heart beats
    /// \param groups raft groups managed by \param n
    /// \param result if the node return successful heartbeats
    void process_reply(
      model::node_id n,
      std::vector<group_id> groups,
      result<heartbeat_reply> result);

    // private members

    clock_type::time_point _hbeat = clock_type::now();
    duration_type _heartbeat_interval;
    timer_type _heartbeat_timer;
    /// \brief used to wait for background ops before shutting down
    ss::gate _bghbeats;
    /// insertion/deletion happens very infrequently.
    /// this is optimized for traversal + finding
    consensus_set _consensus_groups;
    ss::sharded<rpc::connection_cache>& _clients;
};
} // namespace raft
