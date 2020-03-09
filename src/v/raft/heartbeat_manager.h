#pragma once

#include "outcome.h"
#include "raft/consensus.h"
#include "raft/consensus_client_protocol.h"
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
    heartbeat_manager(duration_type interval, consensus_client_protocol);

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
    consensus_client_protocol _client_protocol;

    /// In redpanda heartbeats are send independently to append entries requests
    /// this can lead to sending heartbeat more than once per given heartbeat
    /// interval as append entries requests reset the heartbeat timer.
    /// In order to skip some heartbeats that would be send in close temporal
    /// proximity to previous append entries request introduced the threshold
    /// used to decide when heartbeat manager can skip sending heartbeat to
    /// particular group. The heartbeat manager will only skip sending heartbeat
    /// to the group if append entries occurred not earlier than last heartbeat
    /// + threashold. This will guarantee that election timer on on the
    /// followers will be reset and that the maximum effective interval between
    /// heartbeat would be 
    ///
    ///    (_heartbeat_interval - _skip_hbeat_threshold) + _heartbeat_interval
    /// or:
    ///     (2 - hbeat_threshold_ratio)*_heartbeat_interval;
    ///
    static constexpr auto hbeat_threshold_ratio = 0.8;
    uint64_t _skip_hbeat_threshold;
};
} // namespace raft
