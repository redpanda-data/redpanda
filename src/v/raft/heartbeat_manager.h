/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include "base/outcome.h"
#include "features/fwd.h"
#include "model/metadata.h"
#include "raft/consensus.h"
#include "raft/consensus_client_protocol.h"
#include "raft/fundamental.h"
#include "raft/group_configuration.h"
#include "utils/mutex.h"

#include <seastar/core/sharded.hh>
#include <seastar/util/log.hh>

#include <absl/container/btree_map.h>
#include <absl/container/flat_hash_set.h>
#include <absl/container/node_hash_map.h>
#include <boost/container/flat_set.hpp>

namespace raft::details {
struct consensus_ptr_by_group_id {
    using is_transparent = std::true_type;

    bool operator()(
      const ss::lw_shared_ptr<consensus>& l,
      const ss::lw_shared_ptr<consensus>& r) const {
        return l->group() < r->group();
    }

    bool operator()(
      const ss::lw_shared_ptr<consensus>& ptr, raft::group_id value) const {
        return ptr->group() < value;
    }

    bool operator()(
      raft::group_id value, const ss::lw_shared_ptr<consensus>& ptr) const {
        return value < ptr->group();
    }
};

} // namespace raft::details

namespace raft {
extern ss::logger hbeatlog;

/**
 * The heartbeat manager addresses the scalability challenge of handling
 * heartbeats for a large number of raft groups by batching many heartbeats into
 * a fewer set of requests than would othewise be required.
 *
 * For example, consider three nodes `node-{a,b,c}`, and two groups, 0 and 1,
 * where L0, L1 are the group leaders and F0 and F1 denote any follower in these
 * raft groups.
 *
 *    node-a    node-b   node-c
 *    ======    ======   ======
 *    L0, L1    F0, F1   F0, F1
 *
 * Conceptually raft requires that each leader periodically send heartbeats to
 * each of its followers. For instance, the following messages are required:
 *
 *    heartbeat(L0) -> F0(node-b)
 *    heartbeat(L0) -> F0(node-c)
 *    heartbeat(L1) -> F1(node-b)
 *    heartbeat(L1) -> F1(node-c)
 *
 * For a fixed heartbeat frequency this poses a scalability challenge as the
 * number of groups under management increases. The heartbeat manager addresses
 * this by batching heartbeats being delivered to a common node. For the above
 * example it is sufficient for `node-a` to deliver a single message to
 * `node-b` and `node-c` that itself contains the original two heartbeats:
 *
 *    heartbeat({L0, L1}) -> {F0, F1}(node-b)
 *    heartbeat({L0, L1}) -> {F0, F1}(node-c)
 */
class heartbeat_manager {
public:
    using consensus_ptr = ss::lw_shared_ptr<consensus>;
    using consensus_set = boost::container::
      flat_set<consensus_ptr, details::consensus_ptr_by_group_id>;

    struct follower_request_meta {
        follower_request_meta(
          consensus_ptr, follower_req_seq, model::offset, vnode);

        follower_request_meta(const follower_request_meta&) = delete;
        follower_request_meta(follower_request_meta&&) noexcept = default;
        follower_request_meta& operator=(const follower_request_meta&) = delete;
        follower_request_meta& operator=(follower_request_meta&&) noexcept
          = delete;

        consensus_ptr c;
        follower_req_seq seq;
        model::offset dirty_offset;
        vnode follower_vnode;
        consensus::inflight_appends_guard append_guard;
    };
    // Heartbeats from all groups for single node
    struct node_heartbeat {
        node_heartbeat(
          model::node_id t,
          heartbeat_request_v2 req,
          absl::node_hash_map<raft::group_id, follower_request_meta> seqs)
          : target(t)
          , request(std::move(req))
          , meta_map(std::move(seqs)) {}

        model::node_id target;
        heartbeat_request_v2 request;
        // each raft group has its own follower metadata hence we need map to
        // track a sequence per group
        absl::node_hash_map<raft::group_id, follower_request_meta> meta_map;
    };

    heartbeat_manager(
      config::binding<std::chrono::milliseconds> heartbeat_interval,
      consensus_client_protocol client_protocol,
      model::node_id self_node_id,
      config::binding<std::chrono::milliseconds> heartbeat_timeout,
      config::binding<bool> enable_lw_heartbeats,
      features::feature_table& features);

    ss::future<> register_group(ss::lw_shared_ptr<consensus>);
    ss::future<> deregister_group(raft::group_id);

    ss::future<> start();
    ss::future<> stop();

    bool is_stopped() const { return _bghbeats.is_closed(); }

private:
    struct heartbeat_requests {
        /// Requests to dispatch.  Can include request to self.
        std::vector<heartbeat_manager::node_heartbeat> requests;

        /// These nodes' heartbeat status indicates they need
        /// a transport reconnection before sending next heartbeat
        absl::flat_hash_set<model::node_id> reconnect_nodes;
    };

    void dispatch_heartbeats();

    clock_type::time_point next_heartbeat_timeout();

    /// \brief unprotected, must be used inside the gate & semaphore

    ss::future<> do_dispatch_heartbeats();
    ss::future<> send_heartbeats(std::vector<node_heartbeat>);

    /// \brief sends a batch to one node
    ss::future<> do_heartbeat(node_heartbeat);

    bool needs_full_heartbeat(
      const follower_index_metadata& follower_metadata,
      const protocol_metadata& leader_protocol_metadata,
      model::offset leader_flushed_offset) const;

    /// \brief notifies the consensus groups about append_entries log offsets
    /// \param n the physical node that owns heart beats
    /// \param groups raft groups managed by \param n
    /// \param result if the node return successful heartbeats
    void process_reply(
      model::node_id n,
      const absl::node_hash_map<raft::group_id, follower_request_meta>& groups,
      result<heartbeat_reply> result);

    void process_reply(
      model::node_id n,
      const absl::node_hash_map<raft::group_id, follower_request_meta>& groups,
      result<heartbeat_reply_v2> result);

    consensus_ptr validate_heartbeat_reply(
      model::node_id reply_source_node,
      model::node_id target_node,
      const absl::node_hash_map<raft::group_id, follower_request_meta>& groups,
      group_id group,
      reply_result status);

    ss::future<heartbeat_requests> requests_for_range();
    // private members

    mutex _lock{"heartbeat_manager"};
    clock_type::time_point _hbeat = clock_type::now();
    config::binding<std::chrono::milliseconds> _heartbeat_interval;
    config::binding<std::chrono::milliseconds> _heartbeat_timeout;
    timer_type _heartbeat_timer;
    /// \brief used to wait for background ops before shutting down
    ss::gate _bghbeats;
    /// insertion/deletion happens very infrequently.
    /// this is optimized for traversal + finding
    consensus_set _consensus_groups;
    consensus_client_protocol _client_protocol;
    model::node_id _self;
    config::binding<bool> _enable_lw_heartbeat;
    features::feature_table& _feature_table;
};
} // namespace raft
