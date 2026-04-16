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
#include "model/metadata.h"
#include "raft/heartbeats.h"
#include "raft/transfer_leadership.h"
#include "raft/types.h"
#include "rpc/types.h"

#include <seastar/core/shared_ptr.hh>

namespace raft {

/// Virtualized Raft client protocol. The protocol allows to communicate
/// with other cluster members.
class consensus_client_protocol final {
public:
    struct impl {
        virtual ss::future<bool> ensure_disconnect(model::node_id) = 0;

        virtual ss::future<> reset_backoff(model::node_id) = 0;

        virtual ss::future<result<vote_reply>>
          vote(model::node_id, vote_request, rpc::client_opts) = 0;

        virtual ss::future<result<append_entries_reply>> append_entries(
          model::node_id, append_entries_request, rpc::client_opts) = 0;

        virtual ss::future<result<heartbeat_reply_v2>> heartbeat_v2(
          model::node_id, heartbeat_request_v2, rpc::client_opts) = 0;

        virtual ss::future<result<install_snapshot_reply>> install_snapshot(
          model::node_id, install_snapshot_request, rpc::client_opts) = 0;

        virtual ss::future<result<timeout_now_reply>> timeout_now(
          model::node_id, timeout_now_request, rpc::client_opts) = 0;

        virtual ss::future<result<get_compaction_mcco_reply>>
          get_compaction_mcco(
            model::node_id, get_compaction_mcco_request, rpc::client_opts) = 0;

        virtual ss::future<result<distribute_compaction_mtro_reply>>
          distribute_compaction_mtro(
            model::node_id,
            distribute_compaction_mtro_request,
            rpc::client_opts) = 0;

        virtual ~impl() noexcept = default;
    };

public:
    explicit consensus_client_protocol(ss::shared_ptr<impl> i)
      : _impl(std::move(i)) {}

    /// =======================================================================
    /// Functions that tweak transport:

    /// Attempt to disconnect the transport. Used when a
    /// broken channel is suspected.
    ss::future<bool> ensure_disconnect(model::node_id target_node) {
        return _impl->ensure_disconnect(target_node);
    }

    /// Reset the client backoff state for the target node. Used when the target
    /// node was known to be irresponsive but now came back, so it would make
    /// sense to attempt reconnecting to it with minimal delay.
    ss::future<> reset_backoff(model::node_id target_node) {
        return _impl->reset_backoff(target_node);
    }

    /// =======================================================================
    /// Functions that send messages to nodes:

    /// Candidate requests a vote from a voter node (RequestVote in Raft paper)
    ss::future<result<vote_reply>>
    vote(model::node_id target_node, vote_request r, rpc::client_opts opts) {
        return _impl->vote(target_node, std::move(r), std::move(opts));
    }

    /// Leader sends log entries to a follower (AppendEntries in Raft paper)
    ss::future<result<append_entries_reply>> append_entries(
      model::node_id target_node,
      append_entries_request r,
      rpc::client_opts opts) {
        return _impl->append_entries(
          target_node, std::move(r), std::move(opts));
    }

    /// Leader checks if a follower is alive
    ss::future<result<heartbeat_reply_v2>> heartbeat_v2(
      model::node_id target_node,
      heartbeat_request_v2 r,
      rpc::client_opts opts) {
        return _impl->heartbeat_v2(target_node, std::move(r), std::move(opts));
    }

    /// Leader sends Raft snapshot chunks to a follower
    /// (InstallSnapshot in Raft paper)
    ss::future<result<install_snapshot_reply>> install_snapshot(
      model::node_id target_node,
      install_snapshot_request r,
      rpc::client_opts opts) {
        return _impl->install_snapshot(
          target_node, std::move(r), std::move(opts));
    }

    /// Leader asks another member to immediately start an election
    ss::future<result<timeout_now_reply>> timeout_now(
      model::node_id target_node,
      timeout_now_request r,
      rpc::client_opts opts) {
        return _impl->timeout_now(target_node, std::move(r), std::move(opts));
    }

    /// For coordinating compaction leader requests its max cleanly compacted
    /// offset and max transaction-free offset from a follower
    ss::future<result<get_compaction_mcco_reply>> get_compaction_mcco(
      model::node_id target_node,
      get_compaction_mcco_request r,
      rpc::client_opts opts) {
        return _impl->get_compaction_mcco(
          target_node, std::move(r), std::move(opts));
    }

    /// For coordinating compaction leader lets follower know the calculated max
    /// tombstone removal offset and max transaction removal offset
    ss::future<result<distribute_compaction_mtro_reply>>
    distribute_compaction_mtro(
      model::node_id target_node,
      distribute_compaction_mtro_request r,
      rpc::client_opts opts) {
        return _impl->distribute_compaction_mtro(
          target_node, std::move(r), std::move(opts));
    }

private:
    ss::shared_ptr<impl> _impl;
};

template<typename Impl, typename... Args>
static consensus_client_protocol
make_consensus_client_protocol(Args&&... args) {
    return consensus_client_protocol(
      ss::make_shared<Impl>(std::forward<Args>(args)...));
}
} // namespace raft
