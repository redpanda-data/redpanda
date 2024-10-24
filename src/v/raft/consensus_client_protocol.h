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
        virtual ss::future<result<vote_reply>>
        vote(model::node_id, vote_request&&, rpc::client_opts) = 0;

        virtual ss::future<result<append_entries_reply>> append_entries(
          model::node_id,
          append_entries_request&&,
          rpc::client_opts,
          bool use_all_serde_encoding)
          = 0;

        virtual ss::future<result<heartbeat_reply>>
        heartbeat(model::node_id, heartbeat_request&&, rpc::client_opts) = 0;
        virtual ss::future<result<heartbeat_reply_v2>>
        heartbeat_v2(model::node_id, heartbeat_request_v2&&, rpc::client_opts)
          = 0;

        virtual ss::future<result<install_snapshot_reply>> install_snapshot(
          model::node_id, install_snapshot_request&&, rpc::client_opts)
          = 0;

        virtual ss::future<result<timeout_now_reply>>
        timeout_now(model::node_id, timeout_now_request&&, rpc::client_opts)
          = 0;

        virtual ss::future<bool> ensure_disconnect(model::node_id) = 0;

        virtual ss::future<result<transfer_leadership_reply>>
        transfer_leadership(
          model::node_id, transfer_leadership_request&&, rpc::client_opts)
          = 0;

        virtual ss::future<> reset_backoff(model::node_id) = 0;

        virtual ~impl() noexcept = default;
    };

public:
    explicit consensus_client_protocol(ss::shared_ptr<impl> i)
      : _impl(std::move(i)) {}
    ss::future<result<vote_reply>>
    vote(model::node_id target_node, vote_request&& r, rpc::client_opts opts) {
        return _impl->vote(target_node, std::move(r), std::move(opts));
    }

    ss::future<result<append_entries_reply>> append_entries(
      model::node_id target_node,
      append_entries_request&& r,
      rpc::client_opts opts,
      bool use_all_serde_encoding) {
        return _impl->append_entries(
          target_node, std::move(r), std::move(opts), use_all_serde_encoding);
    }

    ss::future<result<heartbeat_reply>> heartbeat(
      model::node_id target_node,
      heartbeat_request&& r,
      rpc::client_opts opts) {
        return _impl->heartbeat(target_node, std::move(r), std::move(opts));
    }
    ss::future<result<heartbeat_reply_v2>> heartbeat_v2(
      model::node_id target_node,
      heartbeat_request_v2&& r,
      rpc::client_opts opts) {
        return _impl->heartbeat_v2(target_node, std::move(r), std::move(opts));
    }

    ss::future<result<install_snapshot_reply>> install_snapshot(
      model::node_id target_node,
      install_snapshot_request&& r,
      rpc::client_opts opts) {
        return _impl->install_snapshot(
          target_node, std::move(r), std::move(opts));
    }

    ss::future<result<timeout_now_reply>> timeout_now(
      model::node_id target_node,
      timeout_now_request&& r,
      rpc::client_opts opts) {
        return _impl->timeout_now(target_node, std::move(r), std::move(opts));
    }

    ss::future<bool> ensure_disconnect(model::node_id target_node) {
        return _impl->ensure_disconnect(target_node);
    }

    ss::future<result<transfer_leadership_reply>> transfer_leadership(
      model::node_id target_node,
      transfer_leadership_request&& r,
      rpc::client_opts opts) {
        return _impl->transfer_leadership(
          target_node, std::move(r), std::move(opts));
    }

    ss::future<> reset_backoff(model::node_id target_node) {
        return _impl->reset_backoff(target_node);
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
