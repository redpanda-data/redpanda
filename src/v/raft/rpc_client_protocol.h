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

#include "model/metadata.h"
#include "outcome_future_utils.h"
#include "raft/consensus_client_protocol.h"
#include "raft/errc.h"
#include "raft/raftgen_service.h"
#include "rpc/fwd.h"
#include "rpc/transport.h"

#include <system_error>

namespace raft {

/// Raft client protocol implementation underlied by RPC connections cache
class rpc_client_protocol final : public consensus_client_protocol::impl {
public:
    explicit rpc_client_protocol(
      model::node_id self, ss::sharded<rpc::connection_cache>& cache)
      : _self(self)
      , _connection_cache(cache) {}

    ss::future<result<vote_reply>>
    vote(model::node_id, vote_request&&, rpc::client_opts) final;

    ss::future<result<append_entries_reply>> append_entries(
      model::node_id, append_entries_request&&, rpc::client_opts) final;

    ss::future<result<heartbeat_reply>>
    heartbeat(model::node_id, heartbeat_request&&, rpc::client_opts) final;

    ss::future<result<install_snapshot_reply>> install_snapshot(
      model::node_id, install_snapshot_request&&, rpc::client_opts) final;

    ss::future<result<timeout_now_reply>>
    timeout_now(model::node_id, timeout_now_request&&, rpc::client_opts) final;

    ss::future<bool> ensure_disconnect(model::node_id) final;

    ss::future<result<transfer_leadership_reply>> transfer_leadership(
      model::node_id, transfer_leadership_request&&, rpc::client_opts) final;

    ss::future<> reset_backoff(model::node_id n);

private:
    model::node_id _self;
    ss::sharded<rpc::connection_cache>& _connection_cache;
};

inline consensus_client_protocol make_rpc_client_protocol(
  model::node_id self, ss::sharded<rpc::connection_cache>& clients) {
    return raft::make_consensus_client_protocol<raft::rpc_client_protocol>(
      self, clients);
}

} // namespace raft
