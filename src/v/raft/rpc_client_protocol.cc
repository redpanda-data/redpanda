// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/rpc_client_protocol.h"

#include "outcome_future_utils.h"
#include "raft/raftgen_service.h"
#include "rpc/connection_cache.h"
#include "rpc/exceptions.h"
#include "rpc/transport.h"
#include "rpc/types.h"

namespace raft {

ss::future<result<vote_reply>> rpc_client_protocol::vote(
  model::node_id n, vote_request&& r, rpc::client_opts opts) {
    return _connection_cache.local().with_node_client<raftgen_client_protocol>(
      _self,
      ss::this_shard_id(),
      n,
      opts.timeout,
      [r = std::move(r),
       opts = std::move(opts)](raftgen_client_protocol client) mutable {
          return client.vote(std::move(r), std::move(opts))
            .then(&rpc::get_ctx_data<vote_reply>);
      });
}

ss::future<result<append_entries_reply>> rpc_client_protocol::append_entries(
  model::node_id n, append_entries_request&& r, rpc::client_opts opts) {
    return _connection_cache.local().with_node_client<raftgen_client_protocol>(
      _self,
      ss::this_shard_id(),
      n,
      opts.timeout,
      [r = std::move(r),
       opts = std::move(opts)](raftgen_client_protocol client) mutable {
          return client.append_entries(std::move(r), std::move(opts))
            .then(&rpc::get_ctx_data<append_entries_reply>);
      });
}

ss::future<result<heartbeat_reply>> rpc_client_protocol::heartbeat(
  model::node_id n, heartbeat_request&& r, rpc::client_opts opts) {
    return _connection_cache.local().with_node_client<raftgen_client_protocol>(
      _self,
      ss::this_shard_id(),
      n,
      opts.timeout,
      [r = std::move(r),
       opts = std::move(opts)](raftgen_client_protocol client) mutable {
          return client.heartbeat(std::move(r), std::move(opts))
            .then(&rpc::get_ctx_data<heartbeat_reply>);
      });
}

ss::future<result<install_snapshot_reply>>
rpc_client_protocol::install_snapshot(
  model::node_id n, install_snapshot_request&& r, rpc::client_opts opts) {
    return _connection_cache.local().with_node_client<raftgen_client_protocol>(
      _self,
      ss::this_shard_id(),
      n,
      opts.timeout,
      [r = std::move(r),
       opts = std::move(opts)](raftgen_client_protocol client) mutable {
          return client.install_snapshot(std::move(r), std::move(opts))
            .then(&rpc::get_ctx_data<install_snapshot_reply>);
      });
}

ss::future<result<timeout_now_reply>> rpc_client_protocol::timeout_now(
  model::node_id n, timeout_now_request&& r, rpc::client_opts opts) {
    return _connection_cache.local().with_node_client<raftgen_client_protocol>(
      _self,
      ss::this_shard_id(),
      n,
      opts.timeout,
      [r = std::move(r),
       opts = std::move(opts)](raftgen_client_protocol client) mutable {
          return client.timeout_now(std::move(r), std::move(opts))
            .then(&rpc::get_ctx_data<timeout_now_reply>);
      });
}

} // namespace raft
