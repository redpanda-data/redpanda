// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "raft/rpc_client_protocol.h"

#include "base/outcome_future_utils.h"
#include "raft/raftgen_service.h"
#include "raft/types.h"
#include "rpc/connection_cache.h"
#include "rpc/exceptions.h"
#include "rpc/transport.h"
#include "rpc/types.h"

#include <seastar/core/coroutine.hh>

namespace raft {

ss::future<result<vote_reply>> rpc_client_protocol::vote(
  model::node_id n, vote_request&& r, rpc::client_opts opts) {
    auto timeout = opts.timeout;
    return _connection_cache.local().with_node_client<raftgen_client_protocol>(
      _self,
      ss::this_shard_id(),
      n,
      timeout,
      [r = std::move(r),
       opts = std::move(opts)](raftgen_client_protocol client) mutable {
          return client.vote(std::move(r), std::move(opts))
            .then(&rpc::get_ctx_data<vote_reply>);
      });
}

ss::future<result<append_entries_reply>> rpc_client_protocol::append_entries(
  model::node_id n,
  append_entries_request&& r,
  rpc::client_opts opts,
  bool use_all_serde_encoding) {
    auto timeout = opts.timeout;
    return _connection_cache.local().with_node_client<raftgen_client_protocol>(
      _self,
      ss::this_shard_id(),
      n,
      timeout,
      [r = std::move(r), opts = std::move(opts), use_all_serde_encoding](
        raftgen_client_protocol client) mutable {
          if (likely(use_all_serde_encoding)) {
              return client
                .append_entries_full_serde(
                  append_entries_request_serde_wrapper(std::move(r)),
                  std::move(opts))
                .then(&rpc::get_ctx_data<append_entries_reply>);
          }
          return client.append_entries(std::move(r), std::move(opts))
            .then(&rpc::get_ctx_data<append_entries_reply>);
      });
}

ss::future<result<heartbeat_reply>> rpc_client_protocol::heartbeat(
  model::node_id n, heartbeat_request&& r, rpc::client_opts opts) {
    auto timeout = opts.timeout;
    return _connection_cache.local().with_node_client<raftgen_client_protocol>(
      _self,
      ss::this_shard_id(),
      n,
      timeout,
      [r = std::move(r),
       opts = std::move(opts)](raftgen_client_protocol client) mutable {
          return client.heartbeat(std::move(r), std::move(opts))
            .then(&rpc::get_ctx_data<heartbeat_reply>);
      });
}
ss::future<result<heartbeat_reply_v2>> rpc_client_protocol::heartbeat_v2(
  model::node_id n, heartbeat_request_v2&& r, rpc::client_opts opts) {
    auto timeout = opts.timeout;
    return _connection_cache.local().with_node_client<raftgen_client_protocol>(
      _self,
      ss::this_shard_id(),
      n,
      timeout,
      [r = std::move(r),
       opts = std::move(opts)](raftgen_client_protocol client) mutable {
          return client.heartbeat_v2(std::move(r), std::move(opts))
            .then(&rpc::get_ctx_data<heartbeat_reply_v2>);
      });
}

ss::future<result<install_snapshot_reply>>
rpc_client_protocol::install_snapshot(
  model::node_id n, install_snapshot_request&& r, rpc::client_opts opts) {
    auto timeout = opts.timeout;
    return _connection_cache.local().with_node_client<raftgen_client_protocol>(
      _self,
      ss::this_shard_id(),
      n,
      timeout,
      [r = std::move(r),
       opts = std::move(opts)](raftgen_client_protocol client) mutable {
          return client.install_snapshot(std::move(r), std::move(opts))
            .then(&rpc::get_ctx_data<install_snapshot_reply>);
      });
}

ss::future<result<timeout_now_reply>> rpc_client_protocol::timeout_now(
  model::node_id n, timeout_now_request&& r, rpc::client_opts opts) {
    auto timeout = opts.timeout;
    return _connection_cache.local().with_node_client<raftgen_client_protocol>(
      _self,
      ss::this_shard_id(),
      n,
      timeout,
      [r = std::move(r),
       opts = std::move(opts)](raftgen_client_protocol client) mutable {
          return client.timeout_now(std::move(r), std::move(opts))
            .then(&rpc::get_ctx_data<timeout_now_reply>);
      });
}

ss::future<> rpc_client_protocol::reset_backoff(model::node_id n) {
    return _connection_cache.local().reset_client_backoff(
      _self, ss::this_shard_id(), n);
}

ss::future<bool> rpc_client_protocol::ensure_disconnect(model::node_id n) {
    struct resetter {
        ss::lw_shared_ptr<rpc::transport> transport;
        resetter(ss::lw_shared_ptr<rpc::transport> t)
          : transport(t) {}
    };

    return _connection_cache.local()
      .with_node_client<resetter>(
        _self,
        ss::this_shard_id(),
        n,
        std::chrono::milliseconds(100),
        [](resetter r) {
            // Give the caller a bool clue as to whether we really shut
            // anything down (false indicates this was a no-op)
            bool was_valid = r.transport->is_valid();

            r.transport->shutdown();
            return was_valid;
        })
      .then([]([[maybe_unused]] result<bool> r) {
          // if result contains an error no connection was shut down, return
          // false
          return r.has_value() ? r.value() : false;
      });
}

ss::future<result<transfer_leadership_reply>>
rpc_client_protocol::transfer_leadership(
  model::node_id n, transfer_leadership_request&& r, rpc::client_opts opts) {
    auto timeout = opts.timeout;
    return _connection_cache.local().with_node_client<raftgen_client_protocol>(
      _self,
      ss::this_shard_id(),
      n,
      timeout,
      [r = std::move(r),
       opts = std::move(opts)](raftgen_client_protocol client) mutable {
          return client.transfer_leadership(std::move(r), std::move(opts))
            .then(&rpc::get_ctx_data<transfer_leadership_reply>);
      });
}

} // namespace raft
