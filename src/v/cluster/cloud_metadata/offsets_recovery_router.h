// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "cluster/cloud_metadata/offsets_recoverer.h"
#include "cluster/cloud_metadata/offsets_recovery_rpc_types.h"
#include "cluster/leader_router.h"
#include "cluster/offsets_recovery_rpc_service.h"
#include "config/configuration.h"

namespace cluster::cloud_metadata {

class offsets_recovery_handler {
public:
    explicit offsets_recovery_handler(ss::sharded<offsets_recoverer>& recoverer)
      : _recoverer(recoverer) {}

    using proto_t = offsets_recovery_client_protocol;
    static ss::sstring process_name() { return "offsets recovery"; }
    static offsets_recovery_reply error_resp(cluster::errc e) {
        return offsets_recovery_reply{.ec = e};
    }

    static ss::future<result<rpc::client_context<offsets_recovery_reply>>>
    dispatch(
      offsets_recovery_client_protocol proto,
      offsets_recovery_request req,
      model::timeout_clock::duration timeout) {
        return proto.offsets_recovery(
          std::move(req),
          rpc::client_opts(model::timeout_clock::now() + timeout));
    }

    ss::future<offsets_recovery_reply>
    process(ss::shard_id shard, offsets_recovery_request req) {
        auto reply = co_await _recoverer.invoke_on(
          shard, [r = std::move(req)](auto& rec) mutable {
              return rec.recover(std::move(r));
          });
        co_return reply;
    }

private:
    ss::sharded<offsets_recoverer>& _recoverer;
};

class offsets_recovery_router
  : public leader_router<
      offsets_recovery_request,
      offsets_recovery_reply,
      offsets_recovery_handler> {
public:
    offsets_recovery_router(
      ss::sharded<offsets_recoverer>& offsets_recoverer,
      ss::sharded<cluster::shard_table>& shard_table,
      ss::sharded<cluster::metadata_cache>& metadata_cache,
      ss::sharded<rpc::connection_cache>& connection_cache,
      ss::sharded<partition_leaders_table>& leaders,
      const model::node_id node_id)
      : leader_router<
          offsets_recovery_request,
          offsets_recovery_reply,
          offsets_recovery_handler>(
          shard_table,
          metadata_cache,
          connection_cache,
          leaders,
          _handler,
          node_id,
          config::shard_local_cfg()
            .cloud_storage_cluster_metadata_retries.value(),
          5s)
      , _handler(offsets_recoverer) {}

    ss::future<> start() { co_return; }
    ss::future<> stop() { return shutdown(); }

    ss::future<offsets_recovery_reply> request_recovery(
      offsets_recovery_request req, model::timeout_clock::duration timeout) {
        auto ntp = req.offsets_ntp;
        return process_or_dispatch(std::move(req), std::move(ntp), timeout);
    }

private:
    offsets_recovery_handler _handler;
};

} // namespace cluster::cloud_metadata
