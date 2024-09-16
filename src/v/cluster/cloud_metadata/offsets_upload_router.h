// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#pragma once

#include "cluster/cloud_metadata/offsets_upload_rpc_types.h"
#include "cluster/cloud_metadata/offsets_uploader.h"
#include "cluster/leader_router.h"
#include "cluster/offsets_recovery_rpc_service.h"
#include "config/configuration.h"

namespace cluster::cloud_metadata {

class offsets_upload_handler {
public:
    explicit offsets_upload_handler(ss::sharded<offsets_uploader>& uploader)
      : _uploader(uploader) {}

    using proto_t = offsets_recovery_client_protocol;
    static ss::sstring process_name() { return "offsets upload"; }
    static offsets_upload_reply error_resp(cluster::errc e) {
        return offsets_upload_reply{.ec = e, .uploaded_paths = {}};
    }

    static ss::future<result<rpc::client_context<offsets_upload_reply>>>
    dispatch(
      offsets_recovery_client_protocol proto,
      offsets_upload_request req,
      model::timeout_clock::duration timeout) {
        return proto.offsets_upload(
          std::move(req),
          rpc::client_opts(model::timeout_clock::now() + timeout));
    }

    ss::future<offsets_upload_reply>
    process(ss::shard_id shard, offsets_upload_request req) {
        auto reply = co_await _uploader.invoke_on(
          shard, [r = std::move(req)](auto& uploader) mutable {
              return uploader.upload(std::move(r));
          });
        co_return reply;
    }

private:
    ss::sharded<offsets_uploader>& _uploader;
};

class offsets_upload_router
  : public leader_router<
      offsets_upload_request,
      offsets_upload_reply,
      offsets_upload_handler>
  , public offsets_upload_requestor {
public:
    offsets_upload_router(
      ss::sharded<offsets_uploader>& uploader,
      ss::sharded<cluster::shard_table>& shard_table,
      ss::sharded<cluster::metadata_cache>& metadata_cache,
      ss::sharded<rpc::connection_cache>& connection_cache,
      ss::sharded<partition_leaders_table>& leaders,
      const model::node_id node_id)
      : leader_router<
          offsets_upload_request,
          offsets_upload_reply,
          offsets_upload_handler>(
          shard_table,
          metadata_cache,
          connection_cache,
          leaders,
          _handler,
          node_id,
          config::shard_local_cfg()
            .cloud_storage_cluster_metadata_retries.value(),
          config::shard_local_cfg()
            .cloud_storage_cluster_metadata_upload_timeout_ms.value())
      , _handler(uploader) {}

    ss::future<> start() { co_return; }
    void request_stop() { leader_router::request_shutdown(); }
    ss::future<> stop() { return leader_router::shutdown(); }

    ss::future<offsets_upload_reply> request_upload(
      offsets_upload_request req,
      model::timeout_clock::duration timeout) override {
        auto ntp = req.offsets_ntp;
        co_return co_await process_or_dispatch(
          std::move(req), std::move(ntp), timeout);
    }

private:
    offsets_upload_handler _handler;
};

} // namespace cluster::cloud_metadata
