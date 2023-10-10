/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cluster/cloud_metadata/offsets_uploader.h"

#include "cloud_storage/remote.h"
#include "cloud_storage_clients/types.h"
#include "cluster/cloud_metadata/error_outcome.h"
#include "cluster/cloud_metadata/key_utils.h"
#include "cluster/cloud_metadata/types.h"
#include "kafka/server/group_manager.h"
#include "outcome.h"
#include "serde/serde.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/lowres_clock.hh>

namespace cluster::cloud_metadata {

ss::future<offsets_upload_reply>
offsets_uploader::upload(offsets_upload_request req) {
    auto holder = _gate.hold();
    retry_chain_node retry_node(_as, 30s, 1s);
    auto res = co_await upload(
      req.cluster_uuid, req.offsets_ntp, req.meta_id, retry_node);
    offsets_upload_reply reply;
    if (res.has_error()) {
        reply.ec = cluster::errc::partition_operation_failed;
        co_return reply;
    }
    reply.uploaded_paths = std::move(res.value().paths);
    co_return reply;
}

ss::future<offsets_upload_result> offsets_uploader::upload(
  const model::cluster_uuid& uuid,
  const model::ntp& ntp,
  const cluster_metadata_id& meta_id,
  retry_chain_node& retry_node) {
    auto holder = _gate.hold();
    _as.check();
    auto snap_res = co_await _group_manager.local().snapshot_groups(ntp);
    if (snap_res.has_error()) {
        co_return snap_res.error();
    }
    auto& snap = snap_res.value();
    auto buf = serde::to_iobuf(std::move(snap));
    auto remote_key = offsets_snapshot_key(uuid, meta_id, ntp.tp.partition);

    offsets_upload_paths paths;
    auto upload_res = co_await _remote.local().upload_object(
      _bucket, remote_key, std::move(buf), retry_node);
    if (upload_res == cloud_storage::upload_result::success) {
        paths.paths.emplace_back(remote_key().c_str());
        co_return paths;
    }
    co_return error_outcome::upload_failed;
}

} // namespace cluster::cloud_metadata
