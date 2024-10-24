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

#include "base/outcome.h"
#include "base/vlog.h"
#include "cloud_storage/remote.h"
#include "cloud_storage_clients/types.h"
#include "cluster/cloud_metadata/error_outcome.h"
#include "cluster/cloud_metadata/key_utils.h"
#include "cluster/cloud_metadata/types.h"
#include "cluster/logger.h"
#include "config/configuration.h"
#include "kafka/server/group_manager.h"
#include "ssx/future-util.h"
#include "utils/retry_chain_node.h"

#include <seastar/core/lowres_clock.hh>

#include <chrono>
#include <exception>

using namespace std::chrono_literals;

namespace cluster::cloud_metadata {

ss::future<offsets_upload_reply>
offsets_uploader::upload(offsets_upload_request req) {
    auto holder = _gate.hold();
    retry_chain_node parent_node(_as, 30s, 1s);
    auto res = co_await upload(
      req.cluster_uuid, req.offsets_ntp, req.meta_id, parent_node);
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
  retry_chain_node& parent_node) {
    vlog(clusterlog.debug, "Requested to upload offsets from {}", ntp);
    auto holder = _gate.hold();
    _as.check();
    auto snap_res = co_await _group_manager.local().snapshot_groups(
      ntp,
      config::shard_local_cfg()
        .cloud_storage_cluster_metadata_num_consumer_groups_per_upload());
    if (snap_res.has_error()) {
        co_return snap_res.error();
    }
    size_t snap_idx = 0;
    auto& snaps = snap_res.value();
    offsets_upload_paths paths;
    for (auto& snap : snaps) {
        retry_chain_node retry_node(&parent_node);
        auto buf = serde::to_iobuf(std::move(snap));
        auto remote_key = offsets_snapshot_key(
          uuid, meta_id, ntp.tp.partition, snap_idx++);

        try {
            auto upload_res = co_await _remote.local().upload_object({
              .transfer_details
              = {.bucket = _bucket, .key = remote_key, .parent_rtc = retry_node},
              .type = cloud_storage::upload_type::group_offsets_snapshot,
              .payload = std::move(buf),
            });
            if (upload_res == cloud_storage::upload_result::success) {
                paths.paths.emplace_back(remote_key().c_str());
            }
        } catch (...) {
            auto eptr = std::current_exception();
            if (ssx::is_shutdown_exception(eptr)) {
                vlog(clusterlog.debug, "Shutdown while uploading offsets");
            } else {
                vlog(
                  clusterlog.debug, "Error while uploading offsets: {}", eptr);
            }
            co_return error_outcome::upload_failed;
        }
    }
    co_return paths;
}

} // namespace cluster::cloud_metadata
