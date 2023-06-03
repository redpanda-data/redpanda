/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cluster/cloud_metadata/uploader.h"

#include "cloud_storage/remote.h"
#include "cloud_storage/types.h"
#include "cluster/cloud_metadata/cluster_manifest.h"
#include "cluster/cloud_metadata/key_utils.h"
#include "cluster/cloud_metadata/manifest_downloads.h"
#include "cluster/logger.h"
#include "model/fundamental.h"
#include "raft/consensus.h"
#include "raft/types.h"
#include "ssx/future-util.h"
#include "ssx/sleep_abortable.h"

#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/util/defer.hh>

#include <exception>

namespace cluster::cloud_metadata {

uploader::uploader(
  model::cluster_uuid cluster_uuid,
  cloud_storage_clients::bucket_name bucket,
  cloud_storage::remote& remote,
  consensus_ptr raft0)
  : _cluster_uuid(cluster_uuid)
  , _remote(remote)
  , _raft0(std::move(raft0))
  , _bucket(bucket) {}

ss::future<bool> uploader::term_has_changed(model::term_id term) {
    if (!_raft0->is_leader() || _raft0->term() != term) {
        co_return true;
    }
    auto barrier = co_await _raft0->linearizable_barrier();
    if (!barrier.has_value()) {
        co_return true;
    }
    // Following the above barrier, we're a healthy leader. Make sure our term
    // didn't change while linearizing.
    if (!_raft0->is_leader() || _raft0->term() != term) {
        co_return true;
    }
    co_return false;
}

ss::future<cluster_manifest_result>
uploader::download_highest_manifest_or_create(retry_chain_node& retry_node) {
    auto manifest_res = co_await download_highest_manifest_for_cluster(
      _remote, _cluster_uuid, _bucket, retry_node);

    if (manifest_res.has_value()) {
        // Happy path, just return.
        co_return manifest_res;
    }
    if (manifest_res.error() == error_outcome::no_matching_metadata) {
        cluster_metadata_manifest manifest{};
        vlog(
          clusterlog.debug,
          "No manifest found for cluster {}, creating a new one",
          _cluster_uuid);
        manifest.cluster_uuid = _cluster_uuid;
        co_return manifest;
    }
    // Pass through any other errors.
    co_return manifest_res;
}

ss::future<error_outcome> uploader::upload_next_metadata(
  model::term_id synced_term,
  cluster_metadata_manifest& manifest,
  retry_chain_node& retry_node) {
    if (manifest.metadata_id() < 0) {
        manifest.metadata_id = cluster_metadata_id(0);
    } else {
        manifest.metadata_id = cluster_metadata_id(manifest.metadata_id() + 1);
    }
    // TODO: upload cluster metadata.

    if (co_await term_has_changed(synced_term)) {
        co_return error_outcome::term_has_changed;
    }
    manifest.upload_time_since_epoch
      = std::chrono::duration_cast<std::chrono::milliseconds>(
        ss::lowres_system_clock::now().time_since_epoch());
    auto upload_result = co_await _remote.upload_manifest(
      _bucket, manifest, retry_node);
    if (upload_result != cloud_storage::upload_result::success) {
        vlog(
          clusterlog.warn,
          "Failed to upload cluster metadata manifest in term {}: {}",
          synced_term,
          upload_result);
        co_return error_outcome::upload_failed;
    }
    co_return error_outcome::success;
}

} // namespace cluster::cloud_metadata
